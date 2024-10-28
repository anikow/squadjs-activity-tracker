import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { DataTypes } from 'sequelize';
import BasePlugin from './base-plugin.js';
import chokidar from 'chokidar';
import { parse } from 'date-fns';
import SftpClient from 'ssh2-sftp-client';

export default class ActivityTracker extends BasePlugin {
  static get description() {
    return 'The <code>activity-tracker</code> plugin logs player join and leave times by parsing the server log file, handling bot downtime and server restarts.';
  }

  static get defaultEnabled() {
    return false;
  }

  static get optionsSpecification() {
    return {
      database: {
        required: true,
        connector: 'sequelize',
        description: 'The Sequelize connector to log activity to.',
        default: 'mysql',
      },
    };
  }

  constructor(server, options, connectors) {
    super(server, options, connectors);

    this.models = {};

    this.defineModels();

    this.logFilePath = path.join(server.options.logDir, 'SquadGame.log');
    this.serverID = server.options.id;

    this.initRegexPatterns();

    // Determine log reader mode
    this.logReaderMode = server.options.logReaderMode || 'tail';

    // Initialize variables to manage log processing state
    this.isProcessing = false;
    this.pendingProcessing = false;
    this.lastReadPosition = 0;

    if (this.logReaderMode === 'sftp') {
      // Initialize SFTP related properties
      this.sftpConfig = server.options.sftp; // Assuming SFTP config is nested under server.options
      this.sftp = new SftpClient();
      this.sftpConnected = false;
      this.sftpPollInterval = 5000; // Poll every 5 seconds
      this.sftpPollTimer = null;
    }

    // Bind methods to ensure correct context
    this.parseLogLine = this.parseLogLine.bind(this);
    this.processNewData = this.processNewData.bind(this);
    this.handleServerRestart = this.handleServerRestart.bind(this);
    this.handleFileChange = this.handleFileChange.bind(this);
    if (this.logReaderMode === 'sftp') {
      this.pollSftpLogFile = this.pollSftpLogFile.bind(this);
    }
  }

  defineModels() {
    // Define PlayerSession model
    this.createModel('PlayerSession', {
      id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
      },
      server: {
        type: DataTypes.INTEGER,
        allowNull: false,
      },
      steamID: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      eosID: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      nickname: {
        type: DataTypes.STRING,
      },
      joinTime: {
        type: DataTypes.DATE,
        allowNull: false,
      },
      leaveTime: {
        type: DataTypes.DATE,
      },
    }, {
      indexes: [
        {
          fields: ['eosID', 'server'],
        },
      ],
    });

    // Define LogPosition model to store last read position and server ID
    this.createModel('LogPosition', {
      id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
      },
      server: {
        type: DataTypes.INTEGER,
        allowNull: false,
      },
      filePath: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      position: {
        type: DataTypes.BIGINT,
        allowNull: false,
      },
      inode: {
        type: DataTypes.STRING,
        allowNull: true,
      },
    }, {
      // Composite unique key on server and filePath
      indexes: [
        {
          unique: true,
          fields: ['server', 'filePath'],
        },
      ],
    });
  }

  initRegexPatterns() {
    // Regex for player join
    this.joinRegex = /^\[([0-9.:-]+)]\[[ 0-9]*]LogSquad: PostLogin: NewPlayer: BP_PlayerController_C .+PersistentLevel\.([^ ]+) \(IP: ([\d.]+) \| Online IDs:([^)\n]+)\)/;

    // Regex for player leave
    this.leaveRegex = /^\[([0-9.:-]+)]\[[ 0-9]*]LogNet: UNetConnection::Close: .*? RemoteAddr: ([\d.]+):[\d]+, Name: EOSIpNetConnection_[\d]+, Driver: GameNetDriver EOSNetDriver_[\d]+, IsServer: YES, PC: ([^ ]+PlayerController_C_[\d]+), Owner: [^ ]+PlayerController_C_[\d]+, UniqueId: RedpointEOS:([\d\w]+)/;

    // Updated regex for player kick
    this.kickRegex = /^\[([0-9.:-]+)]\[[ 0-9]*]LogSquad: ADMIN COMMAND: Kicked player \d+\.\s*\[Online IDs= EOS: (\w+) steam: (\d+)\]\s*(?:\{[^\}]+\}\s*)?(.*?) from RCON/;

    // Regex for Anti-Cheat kick
    this.antiCheatKickRegex = /^\[([0-9.:-]+)]\[[ 0-9]*]LogEOSAntiCheat: Verbose: Player tracking: (\w+): Removed player as handle count reached 0/;
  }

  createModel(name, schema, options = {}) {
    this.models[name] = this.options.database.define(`ActivityTracker_${name}`, schema, {
      timestamps: false,
      charset: 'utf8mb4',
      collate: 'utf8mb4_unicode_ci',
      ...options,
    });
  }

  async prepareToMount() {
    // Sync the models
    await this.models.PlayerSession.sync();
    await this.models.LogPosition.sync();
  }

  async mount() {
    try {
      if (this.logReaderMode === 'tail') {
        await this.startLogFileWatcher();
      } else if (this.logReaderMode === 'sftp') {
        await this.startSftpLogReader();
      } else {
        this.verbose(1, `Unsupported logReaderMode: ${this.logReaderMode}`);
      }
    } catch (error) {
      this.verbose(1, `Error mounting ActivityTracker: ${error.message}`);
    }
  }

  async unmount() {
    if (this.logReaderMode === 'tail' && this.logWatcher) {
      this.logWatcher.close();
      this.verbose(1, 'Stopped watching log file.');
    }

    if (this.logReaderMode === 'sftp' && this.sftp) {
      if (this.sftpPollTimer) {
        clearInterval(this.sftpPollTimer);
      }
      try {
        await this.sftp.end();
        this.verbose(1, 'SFTP connection closed.');
      } catch (error) {
        this.verbose(1, `Error closing SFTP connection: ${error.message}`);
      }
    }
  }

  async startLogFileWatcher() {
    try {
      const positionRecord = await this.models.LogPosition.findOne({
        where: {
          server: this.serverID,
          filePath: this.logFilePath,
        },
      });

      const logStats = fs.statSync(this.logFilePath);
      this.lastLogFileInode = logStats.ino.toString();

      if (positionRecord) {
        // Check if the inode has changed, indicating a log rotation
        if (positionRecord.inode !== this.lastLogFileInode) {
          this.verbose(1, 'Log file has been rotated or recreated. Resetting last read position.');
          this.lastReadPosition = 0;
          positionRecord.position = 0;
          positionRecord.inode = this.lastLogFileInode;
          await positionRecord.save();
        } else {
          this.lastReadPosition = Number(positionRecord.position);
        }
      } else {
        // No previous position record; start from the beginning
        this.lastReadPosition = 0;
        await this.models.LogPosition.create({
          server: this.serverID,
          filePath: this.logFilePath,
          position: this.lastReadPosition,
          inode: this.lastLogFileInode,
        });
      }

      // On startup, process any missed log entries without size limit
      const logFileSize = logStats.size;

      if (logFileSize > this.lastReadPosition) {
        await this.processNewData(this.lastReadPosition, logFileSize, 'local');
        this.lastReadPosition = logFileSize;
      } else if (logFileSize < this.lastReadPosition) {
        // Log file was truncated or rotated
        this.verbose(1, 'Log file was truncated or rotated on startup. Treating this as a server restart.');
        this.lastReadPosition = 0;
        this.lastLogFileInode = logStats.ino.toString();
        const restartTime = new Date();
        await this.handleServerRestart(restartTime);
        await this.processNewData(0, logFileSize, 'local');
        this.lastReadPosition = logFileSize;
      }

      // Use chokidar with awaitWriteFinish to handle frequent file changes
      this.logWatcher = chokidar.watch(this.logFilePath, {
        persistent: true,
        ignoreInitial: true,
        awaitWriteFinish: {
          stabilityThreshold: 1000, // Wait for 1 second of inactivity
          pollInterval: 100,
        },
      });

      this.logWatcher.on('change', this.handleFileChange);

      this.verbose(1, `Started watching log file: ${this.logFilePath}`);
    } catch (error) {
      this.verbose(1, `Error starting log file watcher: ${error.message}`);
    }
  }

  async startSftpLogReader() {
    try {
      // Connect to the SFTP server
      await this.sftp.connect({
        host: this.sftpConfig.host,
        port: this.sftpConfig.port,
        username: this.sftpConfig.user,
        password: this.sftpConfig.password,
        readyTimeout: this.sftpConfig.timeout ? this.sftpConfig.timeout * 1000 : 10000, // Convert seconds to ms
      });
      this.sftpConnected = true;
      this.verbose(1, `Connected to SFTP server: ${this.sftpConfig.host}:${this.sftpConfig.port}`);

      // Initialize log position
      const positionRecord = await this.models.LogPosition.findOne({
        where: {
          server: this.serverID,
          filePath: this.logFilePath,
        },
      });

      // Get file stats
      const fileStat = await this.sftp.stat(this.logFilePath);
      this.verbose(2, `SFTP fileStat: ${JSON.stringify(fileStat)}`);

      if (positionRecord) {
        this.lastReadPosition = Number(positionRecord.position);
        this.verbose(1, `Resuming from last read position: ${this.lastReadPosition}`);
      } else {
        // No previous position record; start from the beginning of the file
        this.lastReadPosition = 0;
        await this.models.LogPosition.create({
          server: this.serverID,
          filePath: this.logFilePath,
          position: this.lastReadPosition,
          inode: null,
        });
        this.verbose(1, `No previous log position found. Starting from beginning of file.`);
      }

      // Initial read
      await this.pollSftpLogFile();

      // Set up polling
      this.sftpPollTimer = setInterval(this.pollSftpLogFile, this.sftpPollInterval);
      this.verbose(1, `Started polling SFTP log file: ${this.logFilePath} every ${this.sftpPollInterval / 1000} seconds`);
    } catch (error) {
      this.verbose(1, `Error starting SFTP log reader: ${error.message}`);
    }
  }

  async pollSftpLogFile() {
    if (this.isProcessing) {
      this.pendingProcessing = true;
      return;
    }

    this.isProcessing = true;

    try {
      const fileStat = await this.sftp.stat(this.logFilePath);
      const newFileSize = fileStat.size;
      this.verbose(2, `Current file size: ${newFileSize}, Last read position: ${this.lastReadPosition}`);

      if (newFileSize < this.lastReadPosition) {
        const sizeDifference = this.lastReadPosition - newFileSize;
        const sizeDecreaseThreshold = 5 * 1024 * 1024; // 5 MB
        if (sizeDifference >= sizeDecreaseThreshold) {
          // Log file was truncated or rotated
          this.verbose(1, 'SFTP log file was truncated or rotated significantly. Treating this as a server restart.');
          this.lastReadPosition = 0;
          const restartTime = new Date();
          await this.handleServerRestart(restartTime);
        } else {
          // Adjust read position without treating it as a restart
          this.verbose(1, `SFTP log file size decreased by ${sizeDifference} bytes. Adjusting read position.`);
          this.lastReadPosition = newFileSize;
        }
      }

      if (newFileSize > this.lastReadPosition) {
        await this.processNewData(this.lastReadPosition, newFileSize, 'sftp');
        this.lastReadPosition = newFileSize;

        // Update last read position in the database
        const [positionRecord, created] = await this.models.LogPosition.findOrCreate({
          where: {
            server: this.serverID,
            filePath: this.logFilePath,
          },
          defaults: {
            server: this.serverID,
            filePath: this.logFilePath,
            position: this.lastReadPosition,
            inode: null,
          },
        });

        if (!created) {
          positionRecord.position = this.lastReadPosition;
          await positionRecord.save();
        }
      }
    } catch (error) {
      this.verbose(1, `Error polling SFTP log file: ${error.message}`);
      // Optionally, handle reconnection logic here
    } finally {
      this.isProcessing = false;
      if (this.pendingProcessing) {
        this.pendingProcessing = false;
        this.pollSftpLogFile();
      }
    }
  }

  async handleFileChange() {
    if (this.isProcessing) {
      // If processing is ongoing, mark that we have pending changes
      this.pendingProcessing = true;
      return;
    }

    do {
      this.pendingProcessing = false;
      this.isProcessing = true;

      try {
        const logStats = fs.statSync(this.logFilePath);
        const newFileSize = logStats.size;
        const currentInode = logStats.ino.toString();

        if (newFileSize < this.lastReadPosition || currentInode !== this.lastLogFileInode) {
          // Log file was truncated or rotated. Treating this as a server restart.
          this.verbose(1, 'Log file was truncated or rotated. Treating this as a server restart.');
          this.lastReadPosition = 0;
          this.lastLogFileInode = currentInode;
          const restartTime = new Date();
          await this.handleServerRestart(restartTime);
        }

        if (newFileSize > this.lastReadPosition) {
          await this.processNewData(this.lastReadPosition, newFileSize, 'local');
          this.lastReadPosition = newFileSize;

          // Update last read position and inode in the database
          const [positionRecord, created] = await this.models.LogPosition.findOrCreate({
            where: {
              server: this.serverID,
              filePath: this.logFilePath,
            },
            defaults: {
              server: this.serverID,
              filePath: this.logFilePath,
              position: this.lastReadPosition,
              inode: this.lastLogFileInode,
            },
          });

          if (!created) {
            positionRecord.position = this.lastReadPosition;
            positionRecord.inode = this.lastLogFileInode;
            await positionRecord.save();
          }
        }
      } catch (error) {
        this.verbose(1, `Error processing file change: ${error.message}`);
      } finally {
        this.isProcessing = false;
      }
    } while (this.pendingProcessing);
  }

  async processNewData(start, end, mode = 'local') {
    if (start >= end) {
      this.verbose(2, `No new data to process. Start: ${start}, End: ${end}`);
      return;
    }

    if (mode === 'local') {
      // Local processing using fs and chokidar
      const stream = fs.createReadStream(this.logFilePath, {
        start,
        end: end - 1,
        encoding: 'utf8',
      });

      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity,
      });

      try {
        for await (const line of rl) {
          await this.parseLogLine(line);
        }
      } catch (error) {
        this.verbose(1, `Error reading local log file: ${error.message}`);
      }
    } else if (mode === 'sftp') {
      // Processing for SFTP
      try {
        const readStream = await this.sftp.createReadStream(this.logFilePath, {
          start,
          end: end - 1,
        });

        const rl = readline.createInterface({
          input: readStream,
          crlfDelay: Infinity,
        });

        for await (const line of rl) {
          await this.parseLogLine(line);
        }
      } catch (error) {
        this.verbose(1, `Error reading SFTP log file: ${error.message}`);
      }
    }
  }

  async parseLogLine(line) {
    try {
      // Check for player join
      const joinMatch = line.match(this.joinRegex);
      if (joinMatch) {
        const [_, timestampStr, playerController, ip, onlineIDs] = joinMatch;
        const timestamp = parse(timestampStr, 'yyyy.MM.dd-HH.mm.ss:SSS', new Date());
        const timestampUTC = new Date(timestamp.toISOString());

        // Extract EOS ID and Steam ID from online IDs
        const eosIDMatch = onlineIDs.match(/EOS: (\w+)/);
        const steamIDMatch = onlineIDs.match(/steam: (\d+)/);

        if (eosIDMatch && steamIDMatch) {
          const eosID = eosIDMatch[1];
          const steamID = steamIDMatch[1];
          const nickname = playerController; // Assuming playerController holds nickname

          await this.onPlayerJoined({ eosID, steamID, nickname, ip, time: timestampUTC });
        } else {
          this.verbose(1, `Failed to extract EOS ID or Steam ID from online IDs: ${onlineIDs}`);
        }

        return;
      }

      // Check for player leave
      const leaveMatch = line.match(this.leaveRegex);
      if (leaveMatch) {
        const [_, timestampStr, ip, playerController, eosID] = leaveMatch;
        const timestamp = parse(timestampStr, 'yyyy.MM.dd-HH.mm.ss:SSS', new Date());
        const timestampUTC = new Date(timestamp.toISOString());

        await this.onPlayerLeft({ eosID, ip, playerController, time: timestampUTC });
        return;
      }

      // Check for player kick
      const kickMatch = line.match(this.kickRegex);
      if (kickMatch) {
        const [_, timestampStr, eosID, steamID, nickname] = kickMatch;
        const timestamp = parse(timestampStr, 'yyyy.MM.dd-HH.mm.ss:SSS', new Date());
        const timestampUTC = new Date(timestamp.toISOString());

        await this.onPlayerKicked({ eosID, steamID, nickname, time: timestampUTC });
        return;
      }

      // Check for anti-cheat kick
      const antiCheatKickMatch = line.match(this.antiCheatKickRegex);
      if (antiCheatKickMatch) {
        const [_, timestampStr, eosID] = antiCheatKickMatch;
        const timestamp = parse(timestampStr, 'yyyy.MM.dd-HH.mm.ss:SSS', new Date());
        const timestampUTC = new Date(timestamp.toISOString());

        // Attempt to retrieve steamID and nickname from the active session
        let steamID = null;
        let nickname = null;
        const session = await this.models.PlayerSession.findOne({
          where: {
            eosID,
            server: this.serverID,
            leaveTime: null,
          },
          order: [['joinTime', 'DESC']],
        });

        if (session) {
          steamID = session.steamID;
          nickname = session.nickname;
        }

        await this.onPlayerKicked({ eosID, steamID, nickname, time: timestampUTC });
        return;
      }

      // No longer checking for server restart in log lines
    } catch (error) {
      this.verbose(1, `Error parsing log line: ${error.message}`);
    }
  }

  async onPlayerJoined({ eosID, steamID, nickname, ip, time }) {
    try {
      // Start a new session for the player
      await this.models.PlayerSession.create({
        eosID,
        steamID,
        server: this.serverID,
        nickname,
        joinTime: time,
        leaveTime: null,
      });

      this.verbose(1, `Player joined: ${nickname} (Steam ID: ${steamID}, EOS ID: ${eosID}) at ${time} from IP: ${ip}`);
    } catch (error) {
      this.verbose(1, `Error handling player join: ${error.message}`);
    }
  }

  async onPlayerLeft({ eosID, ip, playerController, time }) {
    try {
      // Close the session for the player using eosID
      const session = await this.models.PlayerSession.findOne({
        where: {
          eosID,
          server: this.serverID,
          leaveTime: null,
        },
        order: [['joinTime', 'DESC']],
      });

      if (session) {
        session.leaveTime = time;
        await session.save();
        this.verbose(1, `Player left: EOS ID ${eosID} at ${time} from IP: ${ip}`);
      } else {
        this.verbose(1, `No open session found for EOS ID ${eosID} on disconnect.`);
        // Optionally, create a session with null joinTime to log orphaned leave events
        await this.models.PlayerSession.create({
          eosID,
          steamID: null, // Unknown Steam ID
          server: this.serverID,
          nickname: playerController,
          joinTime: null,
          leaveTime: time,
        });
      }
    } catch (error) {
      this.verbose(1, `Error handling player leave: ${error.message}`);
    }
  }

  async onPlayerKicked({ eosID, steamID, nickname, time }) {
    try {
      // Close the session for the player using eosID
      const session = await this.models.PlayerSession.findOne({
        where: {
          eosID,
          server: this.serverID,
          leaveTime: null,
        },
        order: [['joinTime', 'DESC']],
      });

      if (session) {
        session.leaveTime = time;
        await session.save();
        this.verbose(1, `Player kicked: ${nickname || 'Unknown'} (Steam ID: ${steamID || 'Unknown'}, EOS ID: ${eosID}) at ${time}`);
      } else {
        this.verbose(1, `No open session found for EOS ID ${eosID} on kick.`);
        // Optionally, create a session with null joinTime to log orphaned leave events
        await this.models.PlayerSession.create({
          eosID,
          steamID,
          server: this.serverID,
          nickname,
          joinTime: null,
          leaveTime: time,
        });
      }
    } catch (error) {
      this.verbose(1, `Error handling player kick: ${error.message}`);
    }
  }

  async handleServerRestart(restartTime) {
    try {
      // Close all open sessions for this server
      const openSessions = await this.models.PlayerSession.findAll({
        where: {
          server: this.serverID,
          leaveTime: null,
        },
      });

      for (const session of openSessions) {
        session.leaveTime = restartTime;
        await session.save();
        this.verbose(
          1,
          `Closed session for EOS ID ${session.eosID} due to server restart at ${restartTime}`
        );
      }
    } catch (error) {
      this.verbose(1, `Error handling server restart: ${error.message}`);
    }
  }
}
