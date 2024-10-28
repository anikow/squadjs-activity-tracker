## SquadJS Activity Tracker Plugin

[SquadJS](https://github.com/Team-Silver-Sphere/SquadJS) plugin for activity tracking based on Squad games logs. 

### Features

- Tracks player join and leave times.
- Detects server restarts and handles log rotations.
- Supports multiple log reading modes: `tail` and `sftp`.
- Stores activity data in the SquadJS database. 

### Example configuration
```json
    {
      "plugin": "ActivityTracker",
      "enabled": true,
      "database": "mysql"
    },
```