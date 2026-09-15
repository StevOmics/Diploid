# MediaBridge
Media Archiving Utility

# Core functions
Mediabridge will support two key functions: 1) Cataloging media files (mp4 and mp3 etc files) 2) Managing files

# Architecture
This project follows a microservice architecture, supporting multiple container-based services to manage specific concerns:

web: FastAPI + Jinja frontend to support user interface
database: postgres container to support application as well as media catalog functions

See `CLAUDE.md` for a full architecture/data-model breakdown.

# License
Copyright (c) 2026 Steve Ayers.

Licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0). This means you're free to use, modify, and self-host MediaBridge, but if you run a modified version as a network service, you must make your modified source available to its users.

A separate commercial license (for embedding MediaBridge in a closed-source or SaaS product without AGPL's source-sharing obligations) may be made available in the future — contact the project owner if interested.

