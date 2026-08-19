# MediaBridge
Media Archiving Utility

# Core functions
Mediabridge will support two key functions: 1) Cataloging media files (mp4 and mp3 etc files) 2) Managing files

# Architecture
This project will follow a microservice architecture, supporting multiple container-based services to manage specific concerns:

web: fastapi/VueJS -based frontend to support user interface
database: postgres container to support application as well as media catalog functions

