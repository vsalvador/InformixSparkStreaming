# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased] - yyyy-mm-dd
 
Here we write upgrading notes for brands. It's a team effort to make them as
straightforward as possible.
 
### Added
 
### Changed
 
### Fixed
 
## [0.4] - 2026-04-22
  
This is a tested and almost complete version. As it's used in production only in one system, we still keep the beta status.

### Added
- Support for BOOLEAN, SERIAL8, INT8, NCHAR and NVARCHAR SQL types
- Ability to set the MQTT QoS upon index creation

### Changed
- Changed examples to include more complex data types
- Changed README and CHANGELOG files
- Organize stores7 demo files into examples-stores7 folder

### Fixed
- Fixed transaction storage in memory algorithms to fix original issue in full transaction processing
- Fixed some memory leaks and memory management issues
- Make packet ID consistent by storing counter in shared memory         

## [Original IBM IoT branch] - 2017-02-06

### Added
- Added ability to set the MQTT topic upon index creation
- Added hostname, database name and table name to output

### Changed

- Removed code that set the MQTT topic to the table name
- Changed demo to work with stores_demo database
- Changed examples to include new required topic parameter

