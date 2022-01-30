# RedisExporter
This application makes it possible to export and import specific databases in a Redis server, this is useful for things such as moving databases from one index to another, or simply just exporting a single database and not an entire server.

### Usage
```bash
$ java -jar RedisExporter.jar
```
#### Available Flags
```bash
$ java -jar RedisExporter.jar --help

Option               Description
------               -----------
-?, -h, --help       Show the help message
--confirm            Whether to confirm the import
--export <Boolean>   Whether to export or import the database (default: true)
--file <File>        The data file (default: data.json)
--flush              Whether to flush the database prior to importing
--host <String>      The host to connect to (default: localhost)
--index <Integer>    The database index to use during connection (default: 0)
--password <String>  The password to use during connection
--port <Integer>     The port to connect to (default: 6379)
```