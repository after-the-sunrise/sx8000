# sx8000
[![Build Status][travis-icon]][travis-page]

sx8000 is a command-line tool for executing **S**QL queries, and e**x**porting the query results to CSV files. 
This tool is written in pure-Java and utilizes standard JDBC APIs. 
The name sx8000 comes from the belief that this is probably about the 8000th tool which does something similar. (kudos to [pg8000][pg8000])

* Utilizes Java's standard JDBC API, requiring no additional installations other then the Java Runtime Environment.
* JDBC libraries can be added/swapped/removed on the fly.
* Configurable via launch arguments. No configuration files nor environment variables are required.
* Query can either be supplied as a one-liner command line argument, or from a text file.
* Automatic/configurable escaping of the CSV column values.
* Automatic/configurable output file compression from the filename. (`.deflate` / `.gzip` / `.bz2`)
* Automatic/configurable checksum file generation. (cf: `example.csv.gz.sha256`)

## Usage

### Prerequisites
* JRE 8 or later.
* JDBC driver library. (`Derby` / `H2` / `HSQLDB` / `PostgreSQL` / `MySQL` drivers are included in the archive.)

### Displaying Usage

Download or build the archive file. Move into the archive's root directory and execute the launch script.

```shell script
unzip sx8000-${VERSION}.zip

cd sx-8000-${VERSION}

sh bin/sx8000
```

Usage summary will be displayed to the standard output when the script is launched with zero arguments. 
Specify at least one of the parameters to actually execute the tool.

```shell script
sh bin/sx8000 --out "./foo.csv.gz" --statement "select now()" 

sh bin/sx8000 --out "./bar.csv.gz" --statement "filepath:path/to/hoge.sql" --checksum "false" 
```

### Customizing JDBC Drivers

JDBC driver libraries can be added/changed/removed on the fly. 
The JDBC driver files can be found inin `./lib` directory of the unzipped archive. 
Modify the jar files in this directory and adjust the `CLASSPATH` variable in the launch scripts. 
Older version of the JDBC drivers may require explicitly specifying the driver class name. 

```shell script
sh bin/sx8000 --driver "org.h2.Driver" --url "jdbc:h2:mem:" --out "./hello.csv.gz" --statement "select 'Hello World!' as Greeting"
```

## Building from Source

### Prerequisites
* JDK 8 or later.
* Internet access for downloading the dependencies from Maven central repository.

### Build

Clone the repository, and execute `gradlew clean build` command. 
Archive files will be generated under `./build/distributions/` upon a successful build.   

## Disclaimer

sx8000 is not...
* a GUI application. 
* a scheduler. Use crontab or other scheduling middleware for automation.
* secured nor protected. Arbitrary query (cf: `drop table ...;`) can be executed without any validation.
* for everyone.  Think twice and use at your own risk.


[travis-page]:https://travis-ci.org/after-the-sunrise/sx8000
[travis-icon]:https://travis-ci.org/after-the-sunrise/sx8000.svg?branch=master
[pg8000]:https://github.com/tlocke/pg8000
