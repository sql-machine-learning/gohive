# GoHive: A Go `database/sql` Driver for Hive

[![Build Status](https://travis-ci.org/sql-machine-learning/gohive.svg?branch=develop)](https://travis-ci.org/travis-ci/travis-web)

To access databases, Go programmers call the standard library `database/sql`, which relies on *drivers* to talk to database management systems.  GoHive is such a driver that talks to Hive via [HiveServer2](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2#SettingUpHiveServer2-HiveServer2), a Thrift server.


## For Users

GoHive is go-gettable.  Please run the following command to install it:

```bash
go get sqlflow.org/gohive
```

`sqlflow.org/gohive` is a [vanity import path](https://blog.bramp.net/post/2017/10/02/vanity-go-import-paths/) of GoHive.


## For Developers

Your contribution to GoHive is very welcome!  Please refer to [this document](docker/README.md) on how to build and test GoHive in a Docker container.


## License

GoHive comes with [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
