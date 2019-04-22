# GoHive: A Go `database/sql` Driver for Hive

[![Build Status](https://travis-ci.org/sql-machine-learning/gohive.svg?branch=develop)](https://travis-ci.org/travis-ci/travis-web)

A Hive-SQL-Driver for Go's [database/sql](https://golang.org/pkg/database/sql/) package.

## Features
- Standard [`datbase/sql`](https://golang.org/pkg/database/sql/) API.
- Light weight and fast.
- Native Go implemenation. No C/Python/Java bindings, just pure Go.
- Connection via [HiveServer2](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2#SettingUpHiveServer2-HiveServer2) Thrift server.

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

## Acknowledgement

This implementation is strongly inspired by https://github.com/derekgr/hivething.
