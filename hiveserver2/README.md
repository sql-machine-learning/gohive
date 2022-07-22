# HiveServer 2 Client in Go

For the convenience to access Hive from clients in various languages, the Hive developers created Hive Server, which is a Thrift service.  The currently well-used version is known as Hive Server 2.

To write a Hive Server 2 client in Go, we need to use the `thrift` command to compile the Thrift service definition file [`TCLIService.thrift`](https://github.com/apache/hive/blob/master/service-rpc/if/TCLIService.thrift) from Hive Server 2 codebase, into Go source code:

```bash
curl -sS https://raw.githubusercontent.com/apache/hive/rel/release-3.1.3/service-rpc/if/TCLIService.thrift > TCLIService.thrift
```

According to their [blog post](https://cwiki.apache.org/confluence/display/Hive/HowToContribute#HowToContribute-GeneratingThriftCode), the Hive developers recommends to use Thrift v0.14.1 to generate the Hive's auto-generated Thrift code:

```bash
docker run --rm -it -v $PWD:/work -w /work \
    anthonyroussel/thrift:0.14.1 \
    thrift -r --gen go TCLIService.thrift
```

The above command generates Go source code in the subdirectory `./gen-go/tcliservice`.

It doesn't look very probable for the Hive team to upgrade the Thrift version or the `TCLIService.thrift` file, so we don't expect that you might need to run the above command, and we include the generated Go source files in this Git repo.
