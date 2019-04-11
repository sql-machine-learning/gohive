# GoHive: Build and Test in Docker Containers

GoHive is a Hive driver for Go's database API.  To build and test it, we need not only the building tools but also Hive.  For the convenience of contributors, we install all tools into a Docker image so could we run and test in a Docker container.

The general usage is that we check out the source code on the host computer, then we start a Docker container and run building tools in the container.  The critical point is that we map the source code directory into the container.  Feel free to use any of your favorite editor, Emacs, Vim, Eclipse, installed and running on the host.


## Check out the Source Code

Run the following command to clone GoHive to `$GOPATH/src/sqlflow.org/gohive` on your host computer:

```bash
go get sqlflow.org/gohive
```


## Build the Docker Image

Run the following command in the `/docker` directory to create the Docker image `gohive:dev`:

```bash
cd gohive/docker
docker build -t gohive:dev .
```


## Build and Test GoHive

The following command starts a container with Hive running inside, so could we build and test GoHive:

```bash
docker run --rm -it -v $GOPATH:/go \
    -w /go/src/sqlflow.org/gohive \
    gohive:dev bash
```

The `-v` option maps `$GOPATH` on the host to `/go` in the container.  Please be aware that the Dockerfile configures `/go` as the `$GOPATH` in the container.  After many lines of logs scroll up while the Hive server starts, there comes the shell prompt, where we can run the following command to build and run tests:

```bash
go test -v
```
