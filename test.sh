set -e

go get -d -t ./...
go test -v
