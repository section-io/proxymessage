FROM golang:1.10-alpine

RUN apk add --no-cache \
    redis \
    git

RUN go get -v \
    github.com/stretchr/testify \
    gopkg.in/redis.v5

WORKDIR /go/src/section.io/proxymessage
COPY *.go ./

RUN gofmt -e -s -d . 2>&1 | tee /gofmt.out && test ! -s /gofmt.out

# Tests need a running redis server
RUN nohup sh -c "redis-server &" && \
    sleep 2 && \
    go test -v ./...