FROM golang:1.23-alpine

RUN apk add --no-cache curl redis

ENV CGO_ENABLED=0

RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.60.1

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -v ./...

RUN golangci-lint run ./...

# Tests need a running redis server
RUN nohup sh -c "redis-server &" && \
    sleep 2 && \
    go test -v ./...
