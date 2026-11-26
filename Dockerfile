# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

RUN --mount=type=cache,target=/go/pkg/mod \
    go build modernc.org/sqlite

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-w -s" -o /bin/marking-service ./cmd/gateway

FROM alpine:latest

RUN apk add --no-cache ca-certificates tzdata && mkdir -p /app/data
ENV TZ=Europe/Moscow

WORKDIR /app
COPY --from=builder /bin/marking-service /bin/marking-service

EXPOSE 8000
VOLUME ["/app/data"]

CMD ["/bin/marking-service", "--port", "8000", "--debug", "--data-dir", "/app/data"]