FROM golang:1.25-alpine AS builder

ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown
ARG BRAND_NAME=OpenMark
ARG VENDOR=AstraForce
ARG SUPPORT_URL=sup

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w \
      -X 'rovnoMark/internal/version.Version=${VERSION}' \
      -X 'rovnoMark/internal/version.GitCommit=${GIT_COMMIT}' \
      -X 'rovnoMark/internal/version.BuildDate=${BUILD_DATE}' \
      -X 'rovnoMark/internal/brand.Name=${BRAND_NAME}' \
      -X 'rovnoMark/internal/brand.Vendor=${VENDOR}' \
      -X 'rovnoMark/internal/brand.SupportURL=${SUPPORT_URL}'" \
    -o /bin/marking-service ./cmd/gateway

FROM scratch

ENV TZ=Europe/Moscow

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /bin/marking-service /bin/marking-service

EXPOSE 8000

CMD ["/bin/marking-service", "--port", "8000", "--debug", "--data-dir", "/app/data"]