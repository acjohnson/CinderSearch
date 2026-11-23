# syntax=docker/dockerfile:1.7

FROM golang:1.22-alpine AS builder
WORKDIR /src

ARG VERSION=dev
ARG COMMIT=local

RUN apk add --no-cache ca-certificates git

COPY go.mod ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/cindersearch ./cmd/cindersearch

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 10001 -S cindersearch \
    && adduser -u 10001 -S -G cindersearch -h /var/lib/cindersearch cindersearch \
    && mkdir -p /var/lib/cindersearch \
    && chown -R cindersearch:cindersearch /var/lib/cindersearch

COPY --from=builder /out/cindersearch /usr/local/bin/cindersearch

USER 10001:10001
WORKDIR /var/lib/cindersearch

VOLUME ["/var/lib/cindersearch"]
EXPOSE 9200 6060

ENTRYPOINT ["/usr/local/bin/cindersearch"]
CMD ["serve", "--listen=:9200", "--nvme-dir=/var/lib/cindersearch"]
