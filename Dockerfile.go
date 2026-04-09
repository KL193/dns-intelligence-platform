# Multi-stage build for Go services (normalizer, geo enricher, optional generator)

FROM golang:1.22-alpine AS builder

WORKDIR /src

# Install CA certs for outbound TLS if needed
RUN apk add --no-cache ca-certificates

# Go module files
COPY go.mod go.sum ./
RUN go mod download

# Application source
COPY . .

# Build binaries (CGO disabled – these services do not use RocksDB)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/normalizer ./cmd/normalizer
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/enrich-geo ./cmd/enrich-dns-intel-geo-go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/raw-gateway-publisher ./cmd/raw-gateway-publisher
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/dns-query-api ./cmd/dns-query-api


FROM alpine:3.19

RUN addgroup -S app && adduser -S app -G app
WORKDIR /app

# Copy binaries
COPY --from=builder /out/normalizer /app/normalizer
COPY --from=builder /out/enrich-geo /app/enrich-geo
COPY --from=builder /out/raw-gateway-publisher /app/raw-gateway-publisher
COPY --from=builder /out/dns-query-api /app/dns-query-api

# Geo data used by the enricher
COPY geo-data /app/geo-data

RUN chown -R app:app /app
USER app

# Default NATS URL inside Docker network
ENV NATS_URL=nats://nats:4222

# Default command runs the normalizer; docker-compose services
# override this CMD with their own command arrays as needed.
CMD ["/app/normalizer"]
