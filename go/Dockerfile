FROM golang:1.22-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN go mod tidy \
    && go build -o /out/bbs-broker ./cmd/broker \
    && go build -o /out/bbs-proxy ./cmd/proxy \
    && go build -o /out/bbs-server ./cmd/server \
    && go build -o /out/bbs-client ./cmd/client

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends libzmq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/ /usr/local/bin/
