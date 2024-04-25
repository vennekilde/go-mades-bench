# Build args
ARG BIN_NAME=go-mades-bench
ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src
COPY ./ ./

RUN apk add --no-cache make
RUN go build -ldflags="-s -w" -o ./bin/${BIN_NAME} .

#FROM gcr.io/distroless/static AS final
FROM alpine:latest as final
# Build args
ARG BIN_NAME
# Use an unprivileged user.
RUN addgroup -S -g 65532 nonroot && adduser -D -S nonroot -G nonroot -u 65532
# add bash shell
RUN apk add --no-cache bash

USER nonroot:nonroot

# Copy our static executable
COPY --from=build --chown=nonroot:nonroot /src/bin/${BIN_NAME} /usr/bin/${BIN_NAME}

WORKDIR /go-mades-bench

# Sleep main container processes forever.
# Intended usecase, is to shell into the container and use the cli that way.
ENTRYPOINT ["/bin/bash", "-c", "while true; do sleep 86400; done;"] 