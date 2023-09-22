# Build args
ARG MODULE_NAME=go-mades-bench

#FROM gcr.io/distroless/static AS final
FROM alpine:latest
# Build args
ARG MODULE_NAME
ENV MODULE_NAME=${MODULE_NAME}
# Copy our static executable
# Note: bin is renamed to "app" as we cannot use build args or env vars in ENTRYPOINT when using scratch image
COPY bin/${MODULE_NAME} /usr/bin/${MODULE_NAME}

# Use an unprivileged user.
RUN addgroup -S -g 65532 nonroot && adduser -D -S nonroot -G nonroot -u 65532

RUN mkdir -p /etc/go-mades/bench && chown nonroot:nonroot /etc/go-mades/bench 
RUN mkdir -p /var/go-mades/bench && chown nonroot:nonroot /var/go-mades/bench 
RUN mkdir -p /var/log/go-mades/bench && chown nonroot:nonroot /var/log/go-mades/bench 

USER nonroot:nonroot

WORKDIR /etc/go-mades/bench
ENTRYPOINT ["/usr/bin/go-mades-bench"] 