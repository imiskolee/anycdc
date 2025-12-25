FROM alpine:latest

RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    && rm -rf /var/cache/apk/*

RUN mkdir -p /opt/anycdc/static

COPY cmd/server/anycdc-server /opt/anycdc/

COPY cmd/dashboard/dist /opt/anycdc/static

RUN chmod +x /opt/anycdc/anycdc-server
WORKDIR /opt/anycdc
CMD ["/opt/anycdc/anycdc-server","-config","/opt/anycdc/config.yaml"]



