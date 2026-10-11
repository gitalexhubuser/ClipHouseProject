FROM ghcr.io/home-assistant/base:latest

RUN apk add --no-cache python3

WORKDIR /app
COPY ClipHouse.py /app/ClipHouse.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh

LABEL io.hass.version="4.1.0" \
      io.hass.type="app" \
      io.hass.arch="amd64|aarch64"

CMD ["/run.sh"]
