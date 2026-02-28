FROM nginx:1.27-alpine AS certs

# Generate self-signed cert in throwaway stage
RUN apk add --no-cache openssl && \
    mkdir -p /certs && \
    openssl req -x509 -nodes -days 3650 \
      -newkey rsa:2048 \
      -keyout /certs/key.pem \
      -out /certs/cert.pem \
      -subj "/C=NL/ST=NL/L=NL/O=Internal/CN=localhost"

# ── Final stage (no openssl binary) ────────────────────────
FROM nginx:1.27-alpine

# Rate limiting config
RUN echo 'limit_req_zone $binary_remote_addr zone=app:10m rate=10r/s;' > /etc/nginx/conf.d/rate_limit.conf

COPY --from=certs /certs/ /etc/nginx/ssl/
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
