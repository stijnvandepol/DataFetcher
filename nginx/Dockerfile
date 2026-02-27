FROM nginx:alpine

# Genereer self-signed SSL certificaat
RUN apk add --no-cache openssl && \
    mkdir -p /etc/nginx/ssl && \
    openssl req -x509 -nodes -days 3650 \
      -newkey rsa:2048 \
      -keyout /etc/nginx/ssl/key.pem \
      -out /etc/nginx/ssl/cert.pem \
      -subj "/C=US/ST=State/L=City/O=Org/CN=localhost" && \
    apk del openssl

# Rate limiting config
RUN echo 'limit_req_zone $binary_remote_addr zone=app:10m rate=10r/s;' > /etc/nginx/conf.d/rate_limit.conf

COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80 443
