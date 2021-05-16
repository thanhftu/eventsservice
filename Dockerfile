# Build stage
FROM golang:1.16.3-alpine3.13 AS builder
WORKDIR /app
COPY . .
RUN go build -o main main.go
# RUN apk --no-cache add curl
# RUN curl -L https://github.com/golang-migrate/migrate/releases/download/v4.14.1/migrate.linux-amd64.tar.gz | tar xvz

# Run stage
FROM alpine:3.13
WORKDIR /app
COPY --from=builder /app/main .
# COPY --from=builder /app/migrate.linux-amd64 ./migrate
# COPY app.env .
# COPY start.sh .
# COPY wait-for .
# COPY db/migration ./migration

ENV LISTEN_URL=0.0.0.0:8181
EXPOSE 8181

CMD [ "/main" ]
# ENTRYPOINT [ "/start.sh" ]