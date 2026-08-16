FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server
RUN CGO_ENABLED=0 go build -o /out/kvctl ./cmd/kvctl

FROM alpine:3.20
RUN adduser -D -u 10001 kvstore
COPY --from=build /out/server /usr/local/bin/server
COPY --from=build /out/kvctl /usr/local/bin/kvctl
USER kvstore
WORKDIR /data
EXPOSE 8080 6380
ENTRYPOINT ["server"]
