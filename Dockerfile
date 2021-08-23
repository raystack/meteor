FROM golang:1.16-stretch as builder
WORKDIR /build/
COPY . .
RUN ["make"]

FROM debian:stretch
WORKDIR /opt/meteor
COPY --from=builder /build/meteor /opt/meteor/meteor
RUN apt-get update
RUN apt-get install -y --no-install-recommends ca-certificates

CMD ["./meteor"]
