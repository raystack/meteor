FROM golang:1.16-stretch as base
WORKDIR /build/
COPY . .
RUN ["make", "build-dev"]

FROM alpine:latest
COPY --from=base /build/meteor /usr/bin/meteor
RUN apk update
RUN apk add ca-certificates

CMD ["meteor"]
