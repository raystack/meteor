FROM golang:1.26-bookworm AS builder
WORKDIR /build/
COPY . .
RUN make build-dev

FROM alpine:latest AS base
RUN apk update && apk add --no-cache ca-certificates curl tzdata
CMD ["meteor"]

FROM base AS dev
COPY --from=builder /build/meteor /usr/bin/meteor

FROM base
COPY meteor /usr/bin/meteor
