FROM golang:1.16-stretch as builder
WORKDIR /build/
COPY . .
RUN ["make"]

FROM alpine:latest
RUN apk --no-cache add ca-certificates bash
WORKDIR /opt/meteor
COPY --from=builder /build/meteor /opt/meteor/meteor

# glibc compatibility library, since go binaries 
# don't work well with musl libc that alpine uses
RUN ["apk", "add", "libc6-compat"] 
CMD ["./meteor"]
