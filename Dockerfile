FROM alpine:latest

COPY meteor /usr/bin/meteor
RUN apk update
RUN apk add ca-certificates curl tzdata

CMD ["meteor"]
