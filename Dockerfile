FROM alpine:3.13

COPY meteor /usr/bin/meteor

CMD ["./meteor"]
