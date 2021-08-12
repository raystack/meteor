FROM alpine:3.13

COPY meteor /opt/meteor/meteor

CMD ["./meteor"]
