FROM debian

COPY meteor /opt/meteor/meteor

CMD ["./meteor"]
