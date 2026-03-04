FROM ubuntu:latest
LABEL authors="terry"

ENTRYPOINT ["top", "-b"]