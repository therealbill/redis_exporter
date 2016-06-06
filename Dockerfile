FROM alpine:latest
MAINTAINER therealbill

ENV GOPATH /go
COPY . /go/src/github.com/therealbill/redis_exporter

RUN apk add --update -t build-deps go git mercurial make \
    && apk add -u musl && rm -rf /var/cache/apk/* \
    && cd /go/src/github.com/therealbill/redis_exporter \
    && go get && go build && cp redis_exporter /bin/redis_exporter \
    && rm -rf /go && apk del --purge build-deps

EXPOSE     9121
ENTRYPOINT [ "/bin/redis_exporter" ]
