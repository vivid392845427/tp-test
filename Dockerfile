FROM registry-mirror.pingcap.net/library/golang:1.16 as builder

ADD . /go-randgen
RUN cd /go-randgen/tp-test \
 && go build

FROM registry-mirror.pingcap.net/library/debian:buster

RUN apt-get update \
 && apt-get install -y default-mysql-client wget curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /go-randgen/tp-test/tp-test /usr/local/bin/tp-test

# hub.pingcap.net/test-store/tp-test
