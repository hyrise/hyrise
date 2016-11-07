FROM ubuntu:16.10
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y sudo premake4 libboost-all-dev clang-format gcovr python2.7 gcc-6 clang git build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

