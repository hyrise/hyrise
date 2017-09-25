FROM ubuntu:17.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        bash-completion \
        build-essential \
        clang \
        clang-format \
        cmake \
        gcc-6 \
        gcovr \
        git \
        libboost-all-dev \
        libreadline-dev \
        libtbb-dev \
        llvm \
        man \
        python2.7 \
        sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

ENV OPOSSUM_HEADLESS_SETUP=true

