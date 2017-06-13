FROM ubuntu:17.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y software-properties-common --no-install-recommends \
    && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
    && apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion\
        build-essential \
        clang \
        clang-format \
        cmake \
        gcc-6 \
        gcc-7 \
        gcovr \
        git \
        libboost-all-dev \
        libnuma1 \
        libnuma-dev \
        libtbb-dev\
        libtool \
        llvm\
        man \
        python2.7 \
        sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

ENV OPOSSUM_HEADLESS_SETUP=true