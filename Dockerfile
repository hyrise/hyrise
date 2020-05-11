# https://github.com/hyrise/hyrise/wiki/Docker-Image

FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion \
        bc \
        clang-9 \
        clang-10 \
        clang-format-10 \
        clang-tidy-10 \
        cmake \
        curl \
        g++-9 \
        gcc-9 \
        gcovr \
        git \
        graphviz \
        libboost1.71-all-dev \
        libhwloc-dev \
        libncurses5-dev \
        libnuma-dev \
        libnuma1 \
        libpq-dev \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        lld \
        lsb-release \
        man \
        parallel \
        postgresql-server-dev-all \
        python3 \
        python3-pexpect \
        software-properties-common \
        sudo \
        systemtap \
        systemtap-sdt-dev \
        valgrind \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

ENV OPOSSUM_HEADLESS_SETUP=true
