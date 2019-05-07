# https://github.com/hyrise/hyrise/wiki/Docker-Image

FROM ubuntu:19.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion \
        bc \
        ccache \
        clang-7 \
        clang-format-7 \
        clang-tidy-7 \
        cmake \
        curl \
        gcovr \
        gcc-8 \
        g++-8 \
        git \
        $(apt-cache search --names-only '^libboost1.[0-9]+-all-dev$' | sort | tail -n 1 | cut -f1 -d' ') \
        libclang-7-dev \
        libnuma-dev \
        libncurses5-dev \
        libnuma1 \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        llvm \
        llvm-7-tools \
        man \
        parallel \
        python2.7 \
        python-pip \
        python-pexpect \
        sudo \
        valgrind \
        libpq-dev \
        systemtap \
        systemtap-sdt-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

ENV OPOSSUM_HEADLESS_SETUP=true

