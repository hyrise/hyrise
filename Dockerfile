# https://github.com/hyrise/hyrise/wiki/Docker-Image

FROM ubuntu:18.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion \
        bc \
        ccache \
        clang-6.0 \
        clang-format-6.0 \
        clang-tidy-6.0 \
        cmake \
        curl \
        gcovr \
        gcc-8 \
        g++-8 \
        git \
        $(apt-cache search --names-only '^libboost1.[0-9]+-all-dev$' | sort | tail -n 1 | cut -f1 -d' ') \
        libclang-6.0-dev \
        libnuma-dev \
        libncurses5-dev \
        libnuma1 \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        llvm \
        llvm-6.0-tools \
        man \
        parallel \
        python2.7 \
        python-pip \
        sudo \
        valgrind \
        libpq-dev \
        systemtap \
        systemtap-sdt-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

ENV OPOSSUM_HEADLESS_SETUP=true

