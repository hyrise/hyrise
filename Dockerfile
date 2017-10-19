# https://github.com/hyrise/zweirise/wiki/Docker-Image

FROM ubuntu:17.10
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        bash-completion \
        build-essential \
        ccache \
        clang-5.0 \
        clang-format-3.8 \
        cmake \
        gcovr \
        git \
        $(apt-cache search --names-only '^libboost1.[0-9]+-all-dev$' | sort | tail -n 1 | cut -f1 -d' ') \
        libnuma-dev \
        libncurses5-dev \
        libnuma1 \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        llvm \
        man \
        parallel \
        python2.7 \
        sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

ENV OPOSSUM_HEADLESS_SETUP=true

