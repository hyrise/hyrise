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
        g++-8 \
        gcc-8 \
        gcovr \
        git \
        graphviz \
        $(apt-cache search --names-only '^libboost1.[0-9]+-all-dev$' | sort | tail -n 1 | cut -f1 -d' ') \
        libclang-7-dev \
        libncurses5-dev \
        libnuma-dev \
        libnuma1 \
        libpq-dev \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        llvm \
        llvm-7-tools \
        man \
        parallel \
        postgresql-server-dev-all \
        python2.7 \
        python-glob2 \
        python-pexpect \
        python-pip \
        sudo \
        systemtap \
        systemtap-sdt-dev \
        valgrind \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

ENV OPOSSUM_HEADLESS_SETUP=true
