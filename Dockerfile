# https://github.com/hyrise/hyrise/wiki/Docker-Image

FROM ubuntu:19.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion \
        bc \
        ccache \
        clang-8 \
        clang-format-8 \
        clang-tidy-8 \
        cmake \
        curl \
        g++-9 \
        gcc-9 \
        gcovr \
        git \
        graphviz \
        $(apt-cache search --names-only '^libboost1.[0-9]+-all-dev$' | sort | tail -n 1 | cut -f1 -d' ') \
        libncurses5-dev \
        libnuma-dev \
        libnuma1 \
        libpq-dev \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
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
