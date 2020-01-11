# https://github.com/hyrise/hyrise/wiki/Docker-Image

FROM ubuntu:19.10
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y software-properties-common \
    && add-apt-repository -y ppa:mhier/libboost-latest \
    && apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion \
        bc \
        ccache \
        clang-9 \
        clang-format-9 \
        clang-tidy-9 \
        curl \
        g++-9 \
        gcc-9 \
        gcovr \
        git \
        graphviz \
        libboost1.70-dev \
        libhwloc-dev \
        libncurses5-dev \
        libnuma-dev \
        libnuma1 \
        libpq-dev \
        libreadline-dev \
        libsqlite3-dev \
        libtbb-dev \
        lld \
        man \
        parallel \
        postgresql-server-dev-all \
        python2.7 \
        python-pexpect \
        python-pip \
        sudo \
        systemtap \
        systemtap-sdt-dev \
        valgrind \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer

# Temporarily while cmake 3.16 is not yet in the repositories:
# TODO: Remove this once the default cmake version is >= 3.16, most likely not before Ubuntu 20.04. Then re-add cmake above.
RUN apt-get update && apt-get install -y wget libssl-dev && apt-get clean \
    && wget https://github.com/Kitware/CMake/releases/download/v3.16.0/cmake-3.16.0.tar.gz \
    && tar xf cmake-3.16.0.tar.gz \
    && cd cmake-3.16.0 \
    && ./configure && make -j && make install

ENV OPOSSUM_HEADLESS_SETUP=true
