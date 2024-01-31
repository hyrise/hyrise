# IMPORTANT: Changes in this file do not automatically affect the Docker image used by the CI server.
# You need to build and push it manually, see the wiki for details:
# https://github.com/hyrise/hyrise/wiki/Docker-Image

FROM ubuntu:23.10
ENV DEBIAN_FRONTEND noninteractive
ENV HYRISE_VENV_PATH "/hyrise_venv"
ENV PATH "${HYRISE_VENV_PATH}/bin:$PATH"
RUN apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion \
        bc \
        clang-13 \
        clang-17 \
        clang-format \
        clang-tidy \
        cmake \
        curl \
        dos2unix \
        g++-11 \
        gcc-11 \
        gcovr \
        git \
        graphviz \
        libboost1.81-all-dev \
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
        python3-pip \
        python3-venv \
        software-properties-common \
        sudo \
        valgrind \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-14 /usr/bin/llvm-symbolizer \
    && python3 -m venv "${HYRISE_VENV_PATH}/" \
    && pip3 install scipy pandas matplotlib # preload large Python packages (installs numpy and others)

ENV HYRISE_HEADLESS_SETUP=true
