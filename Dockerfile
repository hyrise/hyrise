# IMPORTANT: Changes in this file do not automatically affect the Docker image used by the CI server.
# You need to build and push it manually, see the wiki for details:
# https://github.com/hyrise/hyrise/wiki/Docker-Image

# While it would be desirable to use Python's virtual environments, they are not straightforward to use in Jenkins'
# scripted pipelines. With Python >= 3.11, we need to use --break-system-packages.

FROM ubuntu:23.10
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        autoconf \
        bash-completion \
        bc \
        clang-15 \
        clang-17 \
        clang-format-17 \
        clang-tidy-17 \
        clang-tools-17 \
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
        lld-17 \
        lsb-release \
        man \
        ninja-build \
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
    && pip3 install --break-system-packages scipy pandas matplotlib  # preload large Python packages (installs numpy and
                                                                       others).

ENV HYRISE_HEADLESS_SETUP=true
