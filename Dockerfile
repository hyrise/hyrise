# IMPORTANT: Changes in this file do not automatically affect the Docker image used by the CI server.
# You need to build and push it manually, see the wiki for details:
# https://github.com/hyrise/hyrise/wiki/Docker-Image

FROM ubuntu:22.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
    autoconf \
    bash-completion \
    bc \
    clang-11 \
    clang-14 \
    clang-format-14 \
    clang-tidy-14 \
    cmake \
    curl \
    dos2unix \
    g++-9 \
    g++-11 \
    gcc-9 \
    gcc-11 \
    gcovr \
    git \
    graphviz \
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
    software-properties-common \
    sudo \
    valgrind \
    ccache \
    linux-tools-common \
    linux-tools-generic \
    linux-cloud-tools-generic \
    linux-tools-`uname -r` \
    bpftrace \
    lldb \
    fio \
    strace \
    openssh-client \
    ninja-build \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-14 /usr/bin/llvm-symbolizer \
    && pip3 install scipy pandas matplotlib # preload large Python packages (installs numpy and others)
RUN cd /home && curl -L  http://downloads.sourceforge.net/project/boost/boost/1.81.0/boost_1_81_0.tar.gz --output boost_1_81_0.tar.gz  \
  && tar xfz boost_1_81_0.tar.gz \
  && rm boost_1_81_0.tar.gz \
  && cd boost_1_81_0 \
  && ./bootstrap.sh --prefix=/usr/local --with-libraries=program_options \
  && ./b2 install \
  && cd /home \
  && rm -rf boost_1_81_0
RUN curl -fsSL https://code-server.dev/install.sh | sh
ENV HYRISE_HEADLESS_SETUP=true
