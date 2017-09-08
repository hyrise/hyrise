FROM ubuntu:17.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y sudo wget cmake libboost-all-dev clang-format gcovr python2.7 gcc-6 clang git build-essential llvm libtbb-dev bash-completion man \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && ln -sf /usr/bin/llvm-symbolizer-3.8 /usr/bin/llvm-symbolizer \
    && wget -q http://dl.bintray.com/boostorg/release/1.65.0/source/boost_1_65_0.tar.gz \
    && tar -xzf boost_1_65_0.tar.gz \
    && cd boost_1_65_0 \
    && ./bootstrap.sh \
    && ./b2 install -j $(nproc) \
    && cd .. \
    && rm -rf boost_1_65_0 boost_1_65_0.tar.gz
    

ENV OPOSSUM_HEADLESS_SETUP=true
