# https://github.com/hyrise/hyrise/wiki/Docker-Image
FROM hyrise/opossum-ci:19.10
LABEL maintainer="upigorsch@me.com"
ENV OPOSSUM_HEADLESS_SETUP=true
# From now on, we're doing additional stuff that would be done manually otherwise
RUN apt-get update \
    && apt-get upgrade -y \
    && cd /usr/local/ \
    && git clone https://github.com/hyrise/hyrise.git hyrise-git \ 
    && cd hyrise-git \
    && git checkout bp1920 \
    && ./install_dependencies.sh \
    && mkdir /usr/local/hyrise-git/cmake-build-release \
    && cd /usr/local/hyrise-git/cmake-build-release \
    && CXXFLAGS=-fdiagnostics-color=always cmake -DENABLE_NUMA_SUPPORT=Off -DCMAKE_BUILD_TYPE=Release .. \
    && make hyriseConsole hyriseServer CompressionPlugin ClusteringPlugin IndexSelectionPlugin \
    && mkdir -p /usr/local/hyrise/lib \
    && cd /usr/local/hyrise-git/cmake-build-release/ \ 
    && cp hyriseServer hyriseConsole /usr/local/hyrise/ \
    && cp lib/libC* lib/libI* /usr/local/hyrise/lib/ \
    && apt-get autoremove 
ENTRYPOINT ["/usr/local/hyrise/hyriseServer"]
