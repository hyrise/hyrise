#!/bin/bash

if [[ -z $OPOSSUM_HEADLESS_SETUP ]]; then
    read -p 'This script installs the dependencies of OpossumDB. It might upgrade already installed packages. Continue? [y|n] ' -n 1 -r < /dev/tty
else
    REPLY="y"
fi

echo
if echo $REPLY | grep -E '^[Yy]$' > /dev/null; then
    unamestr=$(uname)
    if [[ "$unamestr" == 'Darwin' ]]; then
        echo "Installing dependencies (this may take a while)..."
        if brew update >/dev/null; then
            # python2.7 is preinstalled on macOS
            if brew install premake boost gcc clang-format gcovr tbb autoconf automake libtool pkg-config; then
                if git submodule update --init --recursive; then
                    if CPPFLAGS="-Wno-deprecated-declarations" CFLAGS="-Wno-deprecated-declarations -Wno-implicit-function-declaration -Wno-shift-negative-value" make static -j $(sysctl -n hw.ncpu) --directory=third_party/grpc REQUIRE_CUSTOM_LIBRARIES_opt=true; then
                        echo "Installation successful"
                    else
                        echo "Error during gRPC installation."
                        exit 1
                    fi
                else
                    echo "Error during installation."
                    exit 1
                fi
            else
                echo "Error during installation."
                exit 1
            fi
        else
            echo "Error during installation."
            exit 1
        fi
    elif [[ "$unamestr" == 'Linux' ]]; then
        if cat /etc/lsb-release | grep DISTRIB_ID | grep Ubuntu >/dev/null; then
            echo "Installing dependencies (this may take a while)..."
            if sudo apt-get update >/dev/null; then
                if sudo apt-get install -y premake4 libboost-all-dev clang-format gcovr python2.7 gcc-6 clang llvm libnuma-dev libnuma1 libtbb-dev build-essential autoconf libtool; then
                    if git submodule update --init --recursive; then
                        if CPPFLAGS="-Wno-deprecated-declarations" CFLAGS="-Wno-deprecated-declarations -Wno-implicit-function-declaration -Wno-shift-negative-value" make static -j $(cat /proc/cpuinfo | grep processor | wc -l) --directory=third_party/grpc REQUIRE_CUSTOM_LIBRARIES_opt=true; then
                            echo "Installation successful"
                        else
                            echo "Error during gRPC installation."
                            exit 1
                        fi
                    else
                        echo "Error during installation."
                        exit 1
                    fi
                else
                    echo "Error during installation."
                    exit 1
                fi
            else
                echo "Error during installation."
                exit 1
            fi
        fi
    fi
    
    make -f script/install_git_hooks
    (mkdir build; cd build; cmake ..)
fi

exit 0
