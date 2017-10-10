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
            # check, for each programme individually with brew, whether it is already installed
            # due to brew issues on MacOS after system upgrade
            for formula in boost cmake gcc clang-format@3.8 gcovr tbb autoconf automake libtool pkg-config readline sqlite3; do
                # if brew formula is installed
                if brew ls --versions $formula > /dev/null; then
                    continue
                fi
                if ! brew install $formula; then
                    echo "Error during brew formula $formula installation."
                    exit 1
                fi
            done

            # clang-format is keg-only and needs to be explicitly symlinked into /usr/local
            ln -s /usr/local/Cellar/clang-format\@3.8/3.8.0/bin/clang-format /usr/local/bin/clang-format-3.8

            if ! git submodule update --init --recursive; then
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
                sudo apt-get install -y libboost-all-dev clang-format-3.8 gcovr python2.7 gcc-6 clang llvm libnuma-dev libnuma1 libtbb-dev build-essential autoconf libtool cmake libreadline-dev libsqlite3-dev &

                if ! git submodule update --init --recursive; then
                    echo "Error during installation."
                    exit 1
                fi

                wait
                apt=$?
                if [ $apt -ne 0 ] then
                    echo "Error during installation."
                    exit 1
                fi
            else
                echo "Error during installation."
                exit 1
            fi
        fi
    fi
fi

exit 0
