#!/bin/bash

if [[ -z $OPOSSUM_HEADLESS_SETUP ]]; then
    read -p 'This script installs the dependencies of Hyrise. It might upgrade already installed packages. Continue? [y|n] ' -n 1 -r < /dev/tty
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
            for formula in boost cmake gcc clang-format@3.8 gcovr tbb pkg-config readline ncurses sqlite3 parallel; do
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

            # Needed for proper building under macOS
            xcode-select --install

            if ! git submodule update --jobs 5 --init --recursive; then
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

                requiredgccmajor="7"
                requiredgccminor="2"

                requiredgcc="${requiredgccmajor}.${requiredgccminor}"
                availablegcc=$(apt-cache policy gcc-7 2>&1 | grep '^  Candidate: ' | sed -e 's/^  Candidate: \([0-9]\.[0-9]\).*/\1/')
                if [[ -z "${availablegcc// }" || "${availablegcc}" < "${requiredgcc}" ]]; then
                    if [[ -z $OPOSSUM_HEADLESS_SETUP ]]; then
                        read -p "The required GCC version ${requiredgcc} is not available in your installed repositories. OK to add ppa:ubuntu-toolchain-r/test? [y|n] " -n 1 -r < /dev/tty
                    else
                        REPLY="y"
                    fi
                    echo
                    if echo $REPLY | grep -E '^[Yy]$' > /dev/null; then
                        sudo apt-get install -y software-properties-common
                        # We are using xenial here because 17.04 does not have a gcc-7.2 package
                        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 1E9377A2BA9EF27F
                        sudo add-apt-repository -y "deb http://ppa.launchpad.net/ubuntu-toolchain-r/test/ubuntu xenial main"
                        sudo apt-get update
                    else
                        echo "Ok, you will have to install gcc $requiredgcc yourself."
                    fi
                fi

                requiredclang="5.0"
                availableclang=$(apt-cache policy clang-$requiredclang 2>&1 | grep '^  Candidate: ')
                if [[ -z "${availableclang// }" ]]; then
                    if [[ -z $OPOSSUM_HEADLESS_SETUP ]]; then
                        read -p "The required Clang version ${requiredclang} is not available in your installed repositories. OK to add http://apt.llvm.org/$(lsb_release -cs)/? [y|n] " -n 1 -r < /dev/tty
                    else
                        REPLY="y"
                    fi
                    echo
                    if echo $REPLY | grep -E '^[Yy]$' > /dev/null; then
                        sudo apt-get install -y software-properties-common wget
                        sudo wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
                        sudo add-apt-repository -y "deb http://apt.llvm.org/$(lsb_release -cs)/ llvm-toolchain-$(lsb_release -cs)-$requiredclang main"
                        sudo apt-get update
                    else
                        echo "Ok, you will have to install clang $requiredclang yourself."
                    fi
                fi

                boostall=$(apt-cache search --names-only '^libboost1.[0-9]+-all-dev$' | sort | tail -n 1 | cut -f1 -d' ')
                sudo apt-get install --no-install-recommends -y clang-$requiredclang clang-format-3.8 gcovr python2.7 gcc-${requiredgccmajor} g++-${requiredgccmajor} llvm libnuma-dev libnuma1 libtbb-dev build-essential cmake libreadline-dev libncurses5-dev libsqlite3-dev parallel $boostall &

                if ! git submodule update --jobs 5 --init --recursive; then
                    echo "Error during installation."
                    exit 1
                fi

                wait $!
                apt=$?
                if [ $apt -ne 0 ]; then
                    echo "Error during installation."
                    exit 1
                fi

                sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-$requiredgccmajor 60 --slave /usr/bin/g++ g++ /usr/bin/g++-$requiredgccmajor
                sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-$requiredclang 60 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-$requiredclang
            else
                echo "Error during installation."
                exit 1
            fi
        fi
    fi
fi

exit 0
