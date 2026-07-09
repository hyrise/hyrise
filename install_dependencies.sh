#!/bin/bash

if [[ -z $HYRISE_HEADLESS_SETUP ]]; then
    read -p 'This script installs the dependencies of Hyrise. It might upgrade already installed packages. Continue? [y|n] ' -n 1 -r < /dev/tty
else
    REPLY="y"
fi

echo
if echo $REPLY | grep -E '^[Yy]$' > /dev/null; then
    unamestr=$(uname)
    if [[ "$unamestr" == 'Darwin' ]]; then
        brew --version 2>/dev/null || /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

        echo "Installing dependencies (this may take a while)..."
        if brew update >/dev/null; then
            # check, for each program (aka. formula) individually with brew, whether it is already installed due to brew issues on macOS after system upgrade
            # NOTE: The Mac CI server does not execute the install_dependencies.sh - formulas need to be installed manually.
            for formula in autoconf boost cmake coreutils dos2unix graphviz libpq ncurses parallel pkg-config postgresql readline sqlite3 tbb; do
                # if brew formula is installed
                if brew ls --versions $formula > /dev/null; then
                    continue
                fi
                if ! brew install $formula; then
                    echo "Error during brew formula $formula installation."
                    exit 1
                fi
            done

            if ! brew install llvm; then
                echo "Error during llvm/clang installation."
                exit 1
            fi

            if ! git submodule update --jobs 5 --init --recursive --depth 1; then
                echo "Error during git fetching submodules."
                exit 1
            fi

            if ! pip3 install -r requirements.txt; then
                echo "Error during installation of python requirements."
                exit 1
            fi
        else
            echo "Error during installation."
            exit 1
        fi
    elif [[ "$unamestr" == 'Linux' ]]; then
        if [ -f /etc/lsb-release ] && cat /etc/lsb-release | grep DISTRIB_ID | grep Ubuntu >/dev/null; then
            echo "Installing dependencies (this may take a while)..."
            if sudo apt-get update >/dev/null; then

                # Packages added here should also be added to the Dockerfile
                if ! sudo apt-get install --no-install-recommends -y software-properties-common lsb-release git python3 python3-pip autoconf bash-completion bc libclang-rt-dev cmake curl dos2unix g++-15 gcc-15 clang-19 clang-21 graphviz libboost-all-dev libhwloc-dev libncurses5-dev libnuma-dev libnuma1 libpq-dev libreadline-dev libsqlite3-dev libtbb-dev man parallel postgresql-server-dev-all time valgrind wget; then
                    echo "Error during apt-get installations."
                    exit 1
                fi

                if ! wget https://apt.llvm.org/llvm.sh -O llvm.sh; then
                    echo "Error while downloading llvm.sh."
                    exit 1
                fi

                if ! chmod +x llvm.sh; then
                    echo "Error while making llvm.sh executable."
                    exit 1
                fi

                if ! sudo ./llvm.sh 21 all; then
                    echo "Error during LLVM 21 installation."
                    exit 1
                fi

                if ! sudo apt-get install  -y bolt-21 libbolt-21-dev ; then
                    echo "Error during bolt installations."
                    exit 1
                fi

                if ! git submodule update --jobs 5 --init --recursive; then
                    echo "Error during git fetching submodules."
                    exit 1
                fi

                if ! pip3 install --break-system-packages -r requirements.txt; then
                    echo "Error during installation of python requirements."
                    exit 1
                fi

                sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-15 90 --slave /usr/bin/g++ g++ /usr/bin/g++-15
                sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-21 90 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-21 --slave /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-21 --slave /usr/bin/llvm-profdata llvm-profdata /usr/bin/llvm-profdata-21 --slave /usr/bin/llvm-cov llvm-cov /usr/bin/llvm-cov-21 --slave /usr/bin/clang-format clang-format /usr/bin/clang-format-21  --slave /usr/bin/ld.lld ld.lld /usr/bin/ld.lld-21 --slave /usr/bin/llvm-bolt llvm-bolt /usr/bin/llvm-bolt-21 --slave /usr/bin/merge-fdata merge-fdata /usr/bin/merge-fdata-21 --slave /usr/lib/libbolt_rt_instr.a libbolt_rt_instr.a /usr/lib/llvm-21/lib/libbolt_rt_instr.a
            else
                echo "Error during installation."
                exit 1
            fi
        else
            echo "Unsupported system. You might get the install script to work if you remove the '/etc/lsb-release' line, but you will be on your own."
            exit 1
        fi
    else
        echo "Unsupported operating system $unamestr."
        exit 1
    fi
fi

echo "Dependencies installed successfully."
exit 0
