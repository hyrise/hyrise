#!/bin/bash

if [[ -z $OPOSSUM_HEADLESS_SETUP ]]; then
    BOOST_INSTALLED=$(dpkg-query -W --showformat='${Status}\n' libboost1.67-dev 2>/dev/null | grep "install ok installed")
    if [ "" != "$BOOST_INSTALLED" ]; then
         read -p 'libboost1.67-dev is installed but 1.70 is required. Ok to remove 1.67-dev? [y|n] ' -n 1 -r < /dev/tty
        echo
         if echo $REPLY | grep -E '^[Yy]$' > /dev/null; then
             sudo apt-get remove libboost1.67-dev
         fi
    fi
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
            # python2.7 is preinstalled on macOS
            # check, for each program (aka. formula) individually with brew, whether it is already installed due to brew issues on MacOS after system upgrade
            # NOTE: The Mac CI server does not execute the install.sh - formulas need to be installed manually.
            for formula in autoconf boost cmake graphviz libpq ncurses parallel pkg-config postgresql readline sqlite3 tbb; do
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

            if ! git submodule update --jobs 5 --init --recursive; then
                echo "Error during installation."
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
                # packages added here should also be added to the Dockerfile
                sudo apt-get install --no-install-recommends -y software-properties-common
                sudo add-apt-repository -y ppa:mhier/libboost-latest
                sudo apt-get update
                sudo apt-get install --no-install-recommends -y autoconf bash-completion bc ccache clang-9 clang-format-9 clang-tidy-9 cmake curl g++-9 gcc-9 gcovr git graphviz libboost1.70-dev libhwloc-dev libncurses5-dev libnuma-dev libnuma1 libpq-dev libreadline-dev libsqlite3-dev libtbb-dev lld man parallel postgresql-server-dev-all python2.7 python-pexpect python-pip sudo systemtap systemtap-sdt-dev valgrind &

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

                sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 --slave /usr/bin/g++ g++ /usr/bin/g++-9
                sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-9 90 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-9 --slave /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-9 --slave /usr/bin/llvm-profdata llvm-profdata /usr/bin/llvm-profdata-9 --slave /usr/bin/llvm-cov llvm-cov /usr/bin/llvm-cov-9 --slave /usr/bin/clang-format clang-format /usr/bin/clang-format-9
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

exit 0
