#!/bin/bash

unamestr=$(uname)
if [[ "$unamestr" == 'Darwin' ]]; then
    echo "Installing dependencies (this may take a while)..."
    if brew update >/dev/null; then
        # python2.7 is preinstalled on macOS
        if brew install premake boost gcc clang-format gcovr; then
            if git submodule update --init; then
                echo "Installation successful"
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
            if sudo apt-get install premake4 libboost-all-dev clang-format gcovr python2.7 gcc-6 clang; then
                if git submodule update --init; then
                    echo "Installation successful."
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

exit 0