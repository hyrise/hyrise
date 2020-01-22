# avoid boost warnings:
# https://stackoverflow.com/questions/35750089/why-cmake-ignores-the-boost-no-boost-cmake-environment-variable
set(Boost_NO_BOOST_CMAKE TRUE CACHE BOOL "" FORCE)