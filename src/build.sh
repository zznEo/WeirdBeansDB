#!/bin/sh
rm configure.ac
autoscan
cp configure.ac.bak configure.ac
aclocal
autoconf
autoheader
automake --add-missing
./configure CXXFLAGS= CFLAGS=
make