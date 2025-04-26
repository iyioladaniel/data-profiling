#!/bin/bash

cd /opt

wget https://www.python.org/ftp/python/3.9.21/Python-3.9.21.tgz

tar xfv Python-3.9.21.tgz

cd /Python-3.9.21

sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev libgdbm-dev libnss3-dev libedit-dev libc6-dev

./configure --enable-optimizations

sudo make altinstall