#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Minimal setup for Ubuntu 20.04.
set -eufx -o pipefail
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh

install_ubuntu_18.04() {    
    sudo apt-get install -y wget curl tar zip unzip git
    
    wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc  2>/dev/null | gpg --dearmor - | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
    sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/  bionic main' -y

    run_command_with_retry sudo add-apt-repository ppa:team-xbmc/ppa -y
    # ubuntu-toolchain-r/test is used for gcc-9 installation in ubuntu 18.04, but it is unsable due to
    # various issue as 1) The team named '~ubuntu-toolchain-r' has no PPA named 'ubuntu/test'
    # 2) Error: retrieving gpg key timed out.
    # add retry logic to connect this ppa to fix the issue.
    run_command_with_retry sudo add-apt-repository ppa:ubuntu-toolchain-r/test -y
    run_command_with_retry sudo apt-get update -y
    run_command_with_retry sudo apt update -y
    sudo apt-get install openjdk-8-jdk -y
    sudo apt install -y cmake ccache ninja-build checkinstall git libssl-dev libdouble-conversion-dev libgoogle-glog-dev libbz2-dev libgflags-dev libgmock-dev libevent-dev libre2-dev libsnappy-dev liblzo2-dev bison flex tzdata wget
    sudo apt-get install -y build-essential maven llvm-10 clang-10 libdwarf-dev libcurl4-openssl-dev autoconf pkg-config liblzo2-dev libiberty-dev libtool zlib1g-dev
    sudo apt-get -y install zip unzip tar

    sudo apt-get install -y gcc-9 g++-9
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 900 --slave /usr/bin/g++ g++ /usr/bin/g++-9
    sudo apt install -y libiberty-dev libxml2-dev libkrb5-dev libgsasl7-dev libuuid1 uuid-dev

    cd ..
    wget https://github.com/lz4/lz4/archive/v1.9.2.tar.gz
    tar -zxvf v1.9.2.tar.gz
    cd lz4-1.9.2
    make
    sudo make install

    git clone https://github.com/facebook/zstd.git 
    cd zstd
    make install

    cd ..
    # install automake 1.16.5
    wget -O automake-1.16.5.tar.gz http://ftp.gnu.org/gnu/automake/automake-1.16.5.tar.gz 
    tar -xzf automake-1.16.5.tar.gz
    cd automake-1.16.5
    ./configure --prefix=/usr
    make
    sudo make install
}

# Folly must be built with the same compiler flags so that some low level types
# are the same size.
CPU_TARGET="${CPU_TARGET:-avx}"
COMPILER_FLAGS=$(get_cxx_flags "$CPU_TARGET")
export COMPILER_FLAGS
FB_OS_VERSION=v2023.12.04.00
FMT_VERSION=10.1.1
BOOST_VERSION=boost-1.84.0
NPROC=$(getconf _NPROCESSORS_ONLN)
DEPENDENCY_DIR=${DEPENDENCY_DIR:-$(pwd)}
export CMAKE_BUILD_TYPE=Release
SUDO="${SUDO:-"sudo --preserve-env"}"

OsVersion=$(lsb_release -r)
OsVersionNumOnly=$(cut -f2 <<< "$OsVersion")
if  [ "$OsVersionNumOnly" != "18.04" ]; then
  # Install all velox and folly dependencies.
  ${SUDO} apt update
  ${SUDO} apt install -y libunwind-dev
  ${SUDO} apt install -y \
    g++ \
    cmake \
    ccache \
    ninja-build \
    checkinstall \
    git \
    libc-ares-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libicu-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libbz2-dev \
    libgflags-dev \
    libgmock-dev \
    libevent-dev \
    liblz4-dev \
    libzstd-dev \
    libre2-dev \
    libsnappy-dev \
    libsodium-dev \
    libthrift-dev \
    liblzo2-dev \
    libelf-dev \
    libdwarf-dev \
    bison \
    flex \
    libfl-dev \
    tzdata \
    wget
else
    install_ubuntu_18.04
    echo "Dependencies installation is done in ubuntu18."
fi

function run_and_time {
  time "$@"
  { echo "+ Finished running $*"; } 2> /dev/null
}

function prompt {
  (
    while true; do
      local input="${PROMPT_ALWAYS_RESPOND:-}"
      echo -n "$(tput bold)$* [Y, n]$(tput sgr0) "
      [[ -z "${input}" ]] && read input
      if [[ "${input}" == "Y" || "${input}" == "y" || "${input}" == "" ]]; then
        return 0
      elif [[ "${input}" == "N" || "${input}" == "n" ]]; then
        return 1
      fi
    done
  ) 2> /dev/null
}

function install_protobuf {
  wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protobuf-all-21.4.tar.gz
  tar -xzf protobuf-all-21.4.tar.gz
  cd protobuf-21.4
  ./configure \
      CXXFLAGS="-fPIC" \
      --prefix=/usr
  make "-j$(nproc)"
  sudo make install
  sudo ldconfig
}


function install_fmt {
  github_checkout fmtlib/fmt "${FMT_VERSION}"
  cmake_install -DFMT_TEST=OFF
}

function install_boost {
  github_checkout boostorg/boost "${BOOST_VERSION}" --recursive
  ./bootstrap.sh --prefix=/usr/local
  ${SUDO} ./b2 "-j$(nproc)" -d0 install threading=multi
}

function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
   cmake_install -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

function install_conda {
  mkdir -p conda && cd conda
  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
  MINICONDA_PATH=/opt/miniconda-for-velox
  bash Miniconda3-latest-Linux-x86_64.sh -b -u $MINICONDA_PATH
}

function install_velox_deps {
  run_and_time install_fmt
  run_and_time install_boost
  run_and_time install_protobuf
  run_and_time install_folly
  #run_and_time install_conda
}

(return 2> /dev/null) && return # If script was sourced, don't run commands.

(
  if [[ $# -ne 0 ]]; then
    for cmd in "$@"; do
      run_and_time "${cmd}"
    done
  else
    install_velox_deps
  fi
)

echo "All deps for Velox installed! Now try \"make\""
