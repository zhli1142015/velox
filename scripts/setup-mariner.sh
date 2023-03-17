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

set -eufx -o pipefail
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-helper-functions.sh

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

sudo tdnf install -y dnf

# Install extended mariner repos to get glog package.
sudo dnf install -y mariner-repos-extended

sudo dnf install -y \
    cmake \
    ccache \
    ninja-build \
    git \
    openssl-devel \
    icu-devel \
    glog-devel \
    gflags-devel \
    gmock-devel \
    gtest-devel \
    libevent-devel \
    re2-devel \
    lz4-devel \
    lzo-devel \
    snappy-devel \
    bison \
    flex-devel \
    tzdata \
    curl-devel
    #fmt-devel

sudo dnf install -y \
    build-essential \
    maven \
    clang

function run_and_time {
  time "$@"
  { echo "+ Finished running $*"; } 2> /dev/null
}

# Use fmt-devel from mariner packages. 
# https://dev.azure.com/msdata/A365/_workitems/edit/2835513
function install_fmt {
  github_checkout fmtlib/fmt "${FMT_VERSION}"
  cmake_install -DFMT_TEST=OFF
}

function install_boost {
  github_checkout boostorg/boost "${BOOST_VERSION}" --recursive
  ./bootstrap.sh --prefix=/usr/local
  ${SUDO} ./b2 "-j$(nproc)" -d0 install threading=multi
}

# Remove source build of double-conversion, folly & azure-sdk-for-cpp once available from mariner
# https://dev.azure.com/msdata/A365/_workitems/edit/2835512
function install_double_conversion {
  github_checkout google/double-conversion v3.3.0
  cmake_install -DBUILD_SHARED_LIBS=ON
}

function install_folly {
  github_checkout facebook/folly "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF -DFOLLY_HAVE_INT128_T=ON
}

function install_azure-storage-sdk-cpp {
  # Disable VCPKG to install additional static dependencies under the VCPKG installed path
  # instead of using system pre-installed dependencies.
  export AZURE_SDK_DISABLE_AUTO_VCPKG=ON
  vcpkg_commit_id=7a6f366cefd27210f6a8309aed10c31104436509
  github_checkout azure/azure-sdk-for-cpp azure-storage-files-datalake_12.8.0
  sed -i "s/set(VCPKG_COMMIT_STRING .*)/set(VCPKG_COMMIT_STRING $vcpkg_commit_id)/" cmake-modules/AzureVcpkg.cmake

  cd sdk/core/azure-core
  if ! grep -q "baseline" vcpkg.json; then
    # build and install azure-core with the version compatible with system pre-installed openssl
    openssl_version=$(openssl version -v | awk '{print $2}')
    if [[ "$openssl_version" == 1.1.1* ]]; then
      openssl_version="1.1.1n"
    fi
    sed -i "s/\"version-string\"/\"builtin-baseline\": \"$vcpkg_commit_id\",\"version-string\"/" vcpkg.json
    sed -i "s/\"version-string\"/\"overrides\": [{ \"name\": \"openssl\", \"version-string\": \"$openssl_version\" }],\"version-string\"/" vcpkg.json
  fi
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF

  cd -
  # install azure-storage-common
  cd sdk/storage/azure-storage-common
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF

  cd -
  # install azure-storage-blobs
  cd sdk/storage/azure-storage-blobs
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF

  cd -
  # install azure-storage-files-datalake
  cd sdk/storage/azure-storage-files-datalake
  cmake_install -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DBUILD_SHARED_LIBS=OFF
}

function install_velox_deps {
  run_and_time install_fmt
  run_and_time install_boost
  run_and_time install_double_conversion
  run_and_time install_folly
  run_and_time install_azure-storage-sdk-cpp
}

install_velox_deps

echo "All deps for Velox installed! Now try \"make\""
