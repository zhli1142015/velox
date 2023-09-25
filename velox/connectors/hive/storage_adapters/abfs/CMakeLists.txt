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

# for generated headers

add_library(velox_abfs RegisterAbfsFileSystem.cpp BlobClientProviderFactory.cpp
                       ConnectionStringBlobClientProvider.cpp)

if(VELOX_ENABLE_ABFS)
  target_sources(
    velox_abfs
    PRIVATE AbfsFileSystem.cpp
            AbfsWriteFile.cpp
            AbfsUtils.cpp
            VegasReadFile.cpp
            vegas/VegasExecBase.cpp
            vegas/VegasJournalBase.cpp
            vegas/v1/VegasExecV1.cpp
            vegas/v1/VegasJournalV1.cpp
            vegas/client/VegasCacheClient.cpp
            vegas/client/VegasCacheConfig.cpp
            vegas/client/VegasMessageQueue.cpp
            vegas/client/VegasUxSocket.cpp
            vegas/client/SysUtils.cpp)
  target_link_libraries(
    velox_abfs
    PUBLIC velox_file
           velox_core
           velox_hive_config
           velox_dwio_common_exception
           Azure::azure-storage-blobs
           Azure::azure-storage-files-datalake
           Folly::folly
           glog::glog
           rt
           fmt::fmt)

  if(${VELOX_BUILD_TESTING})
    add_subdirectory(tests)
  endif()
endif()
