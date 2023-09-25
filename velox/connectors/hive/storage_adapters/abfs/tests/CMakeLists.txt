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

add_executable(velox_abfs_test AbfsFileSystemTest.cpp AbfsUtilTest.cpp
                               AzuriteServer.cpp MockBlobStorageFileClient.cpp)
add_test(velox_abfs_test velox_abfs_test)
target_link_libraries(
  velox_abfs_test
  PRIVATE velox_file
          velox_abfs
          velox_core
          velox_exec_test_lib
          velox_hive_connector
          velox_dwio_common_exception
          velox_exec
          gtest
          gtest_main
          rt
          Azure::azure-storage-blobs
          Azure::azure-storage-files-datalake)

target_compile_options(velox_abfs_test PRIVATE -Wno-deprecated-declarations)

add_executable(velox_blob_client_test BlobClientProviderTest)
add_test(velox_blob_client_test velox_blob_client_test)
target_link_libraries(
  velox_blob_client_test
  velox_file
  velox_abfs
  velox_core
  velox_exec_test_lib
  velox_hive_connector
  velox_dwio_common_exception
  velox_exec
  gtest
  gtest_main
  Azure::azure-storage-blobs)
target_compile_options(
  velox_blob_client_test PRIVATE -Wno-deprecated-declarations)

add_executable(vegas_file_system_test VegasFileSystemTest AzuriteServer.cpp)
add_test(vegas_file_system_test vegas_file_system_test)
target_link_libraries(
  vegas_file_system_test
  velox_file
  velox_abfs
  velox_core
  velox_exec_test_lib
  velox_hive_connector
  velox_dwio_common_exception
  velox_exec
  gtest
  gtest_main
  rt
  Azure::azure-storage-blobs
  Azure::azure-storage-files-datalake)
target_compile_options(
  vegas_file_system_test PRIVATE -Wno-deprecated-declarations)
