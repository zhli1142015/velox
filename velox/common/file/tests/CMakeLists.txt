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

add_library(velox_file_test_utils TestUtils.cpp FaultyFile.cpp FaultyFileSystem.cpp)

target_link_libraries(velox_file_test_utils PUBLIC velox_file)

set(VELOX_FILE_TEST_SOURCES FileTest.cpp FileInputStreamTest.cpp UtilsTest.cpp)

if(VELOX_ENABLE_IO_URING)
  list(APPEND VELOX_FILE_TEST_SOURCES AsyncLocalFileTest.cpp)
endif()

add_executable(velox_file_test ${VELOX_FILE_TEST_SOURCES})
add_test(velox_file_test velox_file_test)
target_link_libraries(
  velox_file_test
  PRIVATE
    velox_buffer
    velox_file
    velox_file_test_utils
    velox_temp_path
    GTest::gmock
    GTest::gtest
    GTest::gtest_main
)
