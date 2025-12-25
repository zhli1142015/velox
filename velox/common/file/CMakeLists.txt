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
include_directories(.)

# Source files
set(VELOX_FILE_SOURCES File.cpp FileInputStream.cpp FileSystems.cpp Utils.cpp)

# Add AsyncLocalFile if io_uring is enabled
if(VELOX_ENABLE_IO_URING)
  list(APPEND VELOX_FILE_SOURCES AsyncLocalFile.cpp)
endif()

velox_add_library(velox_file ${VELOX_FILE_SOURCES})

# Link libraries
set(VELOX_FILE_PRIVATE_LIBS velox_buffer velox_common_base fmt::fmt glog::glog)

if(VELOX_ENABLE_IO_URING AND liburing_FOUND)
  list(APPEND VELOX_FILE_PRIVATE_LIBS liburing::uring)
endif()

velox_link_libraries(
  velox_file
  PUBLIC velox_exception Folly::folly
  PRIVATE ${VELOX_FILE_PRIVATE_LIBS}
)

if(${VELOX_BUILD_TESTING} OR ${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests)
endif()
