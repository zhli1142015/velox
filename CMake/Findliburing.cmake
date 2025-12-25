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

# Find liburing library for io_uring support in Velox spill operations.
#
# This module defines:
#   liburing_FOUND       - True if liburing is found
#   liburing_INCLUDE_DIR - The include directory
#   liburing_LIBRARY     - The library to link against
#   liburing::uring      - Imported target
#
# Usage:
#   find_package(liburing)
#   if(liburing_FOUND)
#     target_link_libraries(mytarget liburing::uring)
#   endif()

include(FindPackageHandleStandardArgs)

# Only support Linux since io_uring is a Linux kernel feature
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(liburing_FOUND FALSE)
  return()
endif()

# Find the header
find_path(
  liburing_INCLUDE_DIR
  NAMES liburing.h
  PATHS /usr/include /usr/local/include
  DOC "liburing include directory"
)

# Find the library
find_library(
  liburing_LIBRARY
  NAMES uring
  PATHS
    /usr/lib
    /usr/local/lib
    /usr/lib64
    /usr/local/lib64
    /usr/lib/x86_64-linux-gnu
    /usr/lib/aarch64-linux-gnu
  DOC "liburing library"
)

# Verify we found everything
find_package_handle_standard_args(
  liburing
  REQUIRED_VARS liburing_LIBRARY liburing_INCLUDE_DIR
  FAIL_MESSAGE
    "Could not find liburing. Install liburing-dev (Ubuntu) or liburing-devel (CentOS/Fedora)"
)

if(liburing_FOUND)
  # Create imported target
  if(NOT TARGET liburing::uring)
    add_library(liburing::uring UNKNOWN IMPORTED)
    set_target_properties(
      liburing::uring
      PROPERTIES
        IMPORTED_LOCATION "${liburing_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${liburing_INCLUDE_DIR}"
    )
  endif()

  # Check kernel version for io_uring feature support
  execute_process(COMMAND uname -r OUTPUT_VARIABLE KERNEL_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Extract major and minor version
  string(REGEX MATCH "^([0-9]+)\\.([0-9]+)" KERNEL_VERSION_MATCH "${KERNEL_VERSION}")
  set(KERNEL_MAJOR ${CMAKE_MATCH_1})
  set(KERNEL_MINOR ${CMAKE_MATCH_2})

  # Check for kernel 5.6+ (basic io_uring features) or 6.1+ (recommended)
  if(KERNEL_MAJOR GREATER_EQUAL 6)
    set(liburing_KERNEL_SUPPORT TRUE)
    message(STATUS "liburing: Kernel ${KERNEL_VERSION} has full io_uring support (6.x+)")
  elseif(KERNEL_MAJOR EQUAL 5 AND KERNEL_MINOR GREATER_EQUAL 6)
    set(liburing_KERNEL_SUPPORT TRUE)
    message(STATUS "liburing: Kernel ${KERNEL_VERSION} has basic io_uring support (5.6+)")
  else()
    set(liburing_KERNEL_SUPPORT FALSE)
    message(
      WARNING
      "liburing: Kernel ${KERNEL_VERSION} may have limited io_uring support. Recommend 5.6+ for basic support, 6.1+ for full feature set."
    )
  endif()

  message(STATUS "Found liburing: ${liburing_LIBRARY}")
  message(STATUS "liburing include dir: ${liburing_INCLUDE_DIR}")
endif()
