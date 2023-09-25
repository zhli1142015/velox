/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <unistd.h>
#include <chrono>
#include <climits>
#include <cstring>
#include <string>
namespace facebook::velox::filesystems::abfs::vegas {

using namespace std::string_literals;
/**
 * Copied from Vegas with same name
 * Also copied DateUtils
 */

struct DateParts {
  uint64_t yy;
  uint64_t mo;
  uint64_t dd;
  uint64_t hh;
  uint64_t mi;
  uint64_t ss;
  uint64_t nanos;
  int64_t utcOffsetMinutes;

  DateParts() {
    std::memset(this, 0, sizeof(DateParts));
  }
};

class SysUtils {
 public:
  static std::string GetHostName() {
    char host[HOST_NAME_MAX + 1];
    gethostname(host, HOST_NAME_MAX + 1);
    return host;
  }

  // Returns now as an ISO 8601 string rounded to milliseconds (3 digits)
  //
  static std::string now_as_string_millis() {
    auto now = now_as_millis();
    return to_ISO_8601_millis(now);
  }

  // Gets the first valid IP v4 address listed via getifaddrs()
  //
  static std::string GetIpV4Address();

 private:
  // Returns a timepoint rounded to millis
  //
  static std::chrono::system_clock::time_point now_as_millis() {
    auto nowt = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(nowt);
    return now_ms;
  }

  static std::string to_ISO_8601(const DateParts& dp, int fractionalDigits);

  static std::string to_ISO_8601(
      const std::chrono::system_clock::time_point& tp,
      int fractionalDigits);

  // Three fractional digits
  //
  static std::string to_ISO_8601_millis(
      const std::chrono::system_clock::time_point& tp);

  static std::string StringizeNanos(uint64_t nanos, uint32_t digits);
  static void nanos_to_date_parts(uint64_t nanos, DateParts& dest);
  static std::tuple<uint32_t, uint32_t, uint32_t> from_days(uint32_t z);

  static const uint64_t NanosecondsPerDay = 86400ULL * 1000000000ULL;
  static const uint64_t NanosecondsPerHour = 3600ULL * 1000000000ULL;
  static const uint64_t NanosecondsPerMinute = 60ULL * 1000000000ULL;
  static const uint64_t NanosecondsPerSecond = 1000000000ULL;
};

} // namespace facebook::velox::filesystems::abfs::vegas
