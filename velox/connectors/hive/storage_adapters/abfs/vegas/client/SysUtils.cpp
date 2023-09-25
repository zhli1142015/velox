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

#include "SysUtils.h"
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <cstring>
#include <tuple>

namespace facebook::velox::filesystems::abfs::vegas {

// Gets the IPv4 for the interface 'eth0'
//
std::string SysUtils::GetIpV4Address() {
  std::string address = "127.0.0.1";
  struct ifaddrs *ifap, *ifa;
  struct sockaddr_in* sa;
  char* addr;

  getifaddrs(&ifap);
  for (ifa = ifap; ifa; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr && ifa->ifa_addr->sa_family == AF_INET) {
      sa = (struct sockaddr_in*)ifa->ifa_addr;
      addr = inet_ntoa(sa->sin_addr);

      if (std::strcmp("eth0", ifa->ifa_name) == 0) {
        address = addr;
        break;
      }
    }
  }

  freeifaddrs(ifap);
  return address;
}

std::string SysUtils::to_ISO_8601_millis(
    const std::chrono::system_clock::time_point& tp) {
  return to_ISO_8601(tp, 3);
}

std::string SysUtils::to_ISO_8601(const DateParts& dp, int fractionalDigits) {
  auto pad = [](int f) {
    std::string t = std::to_string(f);
    if (t.size() < 2)
      t = '0' + t;
    return t;
  };

  std::string tmp;
  tmp += std::to_string(dp.yy);
  tmp += '-';
  tmp += pad(dp.mo);
  tmp += '-';
  tmp += pad(dp.dd);

  tmp += 'T';
  tmp += pad(dp.hh);
  tmp += ':';
  tmp += pad(dp.mi);
  tmp += ':';
  tmp += pad(dp.ss);

  if (fractionalDigits > 0) {
    std::string nanostr = StringizeNanos(dp.nanos, fractionalDigits);
    tmp += nanostr;
  }

  tmp += 'Z';
  return tmp;
}

// Converts a nanosecond value to the specified number of fractional digits
// rounding as needed.
// Used to convert nanoseconds to millisecond/microsecond suffixes.
//
std::string SysUtils::StringizeNanos(uint64_t nanos, uint32_t digits) {
  uint32_t digCopy = digits + 1;
  int64_t result = nanos;

  int64_t divisor = 1000000000ULL;
  while (digCopy--)
    divisor /= 10;
  if (divisor) {
    result = nanos / divisor;
    int64_t rem = result % 10;
    if (rem >= 5)
      result += 5;
    result /= 10;
  }

  std::string res = std::to_string(result);
  digCopy = digits;

  while (res.size() < digCopy)
    res = "0"s + res;
  if (res.size() > digCopy)
    res.resize(digCopy);
  res = "."s + res;
  return res;
}

std::string SysUtils::to_ISO_8601(
    const std::chrono::system_clock::time_point& tp,
    int fractionalDigits) {
  uint64_t nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       tp.time_since_epoch())
                       .count();

  DateParts dp;
  nanos_to_date_parts(nanos, dp);
  return to_ISO_8601(dp, fractionalDigits);
}

// Returns days and leftover nanos
//
void SysUtils::nanos_to_date_parts(uint64_t nanos, DateParts& dest) {
  uint64_t days = nanos / NanosecondsPerDay;
  auto res = from_days(uint32_t(days));
  uint64_t time = nanos % NanosecondsPerDay;
  uint64_t hours = time / NanosecondsPerHour;
  uint64_t minuteBase = time % NanosecondsPerHour;
  uint64_t minutes = minuteBase / NanosecondsPerMinute;
  uint64_t secondsBase = minuteBase % NanosecondsPerMinute;
  uint64_t seconds = secondsBase / NanosecondsPerSecond;
  uint64_t nanosLeft = secondsBase % NanosecondsPerSecond;

  dest.yy = std::get<0>(res);
  dest.mo = std::get<1>(res);
  dest.dd = std::get<2>(res);

  dest.hh = uint32_t(hours);
  dest.mi = uint32_t(minutes);
  dest.ss = uint32_t(seconds);

  dest.nanos = nanosLeft;
}

std::tuple<uint32_t, uint32_t, uint32_t> SysUtils::from_days(uint32_t z) {
  z += 719468;
  const uint32_t era = (z - 146096) / 146097;
  const uint32_t doe = static_cast<uint32_t>(z - era * 146097); // [0, 146096]
  const uint32_t yoe =
      (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
  const uint32_t y = static_cast<uint32_t>(yoe) + era * 400;
  const uint32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
  const uint32_t mp = (5 * doy + 2) / 153; // [0, 11]
  const uint32_t d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
  const uint32_t m = mp + (mp < 10 ? 3 : -9); // [1, 12]
  return std::tuple<uint32_t, uint32_t, uint32_t>(y + (m <= 2), m, d);
}

} // namespace facebook::velox::filesystems::abfs::vegas