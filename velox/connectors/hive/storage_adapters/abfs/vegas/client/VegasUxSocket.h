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

#include <mqueue.h>
#include <string>
namespace facebook::velox::filesystems::abfs::vegas {
class VegasUxSocket {
 public:
  explicit VegasUxSocket();
  ~VegasUxSocket() {
    Close();
  }

  bool Connect(const std::string& name);

  void Close();

  bool IsConnected() const {
    return fd_ == -1 ? false : true;
  }

  bool AwaitWritable(int msec = WritePollTimeoutMsec);

  long Available() const;

  bool AwaitReadable(int msec);

  bool AwaitReadable() {
    return AwaitReadable(ReadPollTimeoutMsec);
  }

  bool Write(const void* src, long bytesToWrite);

  long Position() {
    return readPhysicalPosition_;
  }

  long Read(const void* buf, long sz);

 private:
  const static int WritePollTimeoutMsec = 5000;
  const static int ReadPollTimeoutMsec = 5000;

  int fd_ = 0;
  long totalWritten_ = 0;

  long readPhysicalPosition_ = 0;

  std::string name_;
};
} // namespace facebook::velox::filesystems::abfs::vegas
