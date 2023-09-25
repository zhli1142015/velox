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
class VegasMessageQueue {
 public:
  virtual ~VegasMessageQueue();
  explicit VegasMessageQueue();

  // These all return 0 on success, -1 on error.
  // errno will be set for details if needed.
  int OpenForSending(const std::string& name);
  int Send(const std::string& value);

  void Close();

  long Count();
  long GetMaxMessages() {
    return maxMessages_;
  }

  long GetMaxMessageSize() {
    return maxMessageSize_;
  }

 private:
  mqd_t q_ = 0;
  long maxMessages_ = 0;
  long maxMessageSize_ = 0;
  char* receiveBuffer_ = nullptr;
};
} // namespace facebook::velox::filesystems::abfs::vegas
