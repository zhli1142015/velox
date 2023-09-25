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

#include "VegasMessageQueue.h"

namespace facebook::velox::filesystems::abfs::vegas {

VegasMessageQueue::VegasMessageQueue() = default;

int VegasMessageQueue::OpenForSending(const std::string& name) {
  mq_attr attr;

  q_ = mq_open(name.c_str(), O_WRONLY | O_NONBLOCK);
  if (q_ == -1) {
    return errno;
  }

  if (mq_getattr(q_, &attr) == -1) {
    return errno;
  }

  maxMessages_ = attr.mq_maxmsg;
  maxMessageSize_ = attr.mq_msgsize;

  return 0;
}

long VegasMessageQueue::Count() {
  mq_attr at;

  int res = mq_getattr(q_, &at);
  if (res == -1) {
    return -1;
  }

  return at.mq_curmsgs;
}

VegasMessageQueue::~VegasMessageQueue() {
  delete receiveBuffer_;
}

int VegasMessageQueue::Send(const std::string& msg) {
  int res =
      mq_send(q_, msg.data(), msg.size(), 1); // Pri 1 for all messages for now

  if (res == -1) {
    return -1;
  }

  return 0;
}

void VegasMessageQueue::Close() {
  mq_close(q_);
  q_ = 0;
}
} // namespace facebook::velox::filesystems::abfs::vegas
