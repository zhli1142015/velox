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

#include "VegasCacheClient.h"
#include <folly/portability/SysUio.h>
#include <chrono>
#include <thread>
#include "SysUtils.h"
#include "velox/common/time/Timer.h"

namespace facebook::velox::filesystems::abfs::vegas {

VegasCacheClient::VegasCacheClient(
    const std::shared_ptr<VegasCacheConfig> config)
    : vegasConfig_(config) {
  HOST_NAME = SysUtils::GetHostName();
  HOST_ADDRESS = SysUtils::GetIpV4Address();
}

bool VegasCacheClient::initialize() {
  int ret = messageQueue_.OpenForSending(messageQueueName_);
  if (ret == 0) {
    sendMessageQueue("__id=vfs.start");
  } else {
    LOG(INFO) << "Vegas message Queue Open: " << folly::errnoStr(ret);
    return false;
  }
  isInitialized_ = true;
  return true;
}

int VegasCacheClient::sendMessageQueue(const std::string& value) {
  int ret = messageQueue_.Send(value);
  return ret;
}

void VegasCacheClient::close() {
  messageQueue_.Close();
  isInitialized_ = false;
}

bool VegasCacheClient::writeIntUsingSocket(
    VegasUxSocket& uxSocket,
    const int32_t value) {
  unsigned char bytes[4];
  bytes[0] = (value >> 24) & 0xFF;
  bytes[1] = (value >> 16) & 0xFF;
  bytes[2] = (value >> 8) & 0xFF;
  bytes[3] = value & 0xFF;
  return uxSocket.Write(bytes, 4);
}

bool VegasCacheClient::writeInt64UsingSocket(
    VegasUxSocket& uxSocket,
    const int64_t value) {
  unsigned char bytes[8];
  bytes[0] = (value >> 56) & 0xFF;
  bytes[1] = (value >> 48) & 0xFF;
  bytes[2] = (value >> 40) & 0xFF;
  bytes[3] = (value >> 32) & 0xFF;
  bytes[4] = (value >> 24) & 0xFF;
  bytes[5] = (value >> 16) & 0xFF;
  bytes[6] = (value >> 8) & 0xFF;
  bytes[7] = value & 0xFF;
  return uxSocket.Write(bytes, 8);
}

int32_t VegasCacheClient::publishCatalogUsingSocket(const std::string& value) {
  VegasUxSocket uxSocket;
  if (uxSocket.Connect("/var/vegas/cache.svc.socket")) {
    if (!writeIntUsingSocket(
            uxSocket, VegasCacheConfig::UX_SOCKET_VERSION_102)) {
      return -1;
    }
    if (!writeIntUsingSocket(
            uxSocket,
            VegasCacheConfig::UX_SOCKET_OPCODE_PUBLISH_CATALOG_ITEM)) {
      return -1;
    }
    if (!writeIntUsingSocket(uxSocket, value.length())) {
      return -1;
    }
    if (!writeInt64UsingSocket(uxSocket, value.length())) {
      // as part of string write
      return -1;
    }
    if (!uxSocket.Write(value.c_str(), value.length())) {
      return -1;
    }
    auto intSize = 4;
    unsigned char buf[4] = {0, 0, 0, 0};
    auto begin = getCurrentTimeMs();
    while (uxSocket.AwaitReadable() && uxSocket.Available() < sizeof(intSize) &&
           getCurrentTimeMs() - begin <
               vegasConfig_->getVfsCatalogPublishTimeoutMs()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (uxSocket.AwaitReadable() && uxSocket.Available() == sizeof(intSize)) {
      uxSocket.Read(buf, intSize);
      int val4 = (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
      if (val4 != VegasCacheConfig::UX_SOCKET_RESPONSE_SUCCESS) {
        LOG(INFO) << "Ux Socket Open: fail with " << val4;
        return val4;
      }
    } else {
      LOG(INFO) << "Ux Socket Open: Did not get Response ";
      return -1;
    }
    uxSocket.Close();
    return 0;
  }

  return -1;
}

const std::string VegasCacheClient::GetHostName() const {
  return HOST_NAME;
}
const std::string VegasCacheClient::GetHostAddress() const {
  return HOST_ADDRESS;
}
} // namespace facebook::velox::filesystems::abfs::vegas
