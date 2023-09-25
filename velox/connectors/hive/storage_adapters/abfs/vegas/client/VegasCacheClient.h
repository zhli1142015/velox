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

#include "velox/connectors/hive/storage_adapters/abfs/vegas/client/VegasCacheConfig.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/client/VegasMessageQueue.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/client/VegasUxSocket.h"
#include "velox/core/Config.h"

namespace facebook::velox::filesystems::abfs::vegas {
class VegasCacheClient {
 public:
  virtual ~VegasCacheClient() = default;

  explicit VegasCacheClient(const std::shared_ptr<VegasCacheConfig> config);

  bool initialize();
  void close();
  int sendMessageQueue(const std::string& value);
  int32_t publishCatalogUsingSocket(const std::string& value);
  const std::string GetHostName() const;
  const std::string GetHostAddress() const;
  bool isInitialized() const {
    return isInitialized_;
  }

 private:
  std::string HOST_NAME;
  std::string HOST_ADDRESS;
  bool writeIntUsingSocket(VegasUxSocket& uxSocket, const int32_t value);
  bool writeInt64UsingSocket(VegasUxSocket& uxSocket, const int64_t value);
  const std::shared_ptr<VegasCacheConfig> vegasConfig_;
  const std::string messageQueueName_ = "/msft.synapse.vegas.vfsq";
  VegasMessageQueue messageQueue_;
  bool isInitialized_ = false;
};
} // namespace facebook::velox::filesystems::abfs::vegas
