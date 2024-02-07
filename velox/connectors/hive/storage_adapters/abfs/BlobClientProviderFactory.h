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

#include "BlobClientProvider.h"

#include <glog/logging.h>

#include <memory>
#include <queue>
#include <unordered_map>

namespace facebook::velox::filesystems::abfs {
using namespace Azure::Storage::Blobs;

/**
 * BlobClientProviderFactory exposes functionality to register
 * BlobClientProvider and get the registered client to perform reads, writes,
 * etc.
 */
class BlobClientProviderFactory {
 private:
  static std::unordered_map<std::string, std::shared_ptr<BlobClientProvider>>
      providers;

  BlobClientProviderFactory() = delete;

 public:
  /**
   * Non thread safe function that is called during Velox initialization to
   * register the BlobClientProvider by adding it to a queue of providers.
   *
   * @param provider concrete BlobClientProvider
   */
  static void registerProvider(std::shared_ptr<BlobClientProvider> provider);

  static bool providerRegistered(std::string account);

  static std::shared_ptr<BlobClient> getBlobClient(
      std::string path,
      AbfsAccount abfsAccount);

  static void deregisterProviders();
};
} // namespace facebook::velox::filesystems::abfs
