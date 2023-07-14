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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

#include <azure/storage/blobs.hpp>
#include <memory>

namespace facebook::velox::filesystems::abfs {
using namespace Azure::Storage::Blobs;

/**
 * BlobClientProvider is an interface that returns a blob client for a given
 * storage account. The hosting application can implement this interface to
 * customize the provider (e.g. different authorization options).
 */
class BlobClientProvider {
 public:
  virtual std::shared_ptr<BlobClient> getBlobClient(
      AbfsAccount abfsAccount) = 0;

  virtual ~BlobClientProvider() {}

  std::string getAccountIdentifier() {
    return accountIdentifier_;
  }

 protected:
  std::string accountIdentifier_ = kDefaultAccountIdentifier;
};
} // namespace facebook::velox::filesystems::abfs
