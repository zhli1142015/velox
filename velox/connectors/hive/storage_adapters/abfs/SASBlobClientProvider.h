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
#include "velox/connectors/hive/storage_adapters/abfs/BlobClientProvider.h"
#include "velox/connectors/hive/storage_adapters/abfs/BlobClientProviderFactory.h"

#include <azure/storage/blobs.hpp>

using namespace facebook::velox::filesystems::abfs;
using namespace Azure::Storage::Blobs;

namespace facebook::velox::filesystems::abfs {

/**
 * Implementation of BlobClientProvider for SAS connections
 * authentication.
 */
class SASBlobClientProvider : public BlobClientProvider {
 public:
  SASBlobClientProvider(std::string accountIdentifier, std::string sas);

  std::shared_ptr<BlobClient> getBlobClient(AbfsAccount abfsAccount) override;

 private:
  std::string sas_;
};
} // namespace facebook::velox::filesystems::abfs