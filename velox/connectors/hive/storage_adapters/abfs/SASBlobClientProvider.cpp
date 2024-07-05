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

#include "SASBlobClientProvider.h"

namespace facebook::velox::filesystems::abfs {
using namespace Azure::Storage::Blobs;

std::shared_ptr<BlobClient> SASBlobClientProvider::getBlobClient(
    AbfsAccount abfsAccount) {
  auto sasConnection = fmt::format(
      "https://{}.blob.{}/{}/{}?{}",
      abfsAccount.accountName(),
      abfsAccount.endpointSuffix(),
      abfsAccount.fileSystem(),
      abfsAccount.filePath(),
      sas_);
  return std::make_shared<BlobClient>(BlobClient(sasConnection));
}

SASBlobClientProvider::SASBlobClientProvider(
    std::string accountIdentifier,
    std::string sas)
    : sas_(sas) {
  accountIdentifier_ = accountIdentifier;
}
} // namespace facebook::velox::filesystems::abfs