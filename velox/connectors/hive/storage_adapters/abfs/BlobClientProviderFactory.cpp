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

#include "BlobClientProviderFactory.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

namespace facebook::velox::filesystems::abfs {
using namespace Azure::Storage::Blobs;

std::unordered_map<std::string, std::shared_ptr<BlobClientProvider>>
    BlobClientProviderFactory::providers;

void BlobClientProviderFactory::registerProvider(
    std::shared_ptr<BlobClientProvider> provider) {
  providers[provider->getAccountIdentifier()] = provider;
  LOG(INFO) << "Registered BlobClient provider for: "
            << provider->getAccountIdentifier();
}

bool BlobClientProviderFactory::providerRegistered(std::string account) {
  auto it = providers.find(account);
  return it != providers.end();
}

std::shared_ptr<BlobClient> BlobClientProviderFactory::getBlobClient(
    std::string path,
    AbfsAccount abfsAccount) {
  if (providers.empty()) {
    throw std::runtime_error("No BlobClientProvider registered for: " + path);
  }

  std::string accountIdentifier = abfsAccount.credKey();
  auto it = providers.find(accountIdentifier);

  // Providers like ConnectionStringBlobClientProvider are registered explicitly
  // with account name and others with "default". If the account name is not
  // present in the map we assume that it must have been registered with
  // "default" account.
  if (it == providers.end()) {
    accountIdentifier = kDefaultAccountIdentifier;
  }

  // Ensure that "default" is actually registered
  if (accountIdentifier == kDefaultAccountIdentifier &&
      providers.find(accountIdentifier) == providers.end()) {
    throw std::runtime_error("No BlobClientProvider registered for: " + path);
  }

  return providers[accountIdentifier]->getBlobClient(abfsAccount);
}

void BlobClientProviderFactory::deregisterProviders() {
  providers.clear();
}
} // namespace facebook::velox::filesystems::abfs
