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

#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

namespace facebook::velox::filesystems::abfs {
AbfsAccount::AbfsAccount(
    const std::string path,
    const std::string abfsEndpoint) {
  auto file = std::string("");
  auto containerBegin = 0;
  if (path.find(kAbfssScheme) == 0) {
    file = std::string(path.substr(8));
    scheme_ = kAbfssScheme.substr(0, 5);
    containerBegin = 8;
  } else {
    file = std::string(path.substr(7));
    scheme_ = kAbfsScheme.substr(0, 4);
    containerBegin = 7;
  }

  auto firstAt = file.find_first_of("@");
  fileSystem_ = std::string(file.substr(0, firstAt));
  auto firstSep = file.find_first_of("/");
  filePath_ = std::string(file.substr(firstSep + 1));

  accountNameWithSuffix_ = file.substr(firstAt + 1, firstSep - firstAt - 1);
  auto firstDot = accountNameWithSuffix_.find_first_of(".");
  accountName_ = accountNameWithSuffix_.substr(0, firstDot);

  size_t position = accountNameWithSuffix_.find_last_of(
      '.', accountNameWithSuffix_.size() - 1);
  position = accountNameWithSuffix_.find_last_of(
      '.', position - 1); // Find second last dot
  position = accountNameWithSuffix_.find_last_of(
      '.', position - 1); // Find third last dot

  if (!abfsEndpoint.empty()) {
    LOG(INFO) << "Using configured endpoint " << abfsEndpoint;
    endpointSuffix_ = abfsEndpoint;
    accountNameWithSuffix_ =
        accountNameWithSuffix_.substr(0, position + 1) + endpointSuffix_;
  } else {
    endpointSuffix_ = accountNameWithSuffix_.substr(position + 1);
    if (kValidEndpoints.count(endpointSuffix_) == 0) {
      if (!abfsEndpoint.empty()) {
        LOG(INFO) << "Using configured endpoint " << abfsEndpoint;
        endpointSuffix_ = abfsEndpoint;
        accountNameWithSuffix_ =
            accountNameWithSuffix_.substr(0, position + 1) + endpointSuffix_;
      } else {
        VELOX_FAIL(
            "Endpoint {} is not valid, please pass a default endpoint using spark.fs.azure.abfs.endpoint",
            endpointSuffix_);
      }
    }
  }
  credKey_ = accountName_;
}

const std::string AbfsAccount::accountNameWithSuffix() const {
  return accountNameWithSuffix_;
}

const std::string AbfsAccount::scheme() const {
  return scheme_;
}

const std::string AbfsAccount::accountName() const {
  return accountName_;
}

const std::string AbfsAccount::endpointSuffix() const {
  return endpointSuffix_;
}

const std::string AbfsAccount::fileSystem() const {
  return fileSystem_;
}

const std::string AbfsAccount::filePath() const {
  return filePath_;
}

const std::string AbfsAccount::credKey() const {
  return credKey_;
}

const std::string AbfsAccount::connectionString(
    const std::string accountKey) const {
  return fmt::format(
      "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix={}",
      accountName(),
      accountKey,
      endpointSuffix());
}

const std::string AbfsAccount::blobURL(bool useHttps) const {
  auto protocol = "https";
  if (!useHttps) {
    protocol = "http";
  }

  return fmt::format(
      "{}://{}.blob.{}/{}/{}",
      protocol,
      accountName(),
      endpointSuffix(),
      fileSystem(),
      filePath());
}
} // namespace facebook::velox::filesystems::abfs
