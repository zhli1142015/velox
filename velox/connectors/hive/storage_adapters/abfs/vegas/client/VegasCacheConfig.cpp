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

#include "VegasCacheConfig.h"

namespace facebook::velox::filesystems::abfs::vegas {

VegasCacheConfig::VegasCacheConfig(const Config* config) : config_(config) {
  vfsCacheDir_ = getVfsCacheDir();
  vfsAccumulationBufferSizeBytes_ = std::max(
      getVfsAccumulateReadThresholdBytes(),
      std::max(
          getVfsAccumulationBufferSizeBytes(), getVfsFlushThresholdBytes()));
  LOG(INFO) << "VegasCacheConfig "
            << " CacheDir:" << vfsCacheDir_ << ", AccumulationBufferSizeBytes:"
            << vfsAccumulationBufferSizeBytes_;
}

std::string VegasCacheConfig::getVfsCacheDir() const {
  return config_->get<std::string>(kVfsCacheDir, "/mnt2/vegas/vfs");
}

std::string VegasCacheConfig::getVfsCachePath(const std::string& path) const {
  return vfsCacheDir_ + "/" + path;
}

int64_t VegasCacheConfig::getVfsAccumulationBufferSizeBytes() const {
  return config_->get<int64_t>(kVfsAccumulationBufferSizeBytes, 4096);
}

int64_t VegasCacheConfig::getVfsFlushThresholdBytes() const {
  return config_->get<int64_t>(kVfsFlushThresholdBytes, 4096);
}

int64_t VegasCacheConfig::getVfsAccumulateReadThresholdBytes() const {
  return config_->get<int64_t>(kVfsAccumulateReadThresholdBytes, 4096);
}

int64_t VegasCacheConfig::getVfsCatalogPublishTimeoutMs() const {
  return config_->get<int64_t>(kVfsCatalogPublishTimeoutMs, 5000);
}

bool VegasCacheConfig::getVfsPublishCatalogEntry() const {
  return config_->get<bool>(kVfsPublishCatalogEntry, true);
}

bool VegasCacheConfig::getVfsLimitTotalWriteLocks() const {
  return config_->get<bool>(kVfsLimitTotalWriteLocks, true);
}

uint64_t VegasCacheConfig::getVfsWriteLocksLimit() const {
  return config_->get<uint64_t>(kVfsWriteLocksLimit, 20000);
}

} // namespace facebook::velox::filesystems::abfs::vegas
