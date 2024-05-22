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

#include "VegasExecV1.h"
#include <boost/algorithm/hex.hpp>
#include <openssl/md5.h>

namespace facebook::velox::filesystems::abfs::vegas {

VegasExecV1::VegasExecV1(
    const std::string& path,
    const std::shared_ptr<vegas::VegasCacheConfig> vegasConfig)
    : VegasExecBase(path, vegasConfig) {
  unsigned char md5output[MD5_DIGEST_LENGTH];
  MD5(reinterpret_cast<const unsigned char*>(path.c_str()),
      path.length(),
      md5output);
  auto md5str =
      std::string(reinterpret_cast<const char*>(md5output), MD5_DIGEST_LENGTH);
  std::string md5hex = boost::algorithm::hex_lower(md5str);
  const std::string localPath = vegasConfig->getVfsCachePath(md5hex);
  blockJournal_ =
      std::make_unique<VegasJournalV1>(vegasConfig, path, localPath, md5hex);
}

bool VegasExecV1::initialize(
    const std::string remoteEtag,
    const uint64_t size,
    const uint64_t splitOffset,
    const uint64_t splitLength) {
  bool cacheInitialized =
      blockJournal_->initialize(remoteEtag, size, splitOffset, splitLength);
  VLOG(1) << "Cache initialized: " << cacheInitialized
          << " remoteEtag: " << remoteEtag << " path: " << blockJournal_->uri()
          << " offset: " << splitOffset << " length: " << splitLength;
  return cacheInitialized;
}

void VegasExecV1::tryClose() {
  blockJournal_->tryClose();
}

void VegasExecV1::writeCacheAsync(
    std::vector<uint64_t>& logicalOffset,
    std::vector<uint64_t>& lenVec,
    std::vector<char*>& bufVec) {
  bool locked = blockJournal_->getWriteLock();
  if (locked) {
    VLOG(1) << "Got write lock for: " << blockJournal_->uri();
    uint64_t fileOffset = blockJournal_->cacheWrite(lenVec, bufVec);
    uint64_t fileOffset_t = fileOffset;
    for (int i = 0; i < lenVec.size(); ++i) {
      blockJournal_->appendBlock(logicalOffset[i], lenVec[i], fileOffset_t);
      fileOffset_t += lenVec[i];
    }

    blockJournal_->saveMap();
  }
}

void VegasExecV1::cleanup(const std::string& uri) {
  VegasJournalV1::cleanup(uri);
}
} // namespace facebook::velox::filesystems::abfs::vegas