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

#include "VegasJournalV1.h"
#include <connectors/hive/storage_adapters/abfs/vegas/client/SysUtils.h>

namespace facebook::velox::filesystems::abfs::vegas {

folly::ConcurrentHashMap<std::string, size_t> VegasJournalV1::kWriteLocks;
std::atomic_uint64_t VegasJournalV1::kWriteLocksCount(0);
bool VegasJournalV1::kShouldCleanup;
uint64_t VegasJournalV1::kWriteLocksLimit;

VegasJournalV1::VegasJournalV1(
    const std::shared_ptr<vegas::VegasCacheConfig> vegasConfig,
    const std::string& uri,
    const std::string& path)
    : VegasJournalBase(vegasConfig, uri, path) {
  kShouldCleanup = vegasConfig->getVfsLimitTotalWriteLocks();
  kWriteLocksLimit = vegasConfig->getVfsWriteLocksLimit();
}

uint64_t VegasJournalV1::getTotalFileOpens() const {
  std::ifstream mapFile;
  mapFile.open(mapFileName());
  if (!mapFile.is_open()) {
    LOG(INFO) << "getTotalFileOpens failed to open mapFile" << mapFileName()
              << " " << uri_;
    return -1;
  }
  std::string mapFileAsStr(
      (std::istreambuf_iterator<char>(mapFile)),
      std::istreambuf_iterator<char>());
  mapFile.close();

  uint64_t sz = mapFileAsStr.size();
  if (sz == 0) {
    LOG(INFO) << "getTotalFileOpens failed sz = " << sz << " path " << uri_;
    return -1;
  }

  auto* buffer = reinterpret_cast<uint8_t*>(mapFileAsStr.data());
  uint32_t bufIndex = 19;

  if (bufIndex >= sz) {
    LOG(INFO) << "getTotalFileOpens failed at totalFileOpens_ " << uri_;
    return -1;
  }
  return readEightBytes(buffer, bufIndex - 7);
}

bool VegasJournalV1::loadMap() {
  blocks_.clear();

  // TODO: any file open or read issue
  std::ifstream mapFile;
  mapFile.open(mapFileName());
  if (!mapFile.is_open()) {
    return true;
  }
  std::string mapFileAsStr(
      (std::istreambuf_iterator<char>(mapFile)),
      std::istreambuf_iterator<char>());
  mapFile.close();

  uint64_t sz = mapFileAsStr.size();
  if (sz == 0) {
    LOG(INFO) << "loadMap failed sz = " << sz << " path " << uri_;
    return true;
  }

  auto* buffer = reinterpret_cast<uint8_t*>(mapFileAsStr.data());
  uint32_t bufIndex = 0;

  // mapFileSignature
  std::string signature;
  bool success = readString(buffer, bufIndex, sz, signature);
  if (!success) {
    LOG(INFO) << "loadMap failed to load  mapFileSignature "
              << " path " << uri_;
    return false;
  }
  if (signature != VegasCacheConfig::mapFileSignature) {
    LOG(INFO) << "loadMap failed to match mapFileSignature: " << signature
              << " vs " << VegasCacheConfig::mapFileSignature << " path "
              << uri_;
    return false;
  }

  // uri
  bufIndex += 1;
  std::string uri;
  success = readString(buffer, bufIndex, sz, uri);
  if (!success) {
    LOG(INFO) << "loadMap failed to load  uri " << uri_;
    return false;
  }

  // file length
  bufIndex += 8;
  if (bufIndex >= sz) {
    LOG(INFO) << "loadMap failed at fileLength " << uri_;
    return false;
  }
  uint64_t fileLength = readEightBytes(buffer, bufIndex - 7);

  // isSequentiallyComplete
  bufIndex += 1;
  if (bufIndex >= sz) {
    LOG(INFO) << "loadMap failed at isSequentiallyComplete " << uri_;
    return false;
  }
  isSequentiallyComplete_ = readOneByte(buffer, bufIndex);

  // etag
  bufIndex += 1;
  std::string eTag;
  success = readString(buffer, bufIndex, sz, eTagFromCache_);
  if (!success) {
    LOG(INFO) << "loadMap failed to load eTag " << uri_;
    return false;
  }

  // total File
  bufIndex += 8;
  if (bufIndex >= sz) {
    LOG(INFO) << "loadMap failed at totalFileOpens_ " << uri_;
    return false;
  }
  totalFileOpens_ = readEightBytes(buffer, bufIndex - 7);

  // number of blocks
  bufIndex += 4;
  if (bufIndex >= sz) {
    LOG(INFO) << "loadMap failed at numBlocks " << uri_;
    return false;
  }
  uint32_t numBlocks = readFourBytes(buffer, bufIndex - 3);

  if (bufIndex + (numBlocks * 20) >= sz) {
    LOG(INFO) << "loadMap failed at blocks " << uri_;
    return false;
  }

  // blocks in loop
  for (uint32_t i = 0; i < numBlocks; i++) {
    // logicalOffset
    bufIndex = bufIndex + 8;
    uint64_t logicalOffset = readEightBytes(buffer, bufIndex - 7);

    // length
    bufIndex = bufIndex + 8;
    uint64_t length = readEightBytes(buffer, bufIndex - 7);

    // fileOffset
    bufIndex = bufIndex + 8;
    uint64_t fileOffset = readEightBytes(buffer, bufIndex - 7);

    if (logicalOffset >= fileLength) {
      LOG(INFO) << "CORRUPTED MAP FILE: logicalOffset >= fileLength" << uri_;
      return false;
    }

    blocks_.emplace_back(logicalOffset, length, fileOffset);
  }

  // terminator
  bufIndex += 1;
  std::string terminator;
  success = readString(buffer, bufIndex, sz, terminator);
  if (!success) {
    LOG(INFO) << "loadMap failed to load terminator " << uri_;
    return false;
  }
  if (terminator != VegasCacheConfig::mapFileTerminator) {
    LOG(INFO) << "loadMap failed to match mapFileTerminator: " << terminator
              << " vs " << VegasCacheConfig::mapFileTerminator << uri_;
    return false;
  }
  fileLengthMap_ = sz;
  return true;
}

bool VegasJournalV1::verifyMap() {
  std::ofstream journalFile(journalFileName(), std::ios_base::app);
  auto size = journalFile.tellp();
  journalFile.close();
  if (blocks_.empty()) {
    return size == 0;
  }

  uint64_t targetExtentSize = 0;
  for (const auto& arr : blocks_) {
    targetExtentSize += arr.length_;
  }
  if (size != targetExtentSize) {
    LOG(INFO) << "VegasJournalV1::verifyMap  fileSize " << size
              << " targetExtentSize = " << targetExtentSize << " path " << uri_;
  }
  fileLengthJournal_ = targetExtentSize;
  return size >= targetExtentSize;
}

uint64_t VegasJournalV1::saveMap() {
  std::ofstream mapFile; // overwrite
  mapFile.open(mapFileName());
  if (!mapFile.is_open()) {
    LOG(INFO) << "saveMap failed to open mapFile " << mapFileName() << uri_;
    return 0;
  }
  uint64_t oldOffset = mapFile.tellp();

  uint16_t signatureSize = VegasCacheConfig::mapFileSignature.size();
  saveTwoBytesInMap(mapFile, signatureSize);
  mapFile.write(VegasCacheConfig::mapFileSignature.data(), signatureSize);

  uint16_t uriSize = uri_.size();
  saveTwoBytesInMap(mapFile, uriSize);
  mapFile.write(uri_.data(), uriSize);

  saveEightBytesInMap(mapFile, fileLengthRemote_);
  computeSequentialness();
  saveOneByteInMap(mapFile, isSequentiallyComplete_ ? 1 : 0);

  uint16_t eTagSize = eTagRemote_.size();
  saveTwoBytesInMap(mapFile, eTagSize);
  mapFile.write(eTagRemote_.data(), eTagSize);

  saveEightBytesInMap(mapFile, totalFileOpens_);

  saveFourBytesInMap(mapFile, blocks_.size());
  for (const auto& arr : blocks_) {
    saveEightBytesInMap(mapFile, arr.logicalOffset_);
    saveEightBytesInMap(mapFile, arr.length_);
    saveEightBytesInMap(mapFile, arr.fileOffset_);
  }

  uint16_t terminatorSize = VegasCacheConfig::mapFileTerminator.size();
  saveTwoBytesInMap(mapFile, terminatorSize);
  mapFile.write(VegasCacheConfig::mapFileTerminator.data(), terminatorSize);

  uint64_t newOffset = mapFile.tellp();
  mapFile.close();
  fileLengthMap_ = newOffset;
  return newOffset;
}

void VegasJournalV1::computeSequentialness() {
  isSequentiallyComplete_ = false;
  uint64_t currentOffset = 0;
  for (const auto& arr : blocks_) {
    if (currentOffset != arr.logicalOffset_) {
      return;
    }
    if (arr.fileOffset_ != arr.logicalOffset_) {
      return;
    }
    currentOffset += arr.length_;
  }
  if (currentOffset != fileLengthRemote_) {
    return;
  }
  isSequentiallyComplete_ = true;
}

std::pair<uint64_t, uint64_t> VegasJournalV1::locateUsableContent(
    const BlockDef& block,
    const uint64_t& startOffset,
    const uint64_t& length) {
  // Bytes from the start of the block that are not needed
  uint64_t adjustment = startOffset - block.logicalOffset_;
  uint64_t usableLength = block.length_ - adjustment;

  // If the length of data needed is smaller than the size of the block
  if (usableLength > length) {
    usableLength = length;
  }

  // Offset to read from in the journal file
  uint64_t fileOffset = block.fileOffset_ + adjustment;
  return std::make_pair(fileOffset, usableLength);
}

uint64_t VegasJournalV1::cachedBlocks(
    const uint64_t startOffset,
    const uint64_t length,
    std::vector<Chunk>& chunks) {
  if (length < 1) {
    LOG(INFO) << "Attempting to read invalid length " << length;
    return 0;
  }

  const uint64_t endOffset = startOffset + length - 1;
  if (endOffset < startOffset) {
    LOG(INFO) << "Endoffset " << endOffset << " is less than start offset "
              << startOffset;
    return 0;
  }

  if (fileLengthRemote_ != 0 && startOffset > fileLengthRemote_) {
    LOG(INFO) << "Attempting to read at " << startOffset
              << " which is greater than known file length "
              << fileLengthRemote_;
    return 0;
  }

  const size_t numBlocks = blocks_.size();
  if (numBlocks == 0) {
    cacheMissBytes_ += length;
    return 0;
  }

  uint64_t outputOffset = 0, currentOffset = startOffset,
           remainingLength = length;
  for (const auto& block : blocks_) {
    // Since blocks_ is ordered by logicalOffset_, if we get to this situation,
    // the rest of the array doesn't matter.
    if (remainingLength == 0 || currentOffset < block.logicalOffset_) {
      break;
    }

    // If the block ends before, skip it and move on.
    if (block.logicalOffset_ + block.length_ <= currentOffset) {
      continue;
    }

    // If here, the block may contain some overlap.
    auto [fileOffset, usableLength] =
        locateUsableContent(block, currentOffset, remainingLength);

    chunks.emplace_back(
        currentOffset, usableLength, fileOffset, true, outputOffset);
    outputOffset += usableLength;
    currentOffset += usableLength;
    remainingLength -= usableLength;
  }

  cacheHitBytes_ += outputOffset;
  cacheMissBytes_ += length - outputOffset;

  // Return number of bytes present in cache.
  return length - remainingLength;
}

uint64_t VegasJournalV1::appendBlock(
    uint64_t logicalOffset,
    uint64_t length,
    uint64_t fileOffset) {
  uint32_t pos = 0; // insert at the front
  for (const auto& arr : blocks_) {
    if (logicalOffset < arr.logicalOffset_) {
      break;
    }
    pos++;
  }

  blocks_.emplace(blocks_.begin() + pos, logicalOffset, length, fileOffset);
  isDirty_ = true;
  return blocks_.size();
}

void VegasJournalV1::cacheRead(uint64_t offset, uint64_t length, uint8_t* pos)
    const {
  std::ifstream journalFile(journalFileName(), std::ios_base::binary);
  journalFile.seekg(offset, std::ios_base::beg);
  journalFile.read(reinterpret_cast<char*>(pos), length);
  journalFile.close();
}

bool VegasJournalV1::getWriteLock() {
  if (kWriteLocks.insert(uri_, totalFileOpens_).second) {
    if (kShouldCleanup) {
      kWriteLocksCount.fetch_add(1);
      if (kWriteLocksCount > kWriteLocksLimit) {
        uint64_t expectedTotalFileOpens = getTotalFileOpens();
        if (expectedTotalFileOpens == -1 ||
            totalFileOpens_ != expectedTotalFileOpens) {
          kWriteLocks.erase(uri_);
          kWriteLocksCount.fetch_sub(1);
          LOG(WARNING) << "Unable to get write lock for " << uri_
                       << " due to totalFileOpens mismatch. ";
          return false;
        }
      }
    }
  }
  auto res =
      kWriteLocks.assign_if_equal(uri_, totalFileOpens_, totalFileOpens_ + 1);
  if (res.hasValue()) {
    totalFileOpens_++;
  }
  return res.hasValue();
}

uint64_t VegasJournalV1::cacheWrite(
    std::vector<uint64_t>& lenVec,
    std::vector<char*>& bufVec) const {
  std::ofstream journalFile(
      journalFileName(), std::ios_base::app | std::ios_base::binary);
  uint64_t oldOffset = journalFile.tellp();
  for (int i = 0; i < lenVec.size(); ++i) {
    journalFile.write(bufVec[i], lenVec[i]);
  }
  uint64_t newOffset = journalFile.tellp();
  journalFile.close();

  return oldOffset;
}

bool VegasJournalV1::initialize(
    const std::string& eTag,
    const uint64_t fileLength,
    const uint64_t splitOffset,
    const uint64_t splitLength) {
  eTagRemote_ = eTag;
  fileLengthRemote_ = fileLength;
  splitOffset_ = splitOffset;
  splitLength_ = splitLength;
  if (!vegasClient_->initialize()) {
    return false;
  }

  // Reference: CacheController::TryPurgeVfsEntry
  fpLock_ = fopen(lockFileName().c_str(), "w+");
  if (nullptr == fpLock_) {
    LOG(INFO) << "VegasJournalV1::initialize: "
              << "Could not open lock file " << uri_;
    std::string msg = "__id=vfs.bypass\n";
    msg += "path=" + uri_ + "\n";
    vegasClient_->sendMessageQueue(msg);
    return false;
  }
  if (0 != lockf(fileno(fpLock_), F_LOCK, 0)) {
    std::fclose(fpLock_);
    fpLock_ = nullptr;
    LOG(INFO) << "VegasJournalV1::initialize: "
              << "Could not acquire lock " << uri_;
    std::string msg = "__id=vfs.bypass\n";
    msg += "path=" + uri_ + "\n";
    vegasClient_->sendMessageQueue(msg);
    return false;
  }

  if (!loadMap()) {
    std::string msg = "__id=vfs.bypass\n";
    msg += "path=" + uri_ + "\n";
    vegasClient_->sendMessageQueue(msg);
    return false;
  }

  if (!verifyMap()) {
    std::string msg = "__id=vfs.bypass\n";
    msg += "path=" + uri_ + "\n";
    vegasClient_->sendMessageQueue(msg);
    return false;
  }

  if (!eTagFromCache_.empty() && eTagFromCache_ != eTagRemote_) {
    LOG(INFO) << "loadMap Mismatched eTag " << eTagFromCache_
              << " vs eTagRemote " << eTagRemote_ << " " << uri_;
    std::string msg = "__id=vfs.cache.miss\n";
    msg += "url=" + uri_ + "\n";
    vegasClient_->sendMessageQueue(msg);
    if (getWriteLock()) {
      // etag doesn't match, clear all content.
      std::ofstream mapFile(mapFileName(), std::ofstream::trunc);
      mapFile.close();
      std::ofstream journalFile(journalFileName(), std::ofstream::trunc);
      journalFile.close();
      LOG(INFO) << "loadMap Mismatched eTag, then clear cache " << uri_;
    }
    return false;
  }

  if (isSequentiallyComplete_) {
    std::string msg = "__id=vfs.cache.hit.localRead\n";
    msg += "url=" + uri_ + "\n";
    vegasClient_->sendMessageQueue(msg);
  } else {
    std::string msg = "__id=vfs.cache.hit.partial\n";
    msg += "url=" + uri_ + "\n";
    vegasClient_->sendMessageQueue(msg);
  }

  return true;
}

void VegasJournalV1::tryClose() {
  if (vegasClient_->isInitialized()) {
    if (isDirty_) {
      uint64_t fileLengthJournal = 0;
      for (const auto& arr : blocks_) {
        fileLengthJournal_ += arr.length_;
      }
      auto res = publishCatalog(fileLengthMap_, fileLengthJournal_);
    } else {
      auto res = publishCatalog(fileLengthMap_, fileLengthJournal_);
    }
  }

  blocks_.clear();

  if (nullptr != fpLock_) {
    if (0 != lockf(fileno(fpLock_), F_ULOCK, 0)) {
      LOG(INFO) << "VegasJournalV1::tryClose: "
                << "Could not unlock lock-file " << uri_;
    }

    if (0 != fclose(fpLock_)) {
      LOG(INFO) << "VegasJournalV1::tryClose: "
                << "Could not close lock-file " << uri_;
    }
    fpLock_ = nullptr;
  }

  if (vegasClient_->isInitialized()) {
    std::string msg = "__id=vfs.results\n";
    msg += "hit.bytes=" + std::to_string(cacheHitBytes_) + "\n";
    msg += "miss.bytes=" + std::to_string(cacheMissBytes_) + "\n";
    msg += "path=" + uri_ + "\n";
    vegasClient_->sendMessageQueue(msg);
    std::string msg1 = "__id=vfs.stream.close\n";
    msg1 += "Url=" + uri_ + "\n";
    msg1 += "ReadFromCache=" + std::to_string(cacheHitBytes_) + "\n";
    msg1 += "BytesDownloaded=" + std::to_string(cacheMissBytes_) + "\n";
    vegasClient_->sendMessageQueue(msg1);
    vegasClient_->sendMessageQueue("__id=vfs.close.fs\n");
    vegasClient_->close();
  }
}

bool VegasJournalV1::publishCatalog(
    uint64_t fileLengthMap,
    uint64_t fileLengthJournal) {
  if (!vegasConfig_->getVfsPublishCatalogEntry()) {
    return false;
  }
  std::string catalogStringBuilder;
  catalogStringBuilder.append("#VFS-generated catalog entry\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_ITEM_TYPE + "=" +
      VegasCacheConfig::CATALOG_ITEM_TYPE_VFS + "\n");

  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_ITEM_ID + "=" + uri_ +
      "?ofs=" + std::to_string(splitOffset_) +
      "&len=" + std::to_string(splitLength_) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_AZURE_URL + "=" + uri_ + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_PARTITION_OFFSET + "=" +
      std::to_string(splitOffset_) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_PARTITION_LENGTH + "=" +
      std::to_string(splitLength_) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_HOST + "=" + vegasClient_->GetHostName() +
      "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_IPV4 + "=" + vegasClient_->GetHostAddress() +
      "\n");

  uint64_t totalLength = fileLengthMap + fileLengthJournal;
  catalogStringBuilder.append(VegasCacheConfig::CATALOG_ACCESS_COUNT + "=1\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_FILE_LENGTH + "=" +
      std::to_string(fileLengthJournal) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_JOURNAL_LENGTH + "=" +
      std::to_string(fileLengthJournal) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_FULL_FILE_LENGTH + "=" +
      std::to_string(splitLength_) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_MAP_FILE_LENGTH + "=" +
      std::to_string(fileLengthMap) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_ITEM_LOCAL_PATH + "=" + path_ + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_CACHE_HIT_BYTES + "=" +
      std::to_string(cacheHitBytes_) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_CACHE_MISS_BYTES + "=" +
      std::to_string(cacheMissBytes_) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_ITEM_BLOCKS + "=" +
      std::to_string(blocks_.size()) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_IS_SEQUENTIAL + "=" +
      std::to_string(isSequentiallyComplete_) + "\n");
  catalogStringBuilder.append(
      VegasCacheConfig::CATALOG_UPDATE_TIME + "=" +
      SysUtils::now_as_string_millis() + "\n");

  int32_t ret = vegasClient_->publishCatalogUsingSocket(catalogStringBuilder);
  return ret == 0;
}

void VegasJournalV1::cleanup(const std::string& uri) {
  if (kShouldCleanup && kWriteLocksCount > kWriteLocksLimit) {
    auto res = kWriteLocks.erase(uri);
    kWriteLocksCount.fetch_sub(1);
  }
}

} // namespace facebook::velox::filesystems::abfs::vegas
