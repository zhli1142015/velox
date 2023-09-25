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

#include "VegasJournalBase.h"
#include <common/file/FileSystems.h>
#include <connectors/hive/storage_adapters/abfs/vegas/client/SysUtils.h>
#include <fstream>
#include <memory>

namespace facebook::velox::filesystems::abfs::vegas {

VegasJournalBase::VegasJournalBase(
    const std::shared_ptr<vegas::VegasCacheConfig> vegasConfig,
    const std::string& uri,
    const std::string& path)
    : uri_(uri),
      path_(path),
      vegasConfig_(vegasConfig),
      vegasClient_(std::make_unique<vegas::VegasCacheClient>(vegasConfig)) {}

bool VegasJournalBase::readString(
    const uint8_t* buffer,
    uint32_t& offset,
    uint64_t szMax,
    std::string& result) {
  offset += 1;
  if (offset >= szMax) {
    return false;
  }
  int strSize = readTwoBytes(buffer, offset - 1);

  offset += strSize;
  if (offset >= szMax) {
    return false;
  }
  result.assign(buffer + offset - strSize + 1, buffer + offset + 1);

  return true;
}

uint8_t VegasJournalBase::readOneByte(const uint8_t* buffer, uint32_t offset) {
  return buffer[offset];
}

uint16_t VegasJournalBase::readTwoBytes(
    const uint8_t* buffer,
    uint32_t offset) {
  return buffer[offset + 1] | buffer[offset] << 8;
}

uint32_t VegasJournalBase::readFourBytes(
    const uint8_t* buffer,
    uint32_t offset) {
  return (uint32_t)buffer[offset + 3] | (uint32_t)buffer[offset + 2] << 8 |
      (uint32_t)buffer[offset + 1] << 16 | (uint32_t)buffer[offset] << 24;
}

uint64_t VegasJournalBase::readEightBytes(
    const uint8_t* buffer,
    uint32_t offset) {
  return (uint64_t)buffer[offset + 7] | (uint64_t)buffer[offset + 6] << 8 |
      (uint64_t)buffer[offset + 5] << 16 | (uint64_t)buffer[offset + 4] << 24 |
      (uint64_t)buffer[offset + 3] << 32 | (uint64_t)buffer[offset + 2] << 40 |
      (uint64_t)buffer[offset + 1] << 48 | (uint64_t)buffer[offset] << 56;
}

void VegasJournalBase::saveOneByteInMap(std::ofstream& mapFile, uint8_t n) {
  mapFile.put(n);
}

void VegasJournalBase::saveTwoBytesInMap(std::ofstream& mapFile, uint16_t n) {
  uint8_t bytes[2];
  bytes[0] = (n >> 8) & 0xFF;
  bytes[1] = (n >> 0) & 0xFF;
  mapFile.put(bytes[0]);
  mapFile.put(bytes[1]);
}

void VegasJournalBase::saveFourBytesInMap(std::ofstream& mapFile, uint32_t n) {
  uint8_t bytes[4];
  bytes[0] = (n >> 24) & 0xFF;
  bytes[1] = (n >> 16) & 0xFF;
  bytes[2] = (n >> 8) & 0xFF;
  bytes[3] = (n >> 0) & 0xFF;
  mapFile.put(bytes[0]);
  mapFile.put(bytes[1]);
  mapFile.put(bytes[2]);
  mapFile.put(bytes[3]);
}

void VegasJournalBase::saveEightBytesInMap(std::ofstream& mapFile, uint64_t n) {
  uint8_t bytes[8];
  bytes[0] = (n >> 56) & 0xFF;
  bytes[1] = (n >> 48) & 0xFF;
  bytes[2] = (n >> 40) & 0xFF;
  bytes[3] = (n >> 32) & 0xFF;
  bytes[4] = (n >> 24) & 0xFF;
  bytes[5] = (n >> 16) & 0xFF;
  bytes[6] = (n >> 8) & 0xFF;
  bytes[7] = (n >> 0) & 0xFF;
  mapFile.put(bytes[0]);
  mapFile.put(bytes[1]);
  mapFile.put(bytes[2]);
  mapFile.put(bytes[3]);
  mapFile.put(bytes[4]);
  mapFile.put(bytes[5]);
  mapFile.put(bytes[6]);
  mapFile.put(bytes[7]);
}
} // namespace facebook::velox::filesystems::abfs::vegas
