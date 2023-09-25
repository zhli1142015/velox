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

#include "BlobClientProviderFactory.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/abfs/vegas/client/VegasCacheConfig.h"

namespace facebook::velox::filesystems::abfs {

/// Implementation of the ABS (Azure Blob Storage) filesystem and file
/// interface. We provide a registration method for reading and writing files so
/// that the appropriate type of file can be constructed based on a filename.
/// The supported schema is `abfs(s)://` to align with the valid scheme
/// identifiers used in the Hadoop Filesystem ABFS driver when integrating with
/// Azure Blob Storage. One key difference here is that the ABFS Hadoop client
/// driver always uses Transport Layer Security (TLS) regardless of the
/// authentication method chosen when using the `abfss` schema, but not mandated
/// when using the `abfs` schema. In our implementation, we always use the HTTPS
/// protocol, regardless of whether the schema is `abfs://` or `abfss://`. The
/// legacy wabs(s):// schema is not supported as it has been deprecated already
/// by Azure Storage team. Reference document -
/// https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage.
class AbfsFileSystem : public FileSystem {
 public:
  static constexpr const char* kReaderAbfsLoadQuantum = "fs.azure.loadQuantum";

  static constexpr const char* kVegasEnabled = "vegas.useCache";

  static constexpr const char* kVegasCacheSize = "vegas.cacheSize";

  static constexpr const char* kReaderAbfsIoThreads = "fs.azure.ioThreads";

  static constexpr const char* kAbfsEndpoint = "fs.azure.abfs.endpoint";

  explicit AbfsFileSystem(const std::shared_ptr<const Config>& config);

  std::string name() const override;

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options = {}) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options = {}) override;

  void rename(
      std::string_view path,
      std::string_view newPath,
      bool overWrite = false) override {
    VELOX_UNSUPPORTED("rename for abfs not implemented");
  }

  void remove(std::string_view path) override {
    VELOX_UNSUPPORTED("remove for abfs not implemented");
  }

  bool exists(std::string_view path) override {
    VELOX_UNSUPPORTED("exists for abfs not implemented");
  }

  std::vector<std::string> list(std::string_view path) override {
    VELOX_UNSUPPORTED("list for abfs not implemented");
  }

  void mkdir(std::string_view path) override {
    VELOX_UNSUPPORTED("mkdir for abfs not implemented");
  }

  void rmdir(std::string_view path) override {
    VELOX_UNSUPPORTED("rmdir for abfs not implemented");
  }

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;

 private:
  bool isVegasEnabled_{false};
  std::shared_ptr<vegas::VegasCacheConfig> vegasConfig_;
};

void registerAbfsFileSystem();
} // namespace facebook::velox::filesystems::abfs
