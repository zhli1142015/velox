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

#include "gtest/gtest.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsReadFile.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"
#include "velox/connectors/hive/storage_adapters/abfs/BlobClientProviderFactory.h"
#include "velox/connectors/hive/storage_adapters/abfs/ConnectionStringBlobClientProvider.h"
#include "velox/connectors/hive/storage_adapters/abfs/tests/AzuriteServer.h"
#include "velox/exec/tests/utils/PortUtil.h"
#include "velox/exec/tests/utils/TempFilePath.h"

#include <mqueue.h>
#include <atomic>
#include <filesystem>
#include <random>

using namespace facebook::velox;
using namespace Azure::Storage::Blobs;

constexpr int kOneMB = 1 << 20;
static const std::string filePath = "test_file.txt";
static const std::string fullFilePath =
    facebook::velox::filesystems::test::AzuriteABFSEndpoint + filePath;
static const std::string vegasCacheDir = "/tmp/vegas/vfs";
static const std::string queueName = "/msft.synapse.vegas.vfsq";
static mqd_t q;

class VegasFileSystemTest : public testing::Test {
 public:
  static std::shared_ptr<const Config> hiveConfig(
      const std::unordered_map<std::string, std::string> configOverride = {}) {
    std::unordered_map<std::string, std::string> config(
        {{connector::hive::HiveConfig::kLoadQuantum, "1024"},
         {filesystems::abfs::AbfsFileSystem::kReaderAbfsIoThreads, "128"},
         {filesystems::abfs::AbfsFileSystem::kVegasEnabled, "true"},
         {filesystems::abfs::AbfsFileSystem::kVegasCacheSize, "50"},
         {filesystems::abfs::vegas::VegasCacheConfig::kVfsCacheDir,
          vegasCacheDir},
         {facebook::velox::filesystems::test::AzuriteSparkConfig,
          azuriteServer_->connectionStr()}});

    // Update the default config map with the supplied configOverride map
    for (const auto& item : configOverride) {
      config[item.first] = item.second;
    }

    return std::make_shared<const core::MemConfig>(std::move(config));
  }

 public:
  static void SetUpTestSuite() {
    port_ = std::to_string(facebook::velox::exec::test::getFreePort());
    azuriteServer_ =
        std::make_shared<facebook::velox::filesystems::test::AzuriteServer>(
            static_cast<int64_t>(stoi(port_)));
    azuriteServer_->start();
    auto tempFile = createFile();
    azuriteServer_->addFile(tempFile->path, filePath);
    auto connectionStringProvider =
        std::make_shared<ConnectionStringBlobClientProvider>(
            facebook::velox::filesystems::test::AzuriteSparkConfig,
            azuriteServer_->connectionStr());
    filesystems::abfs::BlobClientProviderFactory::registerProvider(
        std::static_pointer_cast<filesystems::abfs::BlobClientProvider>(
            connectionStringProvider));
    filesystems::abfs::registerAbfsFileSystem();

    if (!std::filesystem::is_directory(vegasCacheDir)) {
      // Create the directory if it does not exist
      if (!std::filesystem::create_directories(vegasCacheDir)) {
        LOG(ERROR) << "Error creating Vegas cache directory " << vegasCacheDir;
      }
    }
    for (const auto& entry :
         std::filesystem::directory_iterator(vegasCacheDir)) {
      if (std::filesystem::is_regular_file(entry)) {
        std::filesystem::remove(entry);
      }
    }

    // Create message queue for Vegas client
    struct mq_attr attr;
    attr.mq_flags = 0;
    attr.mq_maxmsg = 10; // Maximum number of messages in the queue
    attr.mq_msgsize = 1024; // Maximum message size
    attr.mq_curmsgs = 0; // Current number of messages in the queue

    q = mq_open(queueName.c_str(), O_CREAT | O_RDWR, 0664, &attr);
    if (q == (mqd_t)-1) {
      LOG(ERROR) << folly::errnoStr(errno);
    }
  }

  static void TearDownTestSuite() {
    if (azuriteServer_ != nullptr) {
      azuriteServer_->stop();
    }
    filesystems::abfs::BlobClientProviderFactory::deregisterProviders();

    if (std::filesystem::is_directory(vegasCacheDir)) {
      for (const auto& entry :
           std::filesystem::directory_iterator(vegasCacheDir)) {
        if (std::filesystem::is_regular_file(entry)) {
          std::filesystem::remove(entry);
        }
      }
    }

    mq_close(q);
    mq_unlink(queueName.c_str());
  }

  static std::shared_ptr<facebook::velox::filesystems::test::AzuriteServer>
      azuriteServer_;
  static std::atomic<bool> startThreads;
  static std::string port_;

  static std::shared_ptr<::exec::test::TempFilePath> createFile() {
    auto tempFile = ::exec::test::TempFilePath::create();
    tempFile->append("aaaaa");
    tempFile->append("bbbbb");
    tempFile->append(std::string(kOneMB, 'c'));
    tempFile->append("ddddd");
    return tempFile;
  }
};

std::atomic<bool> VegasFileSystemTest::startThreads = false;
std::string VegasFileSystemTest::port_ = "10010";
std::shared_ptr<facebook::velox::filesystems::test::AzuriteServer>
    VegasFileSystemTest::azuriteServer_ = nullptr;

void readData(ReadFile* readFile, uint64_t& ssdBytesRead) {
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer1[5];
  ASSERT_EQ(
      readFile->pread(10 + kOneMB, 5, &buffer1, ssdBytesRead, 0, 0), "ddddd");
  char buffer2[10];
  ASSERT_EQ(readFile->pread(0, 10, &buffer2, ssdBytesRead, 0, 0), "aaaaabbbbb");
  auto buffer3 = new char[kOneMB];
  ASSERT_EQ(
      readFile->pread(10, kOneMB, buffer3, ssdBytesRead, 0, 0),
      std::string(kOneMB, 'c'));
  delete[] buffer3;
  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer4[10];
  const std::string_view arf =
      readFile->pread(5, 10, &buffer4, ssdBytesRead, 0, 0);
  const std::string zarf = readFile->pread(kOneMB, 15, ssdBytesRead, 0, 0);
  auto buf = std::make_unique<char[]>(8);
  const std::string_view warf =
      readFile->pread(4, 8, buf.get(), ssdBytesRead, 0, 0);
  const std::string_view warfFromBuf(buf.get(), 8);
  ASSERT_EQ(arf, "bbbbbccccc");
  ASSERT_EQ(zarf, "ccccccccccddddd");
  ASSERT_EQ(warf, "abbbbbcc");
  ASSERT_EQ(warfFromBuf, "abbbbbcc");
}

TEST_F(VegasFileSystemTest, readFile) {
  auto config = VegasFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, config);
  auto readFile = abfs->openFileForRead(fullFilePath);
  uint64_t ssdBytesRead = 0;
  readData(readFile.get(), ssdBytesRead);
  // Last 3 reads in readData will already be in cache
  ASSERT_EQ(ssdBytesRead, 33);
  ssdBytesRead = 0;
  readData(readFile.get(), ssdBytesRead);
  // Total bytes read is 5 + 10 + kOneMB + 10 + 15 + 8
  ASSERT_EQ(ssdBytesRead, 1048624);
}

TEST_F(VegasFileSystemTest, readFileWithPartialOverlaps) {
  const std::string path = "test_file2.txt";
  const std::string fullPath =
      facebook::velox::filesystems::test::AzuriteABFSEndpoint + path;
  auto tempFile = VegasFileSystemTest::createFile();
  azuriteServer_->addFile(tempFile->path, path);

  auto config = VegasFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullPath, config);
  auto readFile = abfs->openFileForRead(fullPath);
  uint64_t ssdBytesRead = 0;

  ASSERT_EQ(readFile->size(), 15 + kOneMB);
  char buffer1[10];
  ASSERT_EQ(readFile->pread(0, 10, &buffer1, ssdBytesRead, 0, 0), "aaaaabbbbb");
  char buffer2[10];
  ASSERT_EQ(
      readFile->pread(10, 10, &buffer2, ssdBytesRead, 0, 0), "cccccccccc");
  char buffer3[10];
  const std::string_view sv =
      readFile->pread(5, 10, &buffer3, ssdBytesRead, 0, 0);
  const std::string s = readFile->pread(2, 10, ssdBytesRead, 0, 0);
  auto buf = std::make_unique<char[]>(10);
  const std::string_view warf =
      readFile->pread(18, 10, buf.get(), ssdBytesRead, 0, 0);
  const std::string_view warfFromBuf(buf.get(), 10);
  ASSERT_EQ(sv, "bbbbbccccc");
  ASSERT_EQ(s, "aaabbbbbcc");
  ASSERT_EQ(warf, "cccccccccc");
  ASSERT_EQ(warfFromBuf, "cccccccccc");
  ASSERT_EQ(ssdBytesRead, 22);

  ssdBytesRead = 0;
  char buffer4[10];
  ASSERT_EQ(readFile->pread(0, 10, &buffer4, ssdBytesRead, 0, 0), "aaaaabbbbb");
  char buffer5[10];
  ASSERT_EQ(
      readFile->pread(10, 10, &buffer5, ssdBytesRead, 0, 0), "cccccccccc");
  char buffer6[10];
  const std::string_view sv2 =
      readFile->pread(5, 10, &buffer6, ssdBytesRead, 0, 0);
  const std::string s2 = readFile->pread(2, 10, ssdBytesRead, 0, 0);
  auto buf2 = std::make_unique<char[]>(10);
  const std::string_view warf2 =
      readFile->pread(18, 10, buf2.get(), ssdBytesRead, 0, 0);
  const std::string_view warfFromBuf2(buf2.get(), 10);
  auto buffer7 = new char[kOneMB];
  ASSERT_EQ(
      readFile->pread(10, kOneMB, buffer7, ssdBytesRead, 0, 0),
      std::string(kOneMB, 'c'));
  delete[] buffer7;
  ASSERT_EQ(sv2, "bbbbbccccc");
  ASSERT_EQ(s2, "aaabbbbbcc");
  ASSERT_EQ(warf2, "cccccccccc");
  ASSERT_EQ(warfFromBuf2, "cccccccccc");
  ASSERT_EQ(ssdBytesRead, 68);

  ssdBytesRead = 0;
  auto buffer8 = new char[kOneMB];
  ASSERT_EQ(
      readFile->pread(10, kOneMB, buffer7, ssdBytesRead, 0, 0),
      std::string(kOneMB, 'c'));
  delete[] buffer8;
  ASSERT_EQ(ssdBytesRead, kOneMB);
}

TEST_F(VegasFileSystemTest, multipleThreadsWithReadFile) {
  startThreads = false;
  auto config = VegasFileSystemTest::hiveConfig();
  auto abfs = filesystems::getFileSystem(fullFilePath, config);

  std::vector<std::thread> threads;
  std::mt19937 generator(std::random_device{}());
  std::vector<int> sleepTimesInMicroseconds = {0, 500, 5000};
  std::uniform_int_distribution<std::size_t> distribution(
      0, sleepTimesInMicroseconds.size() - 1);
  for (int i = 0; i < 10; i++) {
    auto thread = std::thread(
        [&abfs, &distribution, &generator, &sleepTimesInMicroseconds] {
          int index = distribution(generator);
          while (!VegasFileSystemTest::startThreads) {
            std::this_thread::yield();
          }
          std::this_thread::sleep_for(
              std::chrono::microseconds(sleepTimesInMicroseconds[index]));
          auto readFile = abfs->openFileForRead(fullFilePath);
          uint64_t ssdBytesRead = 0;
          readData(readFile.get(), ssdBytesRead);
          ASSERT_EQ(ssdBytesRead, 1048624);
        });
    threads.emplace_back(std::move(thread));
  }
  startThreads = true;
  for (auto& thread : threads) {
    thread.join();
  }
}