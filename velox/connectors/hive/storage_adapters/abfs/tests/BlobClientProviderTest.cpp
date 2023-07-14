#include "connectors/hive/storage_adapters/abfs/BlobClientProviderFactory.h"
#include "connectors/hive/storage_adapters/abfs/tests/AzuriteServer.h"
#include "gtest/gtest.h"
#include "velox/common/file/FileSystems.h"

#include <atomic>
#include <random>

using namespace facebook::velox;
using namespace facebook::velox::filesystems::abfs;

static const std::string filePath = "test_file.txt";
static const std::string fullFilePath =
    facebook::velox::filesystems::test::AzuriteABFSEndpoint + filePath;

TEST(BlobClientProviderTest, noBlobProviderRegistered) {
  AbfsAccount abfsAccount(fullFilePath);
  ASSERT_THROW(
      BlobClientProviderFactory::getBlobClient(fullFilePath, abfsAccount),
      std::runtime_error);
  try {
    BlobClientProviderFactory::getBlobClient(fullFilePath, abfsAccount);
    FAIL() << "Expected Velox Exception";
  } catch (const std::runtime_error& err) {
    EXPECT_TRUE(
        std::string(err.what()).find("No BlobClientProvider registered") !=
        std::string::npos);
  }
}