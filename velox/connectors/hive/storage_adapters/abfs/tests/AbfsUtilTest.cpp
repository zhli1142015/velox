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
#include "velox/common/base/VeloxException.h"

#include "gtest/gtest.h"

using namespace facebook::velox::filesystems::abfs;

TEST(AbfsUtilsTest, isAbfsFile) {
  EXPECT_FALSE(isAbfsFile("abfs:"));
  EXPECT_FALSE(isAbfsFile("abfss:"));
  EXPECT_FALSE(isAbfsFile("abfs:/"));
  EXPECT_FALSE(isAbfsFile("abfss:/"));
  EXPECT_TRUE(isAbfsFile("abfs://test@test.dfs.core.windows.net/test"));
  EXPECT_TRUE(isAbfsFile("abfss://test@test.dfs.core.windows.net/test"));
  EXPECT_TRUE(isAbfsFile("wasb://test@test.bolb.core.windows.net/test"));
  EXPECT_TRUE(isAbfsFile("wasbs://test@test.blob.core.windows.net/test"));
}

TEST(AbfsUtilsTest, abfsAccount) {
  auto abfsAccount = AbfsAccount("abfs://test@test.dfs.core.windows.net/test");
  EXPECT_EQ(abfsAccount.accountNameWithSuffix(), "test.dfs.core.windows.net");
  EXPECT_EQ(abfsAccount.accountName(), "test");
  EXPECT_EQ(abfsAccount.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfsAccount.fileSystem(), "test");
  EXPECT_EQ(abfsAccount.filePath(), "test");
  EXPECT_EQ(abfsAccount.credKey(), "test");
  EXPECT_EQ(
      abfsAccount.connectionString("123"),
      "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=123;EndpointSuffix=core.windows.net");

  auto abfssAccount = AbfsAccount(
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(abfssAccount.scheme(), "abfss");
  EXPECT_EQ(abfssAccount.accountNameWithSuffix(), "test.dfs.core.windows.net");
  EXPECT_EQ(abfssAccount.accountName(), "test");
  EXPECT_EQ(abfssAccount.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfssAccount.fileSystem(), "test");
  EXPECT_EQ(
      abfssAccount.filePath(),
      "sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(abfssAccount.credKey(), "test");

  // test with special characters
  auto abfssAccountWithSpecialCharacters = AbfsAccount(
      "abfss://test@test.dfs.core.windows.net/main@dir/brand#51/sub dir/test.txt");
  EXPECT_EQ(abfssAccountWithSpecialCharacters.scheme(), "abfss");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.accountNameWithSuffix(),
      "test.dfs.core.windows.net");
  EXPECT_EQ(abfssAccountWithSpecialCharacters.accountName(), "test");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfssAccountWithSpecialCharacters.fileSystem(), "test");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.filePath(),
      "main@dir/brand#51/sub dir/test.txt");
  EXPECT_EQ(abfssAccountWithSpecialCharacters.credKey(), "test");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.blobURL(true),
      "https://test.blob.core.windows.net/test/main@dir/brand%2351/sub%20dir/test.txt");
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.blobURL(false),
      "http://test.blob.core.windows.net/test/main@dir/brand%2351/sub%20dir/test.txt");
  // china cloud
  auto abfsChinaCloudAccount =
      AbfsAccount("abfs://test@test.dfs.core.chinacloudapi.cn/test");
  EXPECT_EQ(abfsChinaCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsChinaCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.chinacloudapi.cn");
  EXPECT_EQ(abfsChinaCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.endpointSuffix(), "core.chinacloudapi.cn");
  EXPECT_EQ(abfsChinaCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.filePath(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.credKey(), "test");

  // us gov cloud
  auto abfsUsGovCloudAccount =
      AbfsAccount("abfs://test@test.dfs.core.usgovcloudapi.net/test");
  EXPECT_EQ(abfsUsGovCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsUsGovCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.usgovcloudapi.net");
  EXPECT_EQ(abfsUsGovCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.endpointSuffix(), "core.usgovcloudapi.net");
  EXPECT_EQ(abfsUsGovCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.filePath(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.credKey(), "test");

  // germany cloud
  auto abfsGermanyCloudAccount =
      AbfsAccount("abfs://test@test.dfs.core.cloudapi.de/test");
  EXPECT_EQ(abfsGermanyCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsGermanyCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.cloudapi.de");
  EXPECT_EQ(abfsGermanyCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.endpointSuffix(), "core.cloudapi.de");
  EXPECT_EQ(abfsGermanyCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.filePath(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.credKey(), "test");

  // Fabric
  auto abfsDXTAccount =
      AbfsAccount("abfss://test@dxt-onelake.dfs.fabric.microsoft.com/testPath");
  EXPECT_EQ(abfsDXTAccount.scheme(), "abfss");
  EXPECT_EQ(
      abfsDXTAccount.accountNameWithSuffix(),
      "dxt-onelake.dfs.fabric.microsoft.com");
  EXPECT_EQ(abfsDXTAccount.accountName(), "dxt-onelake");
  EXPECT_EQ(abfsDXTAccount.endpointSuffix(), "fabric.microsoft.com");
  EXPECT_EQ(abfsDXTAccount.fileSystem(), "test");
  EXPECT_EQ(abfsDXTAccount.filePath(), "testPath");
  EXPECT_EQ(abfsDXTAccount.credKey(), "dxt-onelake");

  auto abfsMSITAccount = AbfsAccount(
      "abfss://test@msit-onelake.blob.pbidedicated.windows.net/testPath");
  EXPECT_EQ(abfsMSITAccount.scheme(), "abfss");
  EXPECT_EQ(
      abfsMSITAccount.accountNameWithSuffix(),
      "msit-onelake.blob.pbidedicated.windows.net");
  EXPECT_EQ(abfsMSITAccount.accountName(), "msit-onelake");
  EXPECT_EQ(abfsMSITAccount.endpointSuffix(), "pbidedicated.windows.net");
  EXPECT_EQ(abfsMSITAccount.fileSystem(), "test");
  EXPECT_EQ(abfsMSITAccount.filePath(), "testPath");
  EXPECT_EQ(abfsMSITAccount.credKey(), "msit-onelake");

  auto abfsEDogAccount = AbfsAccount(
      "abfss://velox@onelake-int-edog.dfs.pbidedicated.windows-int.net/velox.Lakehouse/Files/testPath");
  EXPECT_EQ(abfsEDogAccount.scheme(), "abfss");
  EXPECT_EQ(
      abfsEDogAccount.accountNameWithSuffix(),
      "onelake-int-edog.dfs.pbidedicated.windows-int.net");
  EXPECT_EQ(abfsEDogAccount.accountName(), "onelake-int-edog");
  EXPECT_EQ(abfsEDogAccount.endpointSuffix(), "pbidedicated.windows-int.net");
  EXPECT_EQ(abfsEDogAccount.fileSystem(), "velox");
  EXPECT_EQ(abfsEDogAccount.filePath(), "velox.Lakehouse/Files/testPath");
  EXPECT_EQ(abfsEDogAccount.credKey(), "onelake-int-edog");

  auto wasbAccount = AbfsAccount("wasb://test@test.blob.core.windows.net/test");
  EXPECT_EQ(wasbAccount.accountNameWithSuffix(), "test.blob.core.windows.net");
  EXPECT_EQ(wasbAccount.accountName(), "test");
  EXPECT_EQ(wasbAccount.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(wasbAccount.fileSystem(), "test");
  EXPECT_EQ(wasbAccount.filePath(), "test");
  EXPECT_EQ(wasbAccount.credKey(), "test");
  EXPECT_EQ(
      wasbAccount.connectionString("123"),
      "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=123;EndpointSuffix=core.windows.net");

  auto wasbsAccount =
      AbfsAccount("wasbs://test@test.blob.core.windows.net/test");
  EXPECT_EQ(wasbsAccount.accountNameWithSuffix(), "test.blob.core.windows.net");
  EXPECT_EQ(wasbsAccount.accountName(), "test");
  EXPECT_EQ(wasbsAccount.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(wasbsAccount.fileSystem(), "test");
  EXPECT_EQ(wasbsAccount.filePath(), "test");
  EXPECT_EQ(wasbsAccount.credKey(), "test");
  EXPECT_EQ(
      wasbsAccount.connectionString("123"),
      "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=123;EndpointSuffix=core.windows.net");
}

TEST(AbfsUtilsTest, CustomEndpoint) {
  try {
    auto abfsAccount =
        AbfsAccount("abfs://testc@testa.dfs.core.window.net/test");
    FAIL() << "Expected Velox exception";
  } catch (facebook::velox::VeloxException const& err) {
    EXPECT_EQ(
        err.message(),
        std::string(
            "Endpoint core.window.net is not valid, please pass a default endpoint using spark.fs.azure.abfs.endpoint"));
  }

  auto abfsAccount =
      AbfsAccount("abfs://testc@testa.dfs.core.window.net/test", "foo.bar.com");
  EXPECT_EQ(abfsAccount.scheme(), "abfs");
  EXPECT_EQ(abfsAccount.accountNameWithSuffix(), "testa.dfs.foo.bar.com");
  EXPECT_EQ(abfsAccount.accountName(), "testa");
  EXPECT_EQ(abfsAccount.endpointSuffix(), "foo.bar.com");
  EXPECT_EQ(abfsAccount.fileSystem(), "testc");
  EXPECT_EQ(abfsAccount.filePath(), "test");
  EXPECT_EQ(abfsAccount.credKey(), "testa");

  auto wasbAccount = AbfsAccount(
      "wasb://testc@testa.blob.core.window.net/test", "foo.bar.com");
  EXPECT_EQ(wasbAccount.scheme(), "wasb");
  EXPECT_EQ(wasbAccount.accountNameWithSuffix(), "testa.blob.foo.bar.com");
  EXPECT_EQ(wasbAccount.accountName(), "testa");
  EXPECT_EQ(wasbAccount.endpointSuffix(), "foo.bar.com");
  EXPECT_EQ(wasbAccount.fileSystem(), "testc");
  EXPECT_EQ(wasbAccount.filePath(), "test");
  EXPECT_EQ(wasbAccount.credKey(), "testa");
}
