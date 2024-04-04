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
  EXPECT_EQ(abfsAccount.etag(), "");
  EXPECT_EQ(abfsAccount.length(), -1);
  EXPECT_EQ(abfsAccount.url(), "abfs://test@test.dfs.core.windows.net/test");
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
  EXPECT_EQ(abfssAccount.etag(), "");
  EXPECT_EQ(abfssAccount.length(), -1);
  EXPECT_EQ(
      abfssAccount.url(),
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(abfssAccount.credKey(), "test");

  // Test with etag and file length suffix
  auto abfssAccountWithSuffix = AbfsAccount(
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet&etag=1681369868000&flength=102345");
  EXPECT_EQ(abfssAccountWithSuffix.scheme(), "abfss");
  EXPECT_EQ(
      abfssAccountWithSuffix.accountNameWithSuffix(),
      "test.dfs.core.windows.net");
  EXPECT_EQ(abfssAccountWithSuffix.accountName(), "test");
  EXPECT_EQ(abfssAccountWithSuffix.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfssAccountWithSuffix.fileSystem(), "test");
  EXPECT_EQ(
      abfssAccountWithSuffix.filePath(),
      "sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(abfssAccountWithSuffix.etag(), "1681369868000");
  EXPECT_EQ(abfssAccountWithSuffix.length(), 102345);
  EXPECT_EQ(
      abfssAccountWithSuffix.url(),
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(abfssAccountWithSuffix.credKey(), "test");

  auto abfssAccountWithRubbishLengthSuffix = AbfsAccount(
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet&etag=1681369868000&flength=10a2345");
  EXPECT_EQ(abfssAccountWithRubbishLengthSuffix.scheme(), "abfss");
  EXPECT_EQ(
      abfssAccountWithRubbishLengthSuffix.accountNameWithSuffix(),
      "test.dfs.core.windows.net");
  EXPECT_EQ(abfssAccountWithRubbishLengthSuffix.accountName(), "test");
  EXPECT_EQ(
      abfssAccountWithRubbishLengthSuffix.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfssAccountWithRubbishLengthSuffix.fileSystem(), "test");
  EXPECT_EQ(
      abfssAccountWithRubbishLengthSuffix.filePath(),
      "sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(abfssAccountWithRubbishLengthSuffix.etag(), "1681369868000");
  EXPECT_EQ(abfssAccountWithRubbishLengthSuffix.length(), -1);
  EXPECT_EQ(
      abfssAccountWithRubbishLengthSuffix.url(),
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000.snappy.parquet");
  EXPECT_EQ(abfssAccountWithRubbishLengthSuffix.credKey(), "test");

  auto abfssAccountWithDupSuffix = AbfsAccount(
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000&etag=.snappy.parquet&etag=1681369868000&flength=102345");
  EXPECT_EQ(abfssAccountWithDupSuffix.scheme(), "abfss");
  EXPECT_EQ(
      abfssAccountWithDupSuffix.accountNameWithSuffix(),
      "test.dfs.core.windows.net");
  EXPECT_EQ(abfssAccountWithDupSuffix.accountName(), "test");
  EXPECT_EQ(abfssAccountWithDupSuffix.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfssAccountWithDupSuffix.fileSystem(), "test");
  EXPECT_EQ(
      abfssAccountWithDupSuffix.filePath(),
      "sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000&etag=.snappy.parquet");
  EXPECT_EQ(abfssAccountWithDupSuffix.etag(), "1681369868000");
  EXPECT_EQ(abfssAccountWithDupSuffix.length(), 102345);
  EXPECT_EQ(
      abfssAccountWithDupSuffix.url(),
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000&etag=.snappy.parquet");
  EXPECT_EQ(abfssAccountWithDupSuffix.credKey(), "test");

  auto abfssAccountWithNoSuffix = AbfsAccount(
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000&etag=.snappy.parquet");
  EXPECT_EQ(abfssAccountWithNoSuffix.scheme(), "abfss");
  EXPECT_EQ(
      abfssAccountWithNoSuffix.accountNameWithSuffix(),
      "test.dfs.core.windows.net");
  EXPECT_EQ(abfssAccountWithNoSuffix.accountName(), "test");
  EXPECT_EQ(abfssAccountWithNoSuffix.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(abfssAccountWithNoSuffix.fileSystem(), "test");
  EXPECT_EQ(
      abfssAccountWithNoSuffix.filePath(),
      "sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000&etag=.snappy.parquet");
  EXPECT_EQ(abfssAccountWithNoSuffix.etag(), "");
  EXPECT_EQ(abfssAccountWithNoSuffix.length(), -1);
  EXPECT_EQ(
      abfssAccountWithNoSuffix.url(),
      "abfss://test@test.dfs.core.windows.net/sf_1/store_sales/ss_sold_date_sk=2450816/part-00002-a29c25f1-4638-494e-8428-a84f51dcea41.c000&etag=.snappy.parquet");
  EXPECT_EQ(abfssAccountWithNoSuffix.credKey(), "test");

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
  EXPECT_EQ(abfssAccountWithSpecialCharacters.etag(), "");
  EXPECT_EQ(abfssAccountWithSpecialCharacters.length(), -1);
  EXPECT_EQ(
      abfssAccountWithSpecialCharacters.url(),
      "abfss://test@test.dfs.core.windows.net/main@dir/brand#51/sub dir/test.txt");

  // china cloud
  auto abfsChinaCloudAccount = AbfsAccount(
      "abfs://test@test.dfs.core.chinacloudapi.cn/test&etag=1681369868000&flength=102345");
  EXPECT_EQ(abfsChinaCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsChinaCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.chinacloudapi.cn");
  EXPECT_EQ(abfsChinaCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.endpointSuffix(), "core.chinacloudapi.cn");
  EXPECT_EQ(abfsChinaCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.filePath(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.credKey(), "test");
  EXPECT_EQ(abfsChinaCloudAccount.etag(), "1681369868000");
  EXPECT_EQ(abfsChinaCloudAccount.length(), 102345);
  EXPECT_EQ(
      abfsChinaCloudAccount.url(),
      "abfs://test@test.dfs.core.chinacloudapi.cn/test");

  // us gov cloud
  auto abfsUsGovCloudAccount = AbfsAccount(
      "abfs://test@test.dfs.core.usgovcloudapi.net/test&etag=1681369868000&flength=102345");
  EXPECT_EQ(abfsUsGovCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsUsGovCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.usgovcloudapi.net");
  EXPECT_EQ(abfsUsGovCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.endpointSuffix(), "core.usgovcloudapi.net");
  EXPECT_EQ(abfsUsGovCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.filePath(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.credKey(), "test");
  EXPECT_EQ(abfsUsGovCloudAccount.etag(), "1681369868000");
  EXPECT_EQ(abfsUsGovCloudAccount.length(), 102345);
  EXPECT_EQ(
      abfsUsGovCloudAccount.url(),
      "abfs://test@test.dfs.core.usgovcloudapi.net/test");

  // germany cloud
  auto abfsGermanyCloudAccount = AbfsAccount(
      "abfs://test@test.dfs.core.cloudapi.de/test&etag=1681369868000&flength=102345");
  EXPECT_EQ(abfsGermanyCloudAccount.scheme(), "abfs");
  EXPECT_EQ(
      abfsGermanyCloudAccount.accountNameWithSuffix(),
      "test.dfs.core.cloudapi.de");
  EXPECT_EQ(abfsGermanyCloudAccount.accountName(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.endpointSuffix(), "core.cloudapi.de");
  EXPECT_EQ(abfsGermanyCloudAccount.fileSystem(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.filePath(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.credKey(), "test");
  EXPECT_EQ(abfsGermanyCloudAccount.etag(), "1681369868000");
  EXPECT_EQ(abfsGermanyCloudAccount.length(), 102345);
  EXPECT_EQ(
      abfsGermanyCloudAccount.url(),
      "abfs://test@test.dfs.core.cloudapi.de/test");

  // Fabric
  auto abfsDXTAccount = AbfsAccount(
      "abfss://test@dxt-onelake.dfs.fabric.microsoft.com/testPath&etag=1681369868000&flength=102345");
  EXPECT_EQ(abfsDXTAccount.scheme(), "abfss");
  EXPECT_EQ(
      abfsDXTAccount.accountNameWithSuffix(),
      "dxt-onelake.dfs.fabric.microsoft.com");
  EXPECT_EQ(abfsDXTAccount.accountName(), "dxt-onelake");
  EXPECT_EQ(abfsDXTAccount.endpointSuffix(), "fabric.microsoft.com");
  EXPECT_EQ(abfsDXTAccount.fileSystem(), "test");
  EXPECT_EQ(abfsDXTAccount.filePath(), "testPath");
  EXPECT_EQ(abfsDXTAccount.credKey(), "dxt-onelake");
  EXPECT_EQ(abfsDXTAccount.etag(), "1681369868000");
  EXPECT_EQ(abfsDXTAccount.length(), 102345);
  EXPECT_EQ(
      abfsDXTAccount.url(),
      "abfss://test@dxt-onelake.dfs.fabric.microsoft.com/testPath");

  auto abfsMSITAccount = AbfsAccount(
      "abfss://test@msit-onelake.blob.pbidedicated.windows.net/testPath&etag=1681369868000&flength=102345");
  EXPECT_EQ(abfsMSITAccount.scheme(), "abfss");
  EXPECT_EQ(
      abfsMSITAccount.accountNameWithSuffix(),
      "msit-onelake.blob.pbidedicated.windows.net");
  EXPECT_EQ(abfsMSITAccount.accountName(), "msit-onelake");
  EXPECT_EQ(abfsMSITAccount.endpointSuffix(), "pbidedicated.windows.net");
  EXPECT_EQ(abfsMSITAccount.fileSystem(), "test");
  EXPECT_EQ(abfsMSITAccount.filePath(), "testPath");
  EXPECT_EQ(abfsMSITAccount.credKey(), "msit-onelake");
  EXPECT_EQ(abfsMSITAccount.etag(), "1681369868000");
  EXPECT_EQ(abfsMSITAccount.length(), 102345);
  EXPECT_EQ(
      abfsMSITAccount.url(),
      "abfss://test@msit-onelake.blob.pbidedicated.windows.net/testPath");

  auto abfsEDogAccount = AbfsAccount(
      "abfss://velox@onelake-int-edog.dfs.pbidedicated.windows-int.net/velox.Lakehouse/Files/testPath&etag=1681369868000&flength=102345");
  EXPECT_EQ(abfsEDogAccount.scheme(), "abfss");
  EXPECT_EQ(
      abfsEDogAccount.accountNameWithSuffix(),
      "onelake-int-edog.dfs.pbidedicated.windows-int.net");
  EXPECT_EQ(abfsEDogAccount.accountName(), "onelake-int-edog");
  EXPECT_EQ(abfsEDogAccount.endpointSuffix(), "pbidedicated.windows-int.net");
  EXPECT_EQ(abfsEDogAccount.fileSystem(), "velox");
  EXPECT_EQ(abfsEDogAccount.filePath(), "velox.Lakehouse/Files/testPath");
  EXPECT_EQ(abfsEDogAccount.credKey(), "onelake-int-edog");
  EXPECT_EQ(abfsEDogAccount.etag(), "1681369868000");
  EXPECT_EQ(abfsEDogAccount.length(), 102345);
  EXPECT_EQ(
      abfsEDogAccount.url(),
      "abfss://velox@onelake-int-edog.dfs.pbidedicated.windows-int.net/velox.Lakehouse/Files/testPath");

  auto wasbAccount = AbfsAccount(
      "wasb://test@test.blob.core.windows.net/test&etag=1681369868000&flength=102345");
  EXPECT_EQ(wasbAccount.accountNameWithSuffix(), "test.blob.core.windows.net");
  EXPECT_EQ(wasbAccount.accountName(), "test");
  EXPECT_EQ(wasbAccount.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(wasbAccount.fileSystem(), "test");
  EXPECT_EQ(wasbAccount.filePath(), "test");
  EXPECT_EQ(wasbAccount.credKey(), "test");
  EXPECT_EQ(
      wasbAccount.connectionString("123"),
      "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=123;EndpointSuffix=core.windows.net");
  EXPECT_EQ(wasbAccount.etag(), "1681369868000");
  EXPECT_EQ(wasbAccount.length(), 102345);
  EXPECT_EQ(wasbAccount.url(), "wasb://test@test.blob.core.windows.net/test");

  auto wasbsAccount = AbfsAccount(
      "wasbs://test@test.blob.core.windows.net/test&etag=1681369868000&flength=102345");
  EXPECT_EQ(wasbsAccount.accountNameWithSuffix(), "test.blob.core.windows.net");
  EXPECT_EQ(wasbsAccount.accountName(), "test");
  EXPECT_EQ(wasbsAccount.endpointSuffix(), "core.windows.net");
  EXPECT_EQ(wasbsAccount.fileSystem(), "test");
  EXPECT_EQ(wasbsAccount.filePath(), "test");
  EXPECT_EQ(wasbsAccount.credKey(), "test");
  EXPECT_EQ(
      wasbsAccount.connectionString("123"),
      "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=123;EndpointSuffix=core.windows.net");
  EXPECT_EQ(wasbsAccount.etag(), "1681369868000");
  EXPECT_EQ(wasbsAccount.length(), 102345);
  EXPECT_EQ(wasbsAccount.url(), "wasbs://test@test.blob.core.windows.net/test");
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

  auto abfsAccount = AbfsAccount(
      "abfs://testc@testa.dfs.core.window.net/test", true, "foo.bar.com");
  EXPECT_EQ(abfsAccount.scheme(), "abfs");
  EXPECT_EQ(abfsAccount.accountNameWithSuffix(), "testa.dfs.foo.bar.com");
  EXPECT_EQ(abfsAccount.accountName(), "testa");
  EXPECT_EQ(abfsAccount.endpointSuffix(), "foo.bar.com");
  EXPECT_EQ(abfsAccount.fileSystem(), "testc");
  EXPECT_EQ(abfsAccount.filePath(), "test");
  EXPECT_EQ(abfsAccount.credKey(), "testa");

  auto wasbAccount = AbfsAccount(
      "wasb://testc@testa.blob.core.window.net/test", true, "foo.bar.com");
  EXPECT_EQ(wasbAccount.scheme(), "wasb");
  EXPECT_EQ(wasbAccount.accountNameWithSuffix(), "testa.blob.foo.bar.com");
  EXPECT_EQ(wasbAccount.accountName(), "testa");
  EXPECT_EQ(wasbAccount.endpointSuffix(), "foo.bar.com");
  EXPECT_EQ(wasbAccount.fileSystem(), "testc");
  EXPECT_EQ(wasbAccount.filePath(), "test");
  EXPECT_EQ(wasbAccount.credKey(), "testa");
}
