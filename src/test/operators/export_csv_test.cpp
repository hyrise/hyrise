#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/export_csv.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "operators/csv.hpp"

namespace opossum {

class OperatorsExportCsvTest : public BaseTest {
 protected:
  void SetUp() override {
    table = std::make_shared<Table>(2);
    table->add_column("a", "int");
    table->add_column("b", "string");
    table->add_column("c", "float");
  }

  void TearDown() override {
    std::remove(fullname.c_str());
    std::remove(meta_fullname.c_str());
  }

  bool fileExists(const std::string& name) {
    std::ifstream file{name};
    return file.good();
  }

  bool compare_file(const std::string& filename, const std::string& expected_content) {
    std::ifstream t(filename);

    std::string content((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());

    int equality = content.compare(expected_content);
    if (equality != 0) {
      std::cout << equality << std::endl;
      std::cout << "Comparison of file to expected content failed. " << std::endl;
      std::cout << "Expected:" << std::endl;
      std::cout << expected_content << std::endl;
      std::cout << "Actual:" << std::endl;
      std::cout << content << std::endl;
    }
    return equality == 0;
  }

  std::shared_ptr<Table> table;
  const std::string directory = "/tmp";
  const std::string filename = "output";
  const std::string fullname = directory + '/' + filename + csv::file_extension;
  const std::string meta_fullname = directory + '/' + filename + csv::meta_file_extension;
};

TEST_F(OperatorsExportCsvTest, SingleChunkAndMetaInfo) {
  table->append({1, "Hallo", 3.5f});
  StorageManager::get().add_table("table_a", std::move(table));
  auto gt = std::make_shared<GetTable>("table_a");
  gt->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(gt, directory, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(fullname));
  EXPECT_TRUE(fileExists(meta_fullname));
  EXPECT_TRUE(compare_file(fullname, "1,\"Hallo\",3.5\n"));
}

TEST_F(OperatorsExportCsvTest, EscapeString) {
  table->append({1, "Sie sagte: \"Mir geht's gut, und dir?\"", 3.5f});
  StorageManager::get().add_table("table_a", std::move(table));
  auto gt = std::make_shared<GetTable>("table_a");
  gt->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(gt, directory, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(fullname));
  EXPECT_TRUE(fileExists(meta_fullname));
  EXPECT_TRUE(compare_file(fullname, "1,\"Sie sagte: \"\"Mir geht's gut, und dir?\"\"\",3.5\n"));
}

TEST_F(OperatorsExportCsvTest, MultipleChunks) {
  table->append({1, "Hallo", 3.5f});
  table->append({2, "Welt!", 3.5f});
  table->append({3, "Gute", -4.0f});
  table->append({4, "Nacht", 7.5f});
  table->append({5, "Guten", 8.33f});
  table->append({6, "Tag", 3.5f});
  StorageManager::get().add_table("table_a", std::move(table));
  auto gt = std::make_shared<GetTable>("table_a");
  gt->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(gt, directory, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(fullname));
  EXPECT_TRUE(fileExists(meta_fullname));
  EXPECT_TRUE(compare_file(fullname,
                           "1,\"Hallo\",3.5\n"
                           "2,\"Welt!\",3.5\n"
                           "3,\"Gute\",-4\n"
                           "4,\"Nacht\",7.5\n"
                           "5,\"Guten\",8.33\n"
                           "6,\"Tag\",3.5\n"));
}

TEST_F(OperatorsExportCsvTest, DictionaryColumn) {
  table->append({1, "Hallo", 3.5f});
  table->append({1, "Hallo", 3.5f});
  table->append({1, "Hallo3", 3.55f});

  table->compress_chunk(0);

  StorageManager::get().add_table("table_a", std::move(table));
  auto gt = std::make_shared<GetTable>("table_a");
  gt->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(gt, directory, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(fullname));
  EXPECT_TRUE(fileExists(meta_fullname));
  EXPECT_TRUE(compare_file(fullname,
                           "1,\"Hallo\",3.5\n"
                           "1,\"Hallo\",3.5\n"
                           "1,\"Hallo3\",3.55\n"));
}

TEST_F(OperatorsExportCsvTest, ExportAllTypes) {
  std::shared_ptr<Table> newTable = std::make_shared<Table>(2);
  newTable->add_column("a", "int");
  newTable->add_column("b", "string");
  newTable->add_column("c", "float");
  // Don't use long until error with MacOS is fixed
  // newTable->add_column("d", "long");
  newTable->add_column("e", "double");
  newTable->append({1, "Hallo", 3.5f, 2.333});

  StorageManager::get().add_table("table_b", std::move(newTable));
  auto gt = std::make_shared<GetTable>("table_b");
  gt->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(gt, directory, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(fullname));
  EXPECT_TRUE(fileExists(meta_fullname));
  EXPECT_TRUE(compare_file(fullname, "1,\"Hallo\",3.5,2.333\n"));
}

}  // namespace opossum
