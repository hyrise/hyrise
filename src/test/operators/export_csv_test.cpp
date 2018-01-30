#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "import_export/csv_meta.hpp"
#include "operators/export_csv.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

class OperatorsExportCsvTest : public BaseTest {
 protected:
  void SetUp() override {
    table = std::make_shared<Table>(2);
    table->add_column("a", DataType::Int);
    table->add_column("b", DataType::String);
    table->add_column("c", DataType::Float);
  }

  void TearDown() override {
    std::remove(filename.c_str());
    std::remove(meta_filename.c_str());
  }

  bool file_exists(const std::string& name) {
    std::ifstream file{name};
    return file.good();
  }

  bool compare_file(const std::string& filename, const std::string& expected_content) {
    std::ifstream t(filename);
    Assert(t.is_open(), "compare_file: Could not find file " + filename);

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
  const std::string filename = test_data_path + "export_test.csv";
  const std::string meta_filename = filename + CsvMeta::META_FILE_EXTENSION;
};

TEST_F(OperatorsExportCsvTest, SingleChunkAndMetaInfo) {
  table->append({1, "Hallo", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));
  EXPECT_TRUE(compare_file(filename, "1,\"Hallo\",3.5\n"));
}

TEST_F(OperatorsExportCsvTest, EscapeString) {
  table->append({1, "Sie sagte: \"Mir geht's gut, und dir?\"", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));
  EXPECT_TRUE(compare_file(filename, "1,\"Sie sagte: \"\"Mir geht's gut, und dir?\"\"\",3.5\n"));
}

TEST_F(OperatorsExportCsvTest, MultipleChunks) {
  table->append({1, "Hallo", 3.5f});
  table->append({2, "Welt!", 3.5f});
  table->append({3, "Gute", -4.0f});
  table->append({4, "Nacht", 7.5f});
  table->append({5, "Guten", 8.33f});
  table->append({6, "Tag", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));
  EXPECT_TRUE(compare_file(filename,
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

  DictionaryCompression::compress_chunks(*table, {ChunkID{0}});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));
  EXPECT_TRUE(compare_file(filename,
                           "1,\"Hallo\",3.5\n"
                           "1,\"Hallo\",3.5\n"
                           "1,\"Hallo3\",3.55\n"));
}

TEST_F(OperatorsExportCsvTest, ReferenceColumn) {
  table->append({1, "abc", 1.1f});
  table->append({2, "asdf", 2.2f});
  table->append({3, "hello", 3.3f});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0}, PredicateCondition::LessThan, 5);
  scan->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(scan, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));
  EXPECT_TRUE(compare_file(filename,
                           "1,\"abc\",1.1\n"
                           "2,\"asdf\",2.2\n"
                           "3,\"hello\",3.3\n"));
}

TEST_F(OperatorsExportCsvTest, ExportAllTypes) {
  std::shared_ptr<Table> new_table = std::make_shared<Table>(2);
  new_table->add_column("a", DataType::Int);
  new_table->add_column("b", DataType::String);
  new_table->add_column("c", DataType::Float);
  new_table->add_column("d", DataType::Long);
  new_table->add_column("e", DataType::Double);
  new_table->append({1, "Hallo", 3.5f, static_cast<int64_t>(12), 2.333});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(new_table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));
  EXPECT_TRUE(compare_file(filename, "1,\"Hallo\",3.5,12,2.333\n"));
}

TEST_F(OperatorsExportCsvTest, NonsensePath) {
  table->append({1, "hello", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportCsv>(table_wrapper, "this/path/does/not/exist");
  EXPECT_THROW(ex->execute(), std::exception);
}

TEST_F(OperatorsExportCsvTest, ExportNumericNullValues) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 4));
  table_wrapper->execute();

  auto ex = std::make_shared<ExportCsv>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));
  EXPECT_TRUE(compare_file(filename,
                           "12345,458.7\n"
                           "123,\n"
                           ",456.7\n"
                           "1234,457.7\n"));
}

TEST_F(OperatorsExportCsvTest, ExportStringNullValues) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/string_with_null.tbl", 4));
  table_wrapper->execute();

  auto ex = std::make_shared<ExportCsv>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));
  EXPECT_TRUE(compare_file(filename,
                           "\"xxx\"\n"
                           "\"www\"\n"
                           "\n"
                           "\"zzz\"\n"));
}

TEST_F(OperatorsExportCsvTest, ExportNullValuesMeta) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 4));
  table_wrapper->execute();

  auto ex = std::make_shared<ExportCsv>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(file_exists(meta_filename));

  auto meta_information = process_csv_meta_file(meta_filename);
  EXPECT_TRUE(meta_information.columns.at(0).nullable);
  EXPECT_TRUE(meta_information.columns.at(1).nullable);
}

}  // namespace opossum
