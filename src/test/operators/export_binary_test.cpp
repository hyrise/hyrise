#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "import_export/binary.hpp"
#include "operators/export_binary.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

class OperatorsExportBinaryTest : public BaseTest {
 protected:
  void SetUp() override {}

  void TearDown() override { std::remove(filename.c_str()); }

  bool file_exists(const std::string& name) {
    std::ifstream file{name};
    return file.good();
  }

  bool compare_files(const std::string& original_file, const std::string& created_file) {
    std::ifstream original(original_file);
    Assert(original.is_open(), "compare_file: Could not find file " + original_file);

    std::ifstream created(created_file);
    Assert(created.is_open(), "compare_file: Could not find file " + created_file);

    std::istreambuf_iterator<char> iterator_original(original);
    std::istreambuf_iterator<char> iterator_created(created);
    std::istreambuf_iterator<char> end;

    while (iterator_original != end && iterator_created != end) {
      if (*iterator_original != *iterator_created) return false;
      ++iterator_original;
      ++iterator_created;
    }
    return ((iterator_original == end) && (iterator_created == end));
  }

  std::shared_ptr<Table> table;
  const std::string filename = test_data_path + "export_test.bin";
};

TEST_F(OperatorsExportBinaryTest, TwoColumnsNoValues) {
  table = std::make_shared<Table>(30000);
  table->add_column("FirstColumn", DataType::Int);
  table->add_column("SecondColumn", DataType::String);
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/TwoColumnsNoValues.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, SingleChunkSingleFloatColumn) {
  auto table = std::make_shared<Table>(5);
  table->add_column("a", DataType::Float);
  table->append({5.5f});
  table->append({13.0f});
  table->append({16.2f});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/SingleChunkSingleFloatColumn.bin", filename));
}
TEST_F(OperatorsExportBinaryTest, MultipleChunkSingleFloatColumn) {
  auto table = std::make_shared<Table>(2);
  table->add_column("a", DataType::Float);
  table->append({5.5f});
  table->append({13.0f});
  table->append({16.2f});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/MultipleChunkSingleFloatColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, StringValueColumn) {
  auto table = std::make_shared<Table>(5);
  table->add_column("a", DataType::String);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/StringValueColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, StringDictionaryColumn) {
  auto table = std::make_shared<Table>(10);
  table->add_column("a", DataType::String);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  DictionaryCompression::compress_table(*table);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/StringDictionaryColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, AllTypesValueColumn) {
  auto table = std::make_shared<opossum::Table>(2);
  table->add_column("a", DataType::String);
  table->add_column("b", DataType::Int);
  table->add_column("c", DataType::Long);
  table->add_column("d", DataType::Float);
  table->add_column("e", DataType::Double);
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesValueColumn.bin", filename));
}
TEST_F(OperatorsExportBinaryTest, AllTypesDictionaryColumn) {
  auto table = std::make_shared<opossum::Table>(2);
  table->add_column("a", DataType::String);
  table->add_column("b", DataType::Int);
  table->add_column("c", DataType::Long);
  table->add_column("d", DataType::Float);
  table->add_column("e", DataType::Double);
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  DictionaryCompression::compress_table(*table);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesDictionaryColumn.bin", filename));
}
TEST_F(OperatorsExportBinaryTest, AllTypesMixColumn) {
  auto table = std::make_shared<opossum::Table>(2);
  table->add_column("a", DataType::String);
  table->add_column("b", DataType::Int);
  table->add_column("c", DataType::Long);
  table->add_column("d", DataType::Float);
  table->add_column("e", DataType::Double);
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  DictionaryCompression::compress_chunks(*table, {ChunkID{0}});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesMixColumn.bin", filename));
}

// A table with reference columns is materialized while exporting. The content of the export file should not be
// different from a exported table with ValueColumns and the same content.
// They only differ in the table's chunk size. The result table of a scan has no chunk size limit.
TEST_F(OperatorsExportBinaryTest, AllTypesReferenceColumn) {
  auto table = std::make_shared<opossum::Table>(2);
  table->add_column("a", DataType::String);
  table->add_column("b", DataType::Int);
  table->add_column("c", DataType::Long);
  table->add_column("d", DataType::Float);
  table->add_column("e", DataType::Double);
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(table_wrapper, ColumnID{1}, PredicateCondition::NotEquals, 5);
  scan->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(scan, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesValueColumnMaxChunkSize.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, EmptyStringsValueColumn) {
  auto table = std::make_shared<opossum::Table>(10);
  table->add_column("a", DataType::String);
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/EmptyStringsValueColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, EmptyStringsDictionaryColumn) {
  auto table = std::make_shared<opossum::Table>(10);
  table->add_column("a", DataType::String);
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});

  DictionaryCompression::compress_table(*table);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/EmptyStringsDictionaryColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, AllTypesNullValues) {
  auto table = std::make_shared<opossum::Table>();
  table->add_column("a", DataType::Int, true);
  table->add_column("b", DataType::Float, true);
  table->add_column("c", DataType::Long, true);
  table->add_column("d", DataType::String, true);
  table->add_column("e", DataType::Double, true);

  table->append({opossum::NULL_VALUE, 1.1f, 100, "one", 1.11});
  table->append({2, opossum::NULL_VALUE, 200, "two", 2.22});
  table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  table->append({4, 4.4f, 400, opossum::NULL_VALUE, 4.44});
  table->append({5, 5.5f, 500, "five", opossum::NULL_VALUE});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesNullValues.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, AllTypesDictionaryNullValues) {
  auto table = std::make_shared<opossum::Table>();
  table->add_column("a", DataType::Int, true);
  table->add_column("b", DataType::Float, true);
  table->add_column("c", DataType::Long, true);
  table->add_column("d", DataType::String, true);
  table->add_column("e", DataType::Double, true);

  table->append({opossum::NULL_VALUE, 1.1f, 100, "one", 1.11});
  table->append({2, opossum::NULL_VALUE, 200, "two", 2.22});
  table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  table->append({4, 4.4f, 400, opossum::NULL_VALUE, 4.44});
  table->append({5, 5.5f, 500, "five", opossum::NULL_VALUE});

  DictionaryCompression::compress_table(*table);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesDictionaryNullValues.bin", filename));
}

}  // namespace opossum
