#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "import_export/binary.hpp"
#include "operators/export_binary.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
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
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("FirstColumn", DataType::Int);
  column_definitions.emplace_back("SecondColumn", DataType::String);

  table = std::make_shared<Table>(column_definitions, TableType::Data, 30000);
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/TwoColumnsNoValues.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, SingleChunkSingleFloatColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Float);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
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
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Float);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
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

TEST_F(OperatorsExportBinaryTest, StringValueSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/StringValueSegment.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, StringDictionarySegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/StringDictionarySegment.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, FixedStringDictionarySegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  ChunkEncoder::encode_all_chunks(table, EncodingType::FixedStringDictionary);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/StringDictionarySegment.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, AllTypesValueSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);
  column_definitions.emplace_back("b", DataType::Int);
  column_definitions.emplace_back("c", DataType::Long);
  column_definitions.emplace_back("d", DataType::Float);
  column_definitions.emplace_back("e", DataType::Double);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesValueSegment.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, AllTypesDictionarySegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);
  column_definitions.emplace_back("b", DataType::Int);
  column_definitions.emplace_back("c", DataType::Long);
  column_definitions.emplace_back("d", DataType::Float);
  column_definitions.emplace_back("e", DataType::Double);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesDictionarySegment.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, AllTypesMixColumn) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);
  column_definitions.emplace_back("b", DataType::Int);
  column_definitions.emplace_back("c", DataType::Long);
  column_definitions.emplace_back("d", DataType::Float);
  column_definitions.emplace_back("e", DataType::Double);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  ChunkEncoder::encode_chunks(table, {ChunkID{0}}, EncodingType::Dictionary);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesMixColumn.bin", filename));
}

// A table with reference segments is materialized while exporting. The content of the export file should not be
// different from a exported table with ValueSegments and the same content.
// They only differ in the table's chunk size. The result table of a scan has no chunk size limit.
TEST_F(OperatorsExportBinaryTest, AllTypesReferenceSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);
  column_definitions.emplace_back("b", DataType::Int);
  column_definitions.emplace_back("c", DataType::Long);
  column_definitions.emplace_back("d", DataType::Float);
  column_definitions.emplace_back("e", DataType::Double);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);

  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan =
      std::make_shared<TableScan>(table_wrapper, OperatorScanPredicate{ColumnID{1}, PredicateCondition::NotEquals, 5});
  scan->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(scan, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesValueSegmentMaxChunkSize.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, EmptyStringsValueSegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
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
  EXPECT_TRUE(compare_files("src/test/binary/EmptyStringsValueSegment.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, EmptyStringsDictionarySegment) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data, 10);
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});

  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/EmptyStringsDictionarySegment.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, AllTypesNullValues) {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);
  column_definitions.emplace_back("b", DataType::Float, true);
  column_definitions.emplace_back("c", DataType::Long, true);
  column_definitions.emplace_back("d", DataType::String, true);
  column_definitions.emplace_back("e", DataType::Double, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

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
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, true);
  column_definitions.emplace_back("b", DataType::Float, true);
  column_definitions.emplace_back("c", DataType::Long, true);
  column_definitions.emplace_back("d", DataType::String, true);
  column_definitions.emplace_back("e", DataType::Double, true);

  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  table->append({opossum::NULL_VALUE, 1.1f, 100, "one", 1.11});
  table->append({2, opossum::NULL_VALUE, 200, "two", 2.22});
  table->append({3, 3.3f, opossum::NULL_VALUE, "three", 3.33});
  table->append({4, 4.4f, 400, opossum::NULL_VALUE, 4.44});
  table->append({5, 5.5f, 500, "five", opossum::NULL_VALUE});

  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesDictionaryNullValues.bin", filename));
}

}  // namespace opossum
