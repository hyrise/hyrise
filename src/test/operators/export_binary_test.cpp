#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/import_export/binary.hpp"
#include "../../lib/operators/export_binary.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class OperatorsExportBinaryTest : public BaseTest {
 protected:
  void SetUp() override {}

  void TearDown() override { std::remove(filename.c_str()); }

  bool fileExists(const std::string& name) {
    std::ifstream file{name};
    return file.good();
  }

  bool compare_files(const std::string& original_file, const std::string& created_file) {
    std::ifstream original(original_file);
    std::ifstream created(created_file);

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
  const std::string filename = "/tmp/output.bin";
};

TEST_F(OperatorsExportBinaryTest, TwoColumnsNoValues) {
  table = std::make_shared<Table>(30000);
  table->add_column("FirstColumn", "int");
  table->add_column("SecondColumn", "string");
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/TwoColumnsNoValues.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, SingleChunkSingleFloatColumn) {
  auto table = std::make_shared<Table>(5);
  table->add_column("a", "float");
  table->append({5.5f});
  table->append({13.0f});
  table->append({16.2f});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/SingleChunkSingleFloatColumn.bin", filename));
}
TEST_F(OperatorsExportBinaryTest, MultipleChunkSingleFloatColumn) {
  auto table = std::make_shared<Table>(2);
  table->add_column("a", "float");
  table->append({5.5f});
  table->append({13.0f});
  table->append({16.2f});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/MultipleChunkSingleFloatColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, StringValueColumn) {
  auto table = std::make_shared<Table>(5);
  table->add_column("a", "string");
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/StringValueColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, StringDictionaryColumn) {
  auto table = std::make_shared<Table>(10);
  table->add_column("a", "string");
  table->append({"This"});
  table->append({"is"});
  table->append({"a"});
  table->append({"test"});
  table->compress_chunk(0);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/StringDictionaryColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, AllTypesValueColumn) {
  auto table = std::make_shared<opossum::Table>(2);
  table->add_column("a", "string");
  table->add_column("b", "int");
  table->add_column("c", "long");
  table->add_column("d", "float");
  table->add_column("e", "double");
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesValueColumn.bin", filename));
}
TEST_F(OperatorsExportBinaryTest, AllTypesDictionaryColumn) {
  auto table = std::make_shared<opossum::Table>(2);
  table->add_column("a", "string");
  table->add_column("b", "int");
  table->add_column("c", "long");
  table->add_column("d", "float");
  table->add_column("e", "double");
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});
  table->compress_chunk(0);
  table->compress_chunk(1);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesDictionaryColumn.bin", filename));
}
TEST_F(OperatorsExportBinaryTest, AllTypesMixColumn) {
  auto table = std::make_shared<opossum::Table>(2);
  table->add_column("a", "string");
  table->add_column("b", "int");
  table->add_column("c", "long");
  table->add_column("d", "float");
  table->add_column("e", "double");
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});
  table->compress_chunk(0);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesMixColumn.bin", filename));
}

// A table with reference columns is materialized while exporting. The content of the export file should not be
// different from a exported table with ValueColumns and the same content.
// They only differ in the table's chunk size. The result table of a scan has no chunk size limit.
TEST_F(OperatorsExportBinaryTest, AllTypesReferenceColumn) {
  auto table = std::make_shared<opossum::Table>(2);
  table->add_column("a", "string");
  table->add_column("b", "int");
  table->add_column("c", "long");
  table->add_column("d", "float");
  table->add_column("e", "double");
  table->append({"AAAAA", 1, static_cast<int64_t>(100), 1.1f, 11.1});
  table->append({"BBBBBBBBBB", 2, static_cast<int64_t>(200), 2.2f, 22.2});
  table->append({"CCCCCCCCCCCCCCC", 3, static_cast<int64_t>(300), 3.3f, 33.3});
  table->append({"DDDDDDDDDDDDDDDDDDDD", 4, static_cast<int64_t>(400), 4.4f, 44.4});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto scan = std::make_shared<TableScan>(table_wrapper, "b", "!=", 5);
  scan->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(scan, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/AllTypesValueColumnNoChunkSizeLimit.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, EmptyStringsValueColumn) {
  auto table = std::make_shared<opossum::Table>(10);
  table->add_column("a", "string");
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/EmptyStringsValueColumn.bin", filename));
}

TEST_F(OperatorsExportBinaryTest, EmptyStringsDictionaryColumn) {
  auto table = std::make_shared<opossum::Table>(10);
  table->add_column("a", "string");
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});
  table->append({""});
  table->compress_chunk(0);

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto ex = std::make_shared<opossum::ExportBinary>(table_wrapper, filename);
  ex->execute();

  EXPECT_TRUE(fileExists(filename));
  EXPECT_TRUE(compare_files("src/test/binary/EmptyStringsDictionaryColumn.bin", filename));
}

}  // namespace opossum
