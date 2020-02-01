#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "constant_mappings.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "operators/export.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class OperatorsExportTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::String, false);
    column_definitions.emplace_back("c", DataType::Float, false);

    table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
  }

  void TearDown() override {
    std::remove(test_filename.c_str());
    std::remove(test_meta_filename.c_str());
  }

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
  const std::string test_filename = test_data_path + "export_test";
  const std::string test_meta_filename = test_filename + CsvMeta::META_FILE_EXTENSION;
  const std::string reference_filepath = "resources/test_data/";
  const std::map<FileType, std::string> reference_filenames{{FileType::Binary, "bin/float.bin"},
                                                            {FileType::Csv, "csv/float.csv"}};
  const std::map<FileType, std::string> file_extensions{{FileType::Binary, ".bin"}, {FileType::Csv, ".csv"}};
};

class OperatorsExportMultiFileTypeTest : public OperatorsExportTest, public ::testing::WithParamInterface<FileType> {};

auto export_test_formatter = [](const ::testing::TestParamInfo<FileType> info) {
  auto stream = std::stringstream{};
  stream << info.param;

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(FileTypes, OperatorsExportMultiFileTypeTest,
                         ::testing::Values(FileType::Csv, FileType::Binary), export_test_formatter);

TEST_P(OperatorsExportMultiFileTypeTest, ExportWithFileType) {
  auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, 5);
  table->append({1.1f});
  table->append({2.2f});
  table->append({3.3f});
  table->append({4.4f});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  std::string reference_filename = reference_filepath + reference_filenames.at(GetParam());
  auto exporter = std::make_shared<opossum::Export>(table_wrapper, test_filename, GetParam());
  exporter->execute();

  EXPECT_TRUE(file_exists(test_filename));
  EXPECT_TRUE(compare_files(reference_filename, test_filename));
}

TEST_P(OperatorsExportMultiFileTypeTest, ExportWithoutFileType) {
  auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Float, false}}, TableType::Data, 5);
  table->append({1.1f});
  table->append({2.2f});
  table->append({3.3f});
  table->append({4.4f});

  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  auto filename = test_filename + file_extensions.at(GetParam());
  auto reference_filename = reference_filepath + reference_filenames.at(GetParam());
  auto exporter = std::make_shared<opossum::Export>(table_wrapper, filename);
  exporter->execute();

  EXPECT_TRUE(file_exists(filename));
  EXPECT_TRUE(compare_files(reference_filename, filename));
}

TEST_F(OperatorsExportTest, NonsensePath) {
  table->append({1, "hello", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto exporter = std::make_shared<opossum::Export>(table_wrapper, "this/path/does/not/exist");
  EXPECT_THROW(exporter->execute(), std::exception);
}

TEST_F(OperatorsExportTest, EmptyPath) {
  table->append({1, "hello", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto exporter = std::make_shared<opossum::Export>(table_wrapper, "");
  EXPECT_THROW(exporter->execute(), std::exception);
}

TEST_F(OperatorsExportTest, UnknownFileExtension) {
  table->append({1, "hello", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto exporter = std::make_shared<opossum::Export>(table_wrapper, "not_existing_file.mp3");
  EXPECT_THROW(exporter->execute(), std::exception);
}

TEST_F(OperatorsExportTest, ReturnsInput) {
  auto table = load_table("resources/test_data/tbl/float.tbl");
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto exporter = std::make_shared<opossum::Export>(table_wrapper, test_filename + ".bin");
  exporter->execute();

  // need to load table again since it moved
  table = load_table("resources/test_data/tbl/float.tbl");

  EXPECT_TABLE_EQ_ORDERED(exporter->get_output(), table);
}

}  // namespace opossum
