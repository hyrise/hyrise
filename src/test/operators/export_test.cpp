#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "import_export/csv/csv_meta.hpp"
#include "operators/export.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

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

  bool compare_file(const std::string& filename, const std::string& expected_content) {
    std::ifstream file(filename);
    Assert(file.is_open(), "compare_file: Could not find file " + filename);

    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

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
  const std::string test_filename = test_data_path + "export_test.csv";
  const std::string test_meta_filename = test_filename + CsvMeta::META_FILE_EXTENSION;
};

TEST_F(OperatorsExportTest, NonsensePath) {
  table->append({1, "hello", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::Export>(table_wrapper, "this/path/does/not/exist");
  EXPECT_THROW(ex->execute(), std::exception);
}

TEST_F(OperatorsExportTest, EmptyPath) {
  table->append({1, "hello", 3.5f});
  auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  auto ex = std::make_shared<opossum::Export>(table_wrapper, "");
  EXPECT_THROW(ex->execute(), std::exception);
}

}  // namespace opossum
