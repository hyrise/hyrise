#include <memory>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/import_csv.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class OperatorsImportCsvTest : public BaseTest {};

TEST_F(OperatorsImportCsvTest, SingleFloatColumn) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv", "float");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, FloatIntTable) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv", "float_int");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float_int.tbl", 2);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, StringNoQuotes) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv", "string");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/string.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, StringQuotes) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv", "string_quotes");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/string.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, StringEscaping) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv", "string_escaped");
  importer->execute();

  auto expected_table = std::make_shared<Table>(5);
  expected_table->add_column("a", "string");
  expected_table->append({"aa\"\"aa"});
  expected_table->append({"xx\"x"});
  expected_table->append({"yy,y"});
  expected_table->append({"zz\nz"});

  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, TrailingNewline) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv", "float_int_trailing_newline");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float_int.tbl", 2);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, FileDoesNotExist) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv", "not_existing_file");
  EXPECT_THROW(importer->execute(), std::exception);
}

}  // namespace opossum
