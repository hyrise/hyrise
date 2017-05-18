#include <memory>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/storage/table.hpp"
#include "operators/import_csv.hpp"

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"

namespace opossum {

class OperatorsImportCsvNonRfcTest : public BaseTest {};

TEST_F(OperatorsImportCsvNonRfcTest, SingleFloatColumn) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float.csv", nullopt, false, 100);
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvNonRfcTest, FloatIntTable) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int.csv", nullopt, false, 100);
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float_int.tbl", 2);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvNonRfcTest, StringNoQuotes) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string.csv", nullopt, false, 100);
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/string.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvNonRfcTest, StringQuotes) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_quotes.csv", nullopt, false, 100);
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/string.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvNonRfcTest, StringEscaping) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_escaped_unsafe.csv", nullopt, false, 100);
  importer->execute();

  auto expected_table = std::make_shared<Table>(5);
  expected_table->add_column("a", "string");
  expected_table->append({"aa\"\"aa"});
  expected_table->append({"xx\"x"});
  expected_table->append({"yy,y"});
  expected_table->append({"zz\nz"});

  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvNonRfcTest, TrailingNewline) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int_trailing_newline.csv", nullopt, false, 100);
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float_int.tbl", 2);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvNonRfcTest, FileDoesNotExist) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/not_existing_file.csv", nullopt, false, 100);
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportCsvNonRfcTest, Parallel) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  auto importer = std::make_shared<OperatorTask>(
      std::make_shared<ImportCsv>("src/test/csv/float_int_large.csv", nullopt, false, 500));
  importer->schedule();

  auto expected_table = std::make_shared<Table>(20);
  expected_table->add_column("b", "float");
  expected_table->add_column("a", "int");

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  CurrentScheduler::get()->finish();
  EXPECT_TABLE_EQ(importer->get_operator()->get_output(), expected_table, true);
  CurrentScheduler::set(nullptr);
}

}  // namespace opossum
