#include <memory>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "operators/import_csv.hpp"

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"

namespace opossum {

class OperatorsImportCsvTest : public BaseTest {};

TEST_F(OperatorsImportCsvTest, SingleFloatColumn) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, FloatIntTable) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float_int.tbl", 2);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, StringNoQuotes) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/string.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, StringQuotes) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_quotes.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/string.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, StringEscaping) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_escaped.csv");
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
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int_trailing_newline.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float_int.tbl", 2);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, FileDoesNotExist) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/not_existing_file.csv");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportCsvTest, SaveToStorageManager) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float.csv", std::string("float_table"));
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
  EXPECT_TABLE_EQ(StorageManager::get().get_table("float_table"), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, FallbackToRetrieveFromStorageManager) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float.csv", std::string("float_table"));
  importer->execute();
  auto retriever = std::make_shared<ImportCsv>("src/test/csv/float.csv", std::string("float_table"));
  retriever->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ(importer->get_output(), retriever->get_output(), true);
  EXPECT_TABLE_EQ(StorageManager::get().get_table("float_table"), retriever->get_output(), true);
}

TEST_F(OperatorsImportCsvTest, EmptyStrings) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/empty_strings.csv");
  importer->execute();

  auto expected_table = std::make_shared<Table>(5);
  expected_table->add_column("a", "string");
  expected_table->add_column("b", "string");
  expected_table->add_column("c", "string");
  for (int i = 0; i < 8; ++i) {
    expected_table->append({"", "", ""});
  }

  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, Parallel) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  auto importer = std::make_shared<OperatorTask>(std::make_shared<ImportCsv>("src/test/csv/float_int_large.csv"));
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

TEST_F(OperatorsImportCsvTest, SemicolonSeparator) {
  CsvConfig config;
  config.separator = ';';
  auto importer = std::make_shared<ImportCsv>("src/test/csv/ints_semicolon_separator.csv", config);
  importer->execute();

  auto expected_table = std::make_shared<Table>(5);
  expected_table->add_column("a", "int");
  expected_table->add_column("b", "int");
  expected_table->add_column("c", "int");
  for (int i = 0; i < 8; ++i) {
    expected_table->append({1, 2, 3});
  }

  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, ChunkSize) {
  // chunk_size is defined as "20" in .meta file
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int_large.csv");
  importer->execute();

  // check if chunk_size property is correct
  EXPECT_EQ(importer->get_output()->chunk_size(), 20U);

  // check if actual chunk_size is correct
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{0}).size(), 20U);
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{1}).size(), 20U);
}

TEST_F(OperatorsImportCsvTest, ChunkSizeZero) {
  // chunk_size is defined as "20" in .meta file
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int_large_chunksize_0.csv");
  importer->execute();

  // check if chunk_size property is correct (maximum chunk size, 0 for unlimited)
  EXPECT_EQ(importer->get_output()->chunk_size(), 0U);

  // check if actual chunk_size and chunk_count is correct
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{0}).size(), 100U);
  EXPECT_EQ(importer->get_output()->chunk_count(), ChunkID{1});

  auto expected_table = std::make_shared<Table>(20);
  expected_table->add_column("b", "float");
  expected_table->add_column("a", "int");

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

TEST_F(OperatorsImportCsvTest, StringEscapingNonRfc) {
  CsvConfig config;
  config.rfc_mode = false;
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_escaped_unsafe.csv", config);
  importer->execute();

  auto expected_table = std::make_shared<Table>(5);
  expected_table->add_column("a", "string");
  expected_table->append({"aa\"\"aa"});
  expected_table->append({"xx\"x"});
  expected_table->append({"yy,y"});
  expected_table->append({"zz\nz"});

  EXPECT_TABLE_EQ(importer->get_output(), expected_table, true);
}

}  // namespace opossum
