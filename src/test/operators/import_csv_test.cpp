#include <memory>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/import_csv.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

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
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, FloatIntTable) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float_int.tbl", 2);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, StringNoQuotes) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/string.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, StringQuotes) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_quotes.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/string.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, StringEscaping) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_escaped.csv");
  importer->execute();

  auto expected_table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::String}}, TableType::Data, 5);
  expected_table->append({"aa\"\"aa"});
  expected_table->append({"xx\"x"});
  expected_table->append({"yy,y"});
  expected_table->append({"zz\nz"});

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, TrailingNewline) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int_trailing_newline.csv");
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float_int.tbl", 2);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, FileDoesNotExist) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/not_existing_file.csv");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportCsvTest, SaveToStorageManager) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float.csv", std::string("float_table"));
  importer->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
  EXPECT_TABLE_EQ_ORDERED(StorageManager::get().get_table("float_table"), expected_table);
}

TEST_F(OperatorsImportCsvTest, FallbackToRetrieveFromStorageManager) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float.csv", std::string("float_table"));
  importer->execute();
  auto retriever = std::make_shared<ImportCsv>("src/test/csv/float.csv", std::string("float_table"));
  retriever->execute();
  std::shared_ptr<Table> expected_table = load_table("src/test/tables/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), retriever->get_output());
  EXPECT_TABLE_EQ_ORDERED(StorageManager::get().get_table("float_table"), retriever->get_output());
}

TEST_F(OperatorsImportCsvTest, EmptyStrings) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/empty_strings.csv");
  importer->execute();

  TableColumnDefinitions column_definitions{{"a", DataType::String}, {"b", DataType::String}, {"c", DataType::String}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
  for (int i = 0; i < 8; ++i) {
    expected_table->append({"", "", ""});
  }

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, Parallel) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
  auto importer = std::make_shared<OperatorTask>(std::make_shared<ImportCsv>("src/test/csv/float_int_large.csv"));
  importer->schedule();

  TableColumnDefinitions column_definitions{{"b", DataType::Float}, {"a", DataType::Int}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 20);

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  CurrentScheduler::get()->finish();
  EXPECT_TABLE_EQ_ORDERED(importer->get_operator()->get_output(), expected_table);
  CurrentScheduler::set(nullptr);
}

TEST_F(OperatorsImportCsvTest, SemicolonSeparator) {
  std::string csv_file = "src/test/csv/ints_semicolon_separator.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.config.separator = ';';
  auto importer = std::make_shared<ImportCsv>(csv_file, csv_meta);
  importer->execute();

  TableColumnDefinitions column_definitions{{"a", DataType::Int}, {"b", DataType::Int}, {"c", DataType::Int}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
  for (int i = 0; i < 8; ++i) {
    expected_table->append({1, 2, 3});
  }

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, ChunkSize) {
  // chunk_size is defined as "20" in meta file
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int_large.csv");
  importer->execute();

  // check if chunk_size property is correct
  EXPECT_EQ(importer->get_output()->max_chunk_size(), 20U);

  // check if actual chunk_size is correct
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{0})->size(), 20U);
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{1})->size(), 20U);
}

TEST_F(OperatorsImportCsvTest, ChunkSizeZero) {
  // chunk_size is not defined in the meta file, resulting in the maximum allowed chunk size
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int_large_chunksize_max.csv");
  importer->execute();

  // check if chunk_size property is correct (maximum chunk size)
  EXPECT_EQ(importer->get_output()->max_chunk_size(), Chunk::MAX_SIZE);

  // check if actual chunk_size and chunk_count is correct
  EXPECT_EQ(importer->get_output()->get_chunk(ChunkID{0})->size(), 100U);
  EXPECT_EQ(importer->get_output()->chunk_count(), ChunkID{1});

  TableColumnDefinitions column_definitions{{"b", DataType::Float}, {"a", DataType::Int}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 20);

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, StringEscapingNonRfc) {
  std::string csv_file = "src/test/csv/string_escaped_unsafe.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.config.rfc_mode = false;
  auto importer = std::make_shared<ImportCsv>(csv_file, csv_meta);
  importer->execute();

  TableColumnDefinitions column_definitions{{"a", DataType::String}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
  expected_table->append({"aa\"\"aa"});
  expected_table->append({"xx\"x"});
  expected_table->append({"yy,y"});
  expected_table->append({"zz\nz"});

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, ImportNumericNullValues) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/float_int_with_null.csv");
  importer->execute();

  TableColumnDefinitions column_definitions{
      {"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Int, true}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 3);

  expected_table->append({458.7f, 12345, NULL_VALUE});
  expected_table->append({NULL_VALUE, 123, 456});
  expected_table->append({457.7f, 1234, 675});

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, ImportStringNullValues) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_with_null.csv");
  importer->execute();

  TableColumnDefinitions column_definitions{{"a", DataType::String, true}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

  expected_table->append({"xxx"});
  expected_table->append({"www"});
  expected_table->append({"null"});
  expected_table->append({"zzz"});
  expected_table->append({NULL_VALUE});

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, ImportUnquotedNullString) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/string_with_bad_null.csv");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportCsvTest, WithAndWithoutQuotes) {
  std::string csv_file = "src/test/csv/with_and_without_quotes.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.config.reject_quoted_nonstrings = false;
  auto importer = std::make_shared<ImportCsv>(csv_file, csv_meta);
  importer->execute();

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String);
  column_definitions.emplace_back("b", DataType::Int);
  column_definitions.emplace_back("c", DataType::Float);
  column_definitions.emplace_back("d", DataType::Double);
  column_definitions.emplace_back("e", DataType::String);
  column_definitions.emplace_back("f", DataType::Int);
  column_definitions.emplace_back("g", DataType::Float);
  column_definitions.emplace_back("h", DataType::Double);
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

  expected_table->append({"xxx", 23, 0.5, 24.23, "xxx", 23, 0.5, 24.23});
  expected_table->append({"yyy", 56, 7.4, 2.123, "yyy", 23, 7.4, 2.123});

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, StringDoubleEscape) {
  std::string csv_file = "src/test/csv/string_double_escape.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.config.escape = '\\';
  auto importer = std::make_shared<ImportCsv>(csv_file, csv_meta);
  importer->execute();

  TableColumnDefinitions column_definitions{{"a", DataType::String}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

  expected_table->append({"xxx\\\"xyz\\\""});

  EXPECT_TABLE_EQ_ORDERED(importer->get_output(), expected_table);
}

TEST_F(OperatorsImportCsvTest, ImportQuotedInt) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/quoted_int.csv");
  EXPECT_THROW(importer->execute(), std::exception);
}

TEST_F(OperatorsImportCsvTest, AutoCompressChunks) {
  std::string csv_file = "src/test/csv/float_int_large.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.auto_compress = true;
  auto importer = std::make_shared<ImportCsv>(csv_file, csv_meta);
  importer->execute();

  TableColumnDefinitions column_definitions{{"b", DataType::Float}, {"a", DataType::Int}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 20);

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  auto result_table = importer->get_output();

  // Check if table content is preserved
  EXPECT_TABLE_EQ_ORDERED(result_table, expected_table);

  // Check if columns are compressed into DictionaryColumns
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < result_table->chunk_count(); ++chunk_id) {
    auto chunk = result_table->get_chunk(chunk_id);
    for (ColumnID column_id = ColumnID{0}; column_id < chunk->column_count(); ++column_id) {
      auto base_column = chunk->get_column(column_id);
      auto dict_column = std::dynamic_pointer_cast<const BaseDictionaryColumn>(base_column);

      EXPECT_TRUE(dict_column != nullptr);
    }
  }
}

TEST_F(OperatorsImportCsvTest, UnconvertedCharactersThrows) {
  auto importer = std::make_shared<ImportCsv>("src/test/csv/unconverted_characters_int.csv");
  EXPECT_THROW(importer->execute(), std::logic_error);

  importer = std::make_shared<ImportCsv>("src/test/csv/unconverted_characters_float.csv");
  EXPECT_THROW(importer->execute(), std::logic_error);

  importer = std::make_shared<ImportCsv>("src/test/csv/unconverted_characters_double.csv");
  EXPECT_THROW(importer->execute(), std::logic_error);
}

}  // namespace opossum
