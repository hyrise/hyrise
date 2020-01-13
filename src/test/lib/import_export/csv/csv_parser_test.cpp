#include "base_test.hpp"

#include "hyrise.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "storage/table.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

class CsvParserTest : public BaseTest {};

TEST_F(CsvParserTest, SingleFloatColumn) {
  auto table = CsvParser::parse("resources/test_data/csv/float.csv");
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/float.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, FloatIntTable) {
  auto table = CsvParser::parse("resources/test_data/csv/float_int.csv");
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/float_int.tbl", 2);
  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, StringNoQuotes) {
  auto table = CsvParser::parse("resources/test_data/csv/string.csv");
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/string.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, StringQuotes) {
  auto table = CsvParser::parse("resources/test_data/csv/string_quotes.csv");
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/string.tbl", 5);
  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, StringEscaping) {
  auto table = CsvParser::parse("resources/test_data/csv/string_escaped.csv");

  auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::String, false}}, TableType::Data, 5);
  expected_table->append({"aa\"\"aa"});
  expected_table->append({"xx\"x"});
  expected_table->append({"yy,y"});
  expected_table->append({"zz\nz"});

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, NoRows) {
  auto table = CsvParser::parse("resources/test_data/csv/float_int_empty.csv");
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/float_int_empty.tbl", 2);
  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, NoRowsNoColumns) {
  auto table = CsvParser::parse("resources/test_data/csv/no_columns.csv");
  std::shared_ptr<Table> expected_table = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data);
  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, TrailingNewline) {
  auto table = CsvParser::parse("resources/test_data/csv/float_int_trailing_newline.csv");
  std::shared_ptr<Table> expected_table = load_table("resources/test_data/tbl/float_int.tbl", 2);
  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, FileDoesNotExist) { EXPECT_THROW(CsvParser::parse("not_existing_file"), std::exception); }

TEST_F(CsvParserTest, EmptyStrings) {
  auto table = CsvParser::parse("resources/test_data/csv/empty_strings.csv");
  TableColumnDefinitions column_definitions{
      {"a", DataType::String, false}, {"b", DataType::String, false}, {"c", DataType::String, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
  for (int i = 0; i < 8; ++i) {
    expected_table->append({"", "", ""});
  }

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, SemicolonSeparator) {
  std::string csv_file = "resources/test_data/csv/ints_semicolon_separator.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.config.separator = ';';
  auto table = CsvParser::parse(csv_file, Chunk::DEFAULT_SIZE, csv_meta);

  TableColumnDefinitions column_definitions{
      {"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
  for (int i = 0; i < 8; ++i) {
    expected_table->append({1, 2, 3});
  }

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, ChunkSize) {
  auto table = CsvParser::parse("resources/test_data/csv/float_int_large.csv", ChunkOffset{20});

  // check if chunk_size property is correct
  EXPECT_EQ(table->max_chunk_size(), 20U);

  // check if actual chunk_size is correct
  EXPECT_EQ(table->get_chunk(ChunkID{0})->size(), 20U);
  EXPECT_EQ(table->get_chunk(ChunkID{1})->size(), 20U);
}

TEST_F(CsvParserTest, MaxChunkSize) {
  auto table = CsvParser::parse("resources/test_data/csv/float_int_large_chunksize_max.csv", Chunk::DEFAULT_SIZE);

  // check if chunk_size property is correct (maximum chunk size)
  EXPECT_EQ(table->max_chunk_size(), Chunk::DEFAULT_SIZE);

  // check if actual chunk_size and chunk_count is correct
  EXPECT_EQ(table->get_chunk(ChunkID{0})->size(), 100U);
  EXPECT_EQ(table->chunk_count(), ChunkID{1});

  TableColumnDefinitions column_definitions{{"b", DataType::Float, false}, {"a", DataType::Int, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 20);

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, StringEscapingNonRfc) {
  std::string csv_file = "resources/test_data/csv/string_escaped_unsafe.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.config.rfc_mode = false;
  auto table = CsvParser::parse(csv_file, Chunk::DEFAULT_SIZE, csv_meta);

  TableColumnDefinitions column_definitions{{"a", DataType::String, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);
  expected_table->append({"aa\"\"aa"});
  expected_table->append({"xx\"x"});
  expected_table->append({"yy,y"});
  expected_table->append({"zz\nz"});

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, ImportNumericNullValues) {
  auto table = CsvParser::parse("resources/test_data/csv/float_int_with_null.csv");

  TableColumnDefinitions column_definitions{
      {"a", DataType::Float, true}, {"b", DataType::Int, false}, {"c", DataType::Int, true}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 3);

  expected_table->append({458.7f, 12345, NULL_VALUE});
  expected_table->append({NULL_VALUE, 123, 456});
  expected_table->append({457.7f, 1234, 675});

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, ImportStringNullValues) {
  auto table = CsvParser::parse("resources/test_data/csv/string_with_null.csv");

  TableColumnDefinitions column_definitions{{"a", DataType::String, true}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

  expected_table->append({"xxx"});
  expected_table->append({"www"});
  expected_table->append({"null"});
  expected_table->append({"zzz"});
  expected_table->append({NULL_VALUE});

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, ImportUnquotedNullStringAsNull) {
  auto table = CsvParser::parse("resources/test_data/csv/null_literal.csv");

  TableColumnDefinitions column_definitions{{"a", DataType::Int, true}, {"b", DataType::String, true}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 3);

  expected_table->append({1, "Hello"});
  expected_table->append({NULL_VALUE, "World"});
  expected_table->append({3, NULL_VALUE});
  expected_table->append({4, NULL_VALUE});
  expected_table->append({5, NULL_VALUE});
  expected_table->append({6, "!"});

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, ImportUnquotedNullStringAsValue) {
  auto table = CsvParser::parse("resources/test_data/csv/null_literal_as_string.csv");

  TableColumnDefinitions column_definitions{{"a", DataType::Int, true}, {"b", DataType::String, true}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 3);

  expected_table->append({1, "Hello"});
  expected_table->append({NULL_VALUE, "World"});
  expected_table->append({3, "null"});
  expected_table->append({4, "Null"});
  expected_table->append({5, "NULL"});
  expected_table->append({6, "!"});

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, ImportUnquotedNullStringThrows) {
  EXPECT_THROW(CsvParser::parse("resources/test_data/csv/string_with_bad_null.csv"), std::exception);
}

TEST_F(CsvParserTest, WithAndWithoutQuotes) {
  std::string csv_file = "resources/test_data/csv/with_and_without_quotes.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.config.reject_quoted_nonstrings = false;
  auto table = CsvParser::parse(csv_file, Chunk::DEFAULT_SIZE, csv_meta);

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::String, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  column_definitions.emplace_back("c", DataType::Float, false);
  column_definitions.emplace_back("d", DataType::Double, false);
  column_definitions.emplace_back("e", DataType::String, false);
  column_definitions.emplace_back("f", DataType::Int, false);
  column_definitions.emplace_back("g", DataType::Float, false);
  column_definitions.emplace_back("h", DataType::Double, false);
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

  expected_table->append({"xxx", 23, 0.5f, 24.23, "xxx", 23, 0.5f, 24.23});
  expected_table->append({"yyy", 56, 7.4f, 2.123, "yyy", 23, 7.4f, 2.123});

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, StringDoubleEscape) {
  std::string csv_file = "resources/test_data/csv/string_double_escape.csv";
  auto csv_meta = process_csv_meta_file(csv_file + CsvMeta::META_FILE_EXTENSION);
  csv_meta.config.escape = '\\';
  auto table = CsvParser::parse(csv_file, Chunk::DEFAULT_SIZE, csv_meta);

  TableColumnDefinitions column_definitions{{"a", DataType::String, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 5);

  expected_table->append({"xxx\\\"xyz\\\""});

  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
}

TEST_F(CsvParserTest, ImportQuotedInt) {
  EXPECT_THROW(CsvParser::parse("resources/test_data/csv/quoted_int.csv"), std::exception);
}

TEST_F(CsvParserTest, UnconvertedCharactersThrows) {
  EXPECT_THROW(CsvParser::parse("resources/test_data/csv/unconverted_characters_int.csv"), std::logic_error);

  EXPECT_THROW(CsvParser::parse("resources/test_data/csv/unconverted_characters_float.csv"), std::logic_error);

  EXPECT_THROW(CsvParser::parse("resources/test_data/csv/unconverted_characters_double.csv"), std::logic_error);
}

TEST_F(CsvParserTest, EmptyTableFromMetaFile) {
  const auto csv_meta_table = CsvParser{}.create_table_from_meta_file("resources/test_data/csv/float_int.csv.json");
  const auto expected_table = std::make_shared<Table>(
      TableColumnDefinitions{{"b", DataType::Float, false}, {"a", DataType::Int, false}}, TableType::Data);

  EXPECT_EQ(csv_meta_table->row_count(), 0);
  EXPECT_TABLE_EQ_UNORDERED(csv_meta_table, expected_table);
}

TEST_F(CsvParserTest, WithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  auto scheduler = Hyrise::get().scheduler();
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto table = CsvParser::parse("resources/test_data/csv/float_int_large.csv");

  TableColumnDefinitions column_definitions{{"b", DataType::Float, false}, {"a", DataType::Int, false}};
  auto expected_table = std::make_shared<Table>(column_definitions, TableType::Data, 20);

  for (int i = 0; i < 100; ++i) {
    expected_table->append({458.7f, 12345});
  }

  Hyrise::get().scheduler()->finish();
  EXPECT_TABLE_EQ_ORDERED(table, expected_table);
  Hyrise::get().set_scheduler(scheduler);
}

}  // namespace opossum
