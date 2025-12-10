#include <memory>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "import_export/csv/csv_meta.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "storage/table_column_definition.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/check_table_equal.hpp"
#include "utils/load_table.hpp"

namespace hyrise {
class CsvOptionsTest : public BaseTest {};

TEST_F(CsvOptionsTest, ParseDelimier) {
  const auto columns = std::vector{ColumnMeta{.name = "Int1", .type = "int"}, ColumnMeta{.name = "Int2", .type = "int"},
                                   ColumnMeta{.name = "Int3", .type = "int"}};
  const auto csv_meta = CsvMeta{.config = ParseConfig{.separator = ';'}, .columns = columns};
  const auto expected_table = CsvParser::parse("resources/test_data/csv/ints_semicolon_separator.csv", csv_meta);

  auto table_column_definitions = TableColumnDefinitions(columns.size());
  std::ranges::transform(columns, table_column_definitions.begin(), [&](const auto& column) {
    return TableColumnDefinition(column.name, data_type_to_string.right.at(column.type), column.nullable);
  });
  Hyrise::get().storage_manager.add_table(
      "delimiter_table",
      std::make_shared<Table>(table_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes));

  const auto sql_statement = std::string{
      "COPY delimiter_table FROM 'resources/test_data/csv/ints_semicolon_delimiter.csv' WITH (FORMAT CSV, DELIMITER "
      "';')"};
  const auto [status, tables] = SQLPipelineBuilder(sql_statement).create_pipeline().get_result_tables();

  EXPECT_EQ(status, SQLPipelineStatus::Success);
  EXPECT_TABLE_EQ(Hyrise::get().storage_manager.get_table("delimiter_table"), expected_table, OrderSensitivity::Yes,
                  TypeCmpMode::Strict, FloatComparisonMode::AbsoluteDifference);
}

TEST_F(CsvOptionsTest, ParseQuote) {
  const auto columns =
      std::vector{ColumnMeta{.name = "Int", .type = "int"}, ColumnMeta{.name = "String", .type = "string"}};
  const auto csv_meta = CsvMeta{.config = ParseConfig{.quote = '"'}, .columns = columns};
  const auto expected_table = CsvParser::parse("resources/test_data/csv/int_string2.csv", csv_meta);

  auto table_column_definitions = TableColumnDefinitions(columns.size());
  std::ranges::transform(columns, table_column_definitions.begin(), [&](const auto& column) {
    return TableColumnDefinition(column.name, data_type_to_string.right.at(column.type), column.nullable);
  });
  Hyrise::get().storage_manager.add_table(
      "quote_table", std::make_shared<Table>(table_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes));

  const auto sql_statement =
      std::string{"COPY quote_table FROM 'resources/test_data/csv/int_string_quoted.csv' WITH (FORMAT CSV, QUOTE '`')"};
  const auto [status, tables] = SQLPipelineBuilder(sql_statement).create_pipeline().get_result_tables();

  EXPECT_EQ(status, SQLPipelineStatus::Success);
  EXPECT_TABLE_EQ(Hyrise::get().storage_manager.get_table("quote_table"), expected_table, OrderSensitivity::Yes,
                  TypeCmpMode::Strict, FloatComparisonMode::AbsoluteDifference);
}

TEST_F(CsvOptionsTest, ParseNull) {
  const auto columns = std::vector{ColumnMeta{.name = "Float", .type = "float", .nullable = true},
                                   ColumnMeta{.name = "Int1", .type = "int", .nullable = true},
                                   ColumnMeta{.name = "Int2", .type = "int", .nullable = true}};
  const auto csv_meta = CsvMeta{.config = ParseConfig{}, .columns = columns};
  const auto expected_table = CsvParser::parse("resources/test_data/csv/float_int_with_null.csv", csv_meta);

  auto table_column_definitions = TableColumnDefinitions(columns.size());
  std::ranges::transform(columns, table_column_definitions.begin(), [&](const auto& column) {
    return TableColumnDefinition(column.name, data_type_to_string.right.at(column.type), column.nullable);
  });
  Hyrise::get().storage_manager.add_table(
      "null_table", std::make_shared<Table>(table_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes));

  const auto sql_statement = std::string{
      "COPY null_table FROM 'resources/test_data/csv/float_int_with_special_null.csv' WITH (FORMAT CSV, NULL 'EMPTY')"};
  const auto [status, tables] = SQLPipelineBuilder(sql_statement).create_pipeline().get_result_tables();

  EXPECT_EQ(status, SQLPipelineStatus::Success);
  EXPECT_TABLE_EQ(Hyrise::get().storage_manager.get_table("null_table"), expected_table, OrderSensitivity::Yes,
                  TypeCmpMode::Strict, FloatComparisonMode::AbsoluteDifference);
}

}  // namespace hyrise
