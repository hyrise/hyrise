#include "base_test.hpp" // NEEDEDINCLUDE

#include "import_export/csv_parser.hpp" // NEEDEDINCLUDE

namespace opossum {

class CsvParserTest : public BaseTest {};

TEST_F(CsvParserTest, EmptyTableFromMetaFile) {
  CsvParser parser;
  const auto csv_meta_table = parser.create_table_from_meta_file("resources/test_data/csv/float_int.csv.json");
  const auto expected_table =
      std::make_shared<Table>(TableColumnDefinitions{{"b", DataType::Float}, {"a", DataType::Int}}, TableType::Data);

  EXPECT_EQ(csv_meta_table->row_count(), 0);
  EXPECT_TABLE_EQ_UNORDERED(csv_meta_table, expected_table);
}

}  // namespace opossum
