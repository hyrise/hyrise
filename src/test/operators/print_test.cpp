#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorsPrintTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, true);
    column_definitions.emplace_back("column_2", DataType::String, false);
    _t = std::make_shared<Table>(column_definitions, TableType::Data, _chunk_size);
    Hyrise::get().storage_manager.add_table(_table_name, _t);

    _gt = std::make_shared<GetTable>(_table_name);
    _gt->execute();
  }

  std::ostringstream output;

  std::shared_ptr<Table> _t;
  std::shared_ptr<GetTable> _gt;

  const std::string _table_name = "printTestTable";
  const uint32_t _chunk_size = 10;
};

// class used to make protected methods visible without
// modifying the base class with testing code.
class PrintWrapper : public Print {
  std::shared_ptr<const Table> tab;

 public:
  explicit PrintWrapper(const std::shared_ptr<AbstractOperator> in) : Print(in), tab(in->get_output()) {}
  explicit PrintWrapper(const std::shared_ptr<AbstractOperator> in, std::ostream& out,
                        PrintFlags flags = PrintFlags::None)
      : Print(in, out, flags), tab(in->get_output()) {}

  std::vector<uint16_t> test_column_string_widths(uint16_t min, uint16_t max) {
    return _column_string_widths(min, max, tab);
  }

  std::string test_truncate_cell(const AllTypeVariant& cell, uint16_t max_width) {
    return _truncate_cell(cell, max_width);
  }

  uint16_t get_max_cell_width() { return _max_cell_width; }

  bool is_printing_mvcc_information() {
    return static_cast<uint32_t>(_flags) & static_cast<uint32_t>(PrintFlags::Mvcc);
  }
};

TEST_F(OperatorsPrintTest, TableColumnDefinitions) {
  auto pr = std::make_shared<Print>(_gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), _t);

  auto output_string = output.str();

  // rather hard-coded tests
  EXPECT_TRUE(output_string.find("column_1") != std::string::npos);
  EXPECT_TRUE(output_string.find("column_2") != std::string::npos);
  EXPECT_TRUE(output_string.find("int") != std::string::npos);
  EXPECT_TRUE(output_string.find("string") != std::string::npos);
}

TEST_F(OperatorsPrintTest, FilledTable) {
  const size_t chunk_count = 117;
  auto tab = Hyrise::get().storage_manager.get_table(_table_name);
  for (size_t i = 0; i < _chunk_size * chunk_count; i++) {
    // char 97 is an 'a'. Modulo 26 to stay within the alphabet.
    tab->append({static_cast<int>(i % _chunk_size), pmr_string(1, 97 + static_cast<int>(i / _chunk_size) % 26)});
  }

  auto pr = std::make_shared<Print>(_gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), tab);

  auto output_string = output.str();

  // check the line count of the output string
  size_t line_count = std::count(output_string.begin(), output_string.end(), '\n');
  size_t expected_line_count = 4 + 12 * chunk_count;  // 4 header lines + all 10-line chunks with chunk header
  EXPECT_EQ(line_count, expected_line_count);

  EXPECT_TRUE(output_string.find("Chunk 0") != std::string::npos);
  auto non_existing_chunk_header = std::string("Chunk ").append(std::to_string(chunk_count));
  EXPECT_TRUE(output_string.find(non_existing_chunk_header) == std::string::npos);

  // remove spaces for some simple tests
  output_string.erase(remove_if(output_string.begin(), output_string.end(), isspace), output_string.end());
  EXPECT_TRUE(output_string.find("|9|b|") != std::string::npos);
  EXPECT_TRUE(output_string.find("|7|z|") != std::string::npos);
  EXPECT_TRUE(output_string.find("|10|a|") == std::string::npos);
}

TEST_F(OperatorsPrintTest, GetColumnWidths) {
  uint16_t min = 8;
  uint16_t max = 20;

  auto tab = Hyrise::get().storage_manager.get_table(_table_name);

  auto pr_wrap = std::make_shared<PrintWrapper>(_gt);
  auto print_lengths = pr_wrap->test_column_string_widths(min, max);

  // we have two columns, thus two 'lengths'
  ASSERT_EQ(print_lengths.size(), static_cast<size_t>(2));
  // with empty columns and short column names, we should see the minimal lengths
  EXPECT_EQ(print_lengths.at(0), static_cast<size_t>(min));
  EXPECT_EQ(print_lengths.at(1), static_cast<size_t>(min));

  int ten_digits_ints = 1234567890;

  tab->append({ten_digits_ints, "quite a long string with more than $max chars"});

  print_lengths = pr_wrap->test_column_string_widths(min, max);
  EXPECT_EQ(print_lengths.at(0), static_cast<size_t>(10));
  EXPECT_EQ(print_lengths.at(1), static_cast<size_t>(max));
}

TEST_F(OperatorsPrintTest, OperatorName) {
  auto pr = std::make_shared<opossum::Print>(_gt, output);

  EXPECT_EQ(pr->name(), "Print");
}

TEST_F(OperatorsPrintTest, TruncateLongValue) {
  auto print_wrap = std::make_shared<PrintWrapper>(_gt);

  auto cell = AllTypeVariant{"abcdefghijklmnopqrstuvwxyz"};

  auto truncated_cell_20 = print_wrap->test_truncate_cell(cell, 20);
  EXPECT_EQ(truncated_cell_20, "abcdefghijklmnopq...");

  auto truncated_cell_30 = print_wrap->test_truncate_cell(cell, 30);
  EXPECT_EQ(truncated_cell_30, "abcdefghijklmnopqrstuvwxyz");

  auto truncated_cell_10 = print_wrap->test_truncate_cell(cell, 10);
  EXPECT_EQ(truncated_cell_10, "abcdefg...");
}

TEST_F(OperatorsPrintTest, TruncateLongValueInOutput) {
  auto print_wrap = std::make_shared<PrintWrapper>(_gt);
  auto tab = Hyrise::get().storage_manager.get_table(_table_name);

  pmr_string cell_string = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
  auto input = AllTypeVariant{cell_string};

  tab->append({0, input});

  auto substr_length = std::min(static_cast<int>(cell_string.length()), print_wrap->get_max_cell_width() - 3);

  std::string manual_substring = "|";
  for (uint16_t i = 0; i < substr_length; i++) {
    manual_substring += cell_string.at(i);
  }
  manual_substring += "...|";

  auto wrap = std::make_shared<TableWrapper>(tab);
  wrap->execute();

  auto printer = std::make_shared<Print>(wrap, output);
  printer->execute();

  auto output_string = output.str();
  EXPECT_TRUE(output_string.find(manual_substring) != std::string::npos);
}

TEST_F(OperatorsPrintTest, MVCCFlag) {
  auto print_wrap = PrintWrapper(_gt, output, PrintFlags::Mvcc);
  print_wrap.execute();

  auto expected_output =
      "=== Columns\n"
      "|column_1|column_2||        MVCC        |\n"
      "|     int|  string||_BEGIN|_END  |_TID  |\n"
      "|    null|not null||      |      |      |\n";

  EXPECT_EQ(output.str(), expected_output);
  EXPECT_TRUE(print_wrap.is_printing_mvcc_information());
}

TEST_F(OperatorsPrintTest, MVCCTableLoad) {
  // Per default, MVCC data is created when loading tables.
  // This test passes the flag for printing MVCC information, which is not printed by default.
  std::shared_ptr<TableWrapper> table =
      std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
  table->execute();

  Print::print(table, PrintFlags::Mvcc, output);

  auto expected_output =
      "=== Columns\n"
      "|       a|       b||        MVCC        |\n"
      "|     int|   float||_BEGIN|_END  |_TID  |\n"
      "|not null|not null||      |      |      |\n"
      "=== Chunk 0 ===\n"
      "|<ValueS>|<ValueS>||\n"
      "|   12345|   458.7||     0|      |      |\n"
      "|     123|   456.7||     0|      |      |\n"
      "=== Chunk 1 ===\n"
      "|<ValueS>|<ValueS>||\n"
      "|    1234|   457.7||     0|      |      |\n";
  EXPECT_EQ(output.str(), expected_output);
}

TEST_F(OperatorsPrintTest, PrintFlagsIgnoreChunkBoundaries) {
  std::shared_ptr<TableWrapper> table =
      std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
  table->execute();

  Print::print(table, PrintFlags::IgnoreChunkBoundaries, output);

  auto expected_output =
      "=== Columns\n"
      "|       a|       b|\n"
      "|     int|   float|\n"
      "|not null|not null|\n"
      "|   12345|   458.7|\n"
      "|     123|   456.7|\n"
      "|    1234|   457.7|\n";
  EXPECT_EQ(output.str(), expected_output);
}

TEST_F(OperatorsPrintTest, DirectInstantiations) {
  // We expect the same output from both instantiations.
  auto expected_output =
      "=== Columns\n"
      "|column_1|column_2|\n"
      "|     int|  string|\n"
      "|    null|not null|\n";

  std::ostringstream output_ss_op_inst;
  Print::print(_gt, PrintFlags::None, output_ss_op_inst);
  EXPECT_EQ(output_ss_op_inst.str(), expected_output);

  std::ostringstream output_ss_tab_inst;
  Print::print(_t, PrintFlags::None, output_ss_tab_inst);
  EXPECT_EQ(output_ss_tab_inst.str(), expected_output);
}

TEST_F(OperatorsPrintTest, NullableColumnPrinting) {
  TableColumnDefinitions nullable_column_definitions;
  nullable_column_definitions.emplace_back("l_returnflag", DataType::String, false);
  nullable_column_definitions.emplace_back("l_linestatus", DataType::String, false);
  nullable_column_definitions.emplace_back("sum_qty", DataType::Double, true);
  nullable_column_definitions.emplace_back("sum_base_price", DataType::Double, true);
  const auto tab = std::make_shared<Table>(nullable_column_definitions, TableType::Data, _chunk_size);

  auto expected_output =
      "=== Columns\n"
      "|l_returnflag|l_linestatus| sum_qty|sum_base_price|\n"
      "|      string|      string|  double|        double|\n"
      "|    not null|    not null|    null|          null|\n";

  Print::print(tab, PrintFlags::None, output);

  EXPECT_EQ(output.str(), expected_output);
}

TEST_F(OperatorsPrintTest, SegmentType) {
  auto table = load_table("resources/test_data/tbl/int_float.tbl", 1);

  ChunkEncoder::encode_chunks(table, {ChunkID{0}}, EncodingType::Dictionary);
  ChunkEncoder::encode_chunks(table, {ChunkID{1}}, EncodingType::RunLength);

  Print::print(table, PrintFlags::None, output);

  auto expected_output =
      "=== Columns\n"
      "|       a|       b|\n"
      "|     int|   float|\n"
      "|not null|not null|\n"
      "=== Chunk 0 ===\n"
      "|<Dic:1B>|<Dic:1B>|\n"
      "|   12345|   458.7|\n"
      "=== Chunk 1 ===\n"
      "|<RLE>   |<RLE>   |\n"
      "|     123|   456.7|\n"
      "=== Chunk 2 ===\n"
      "|<ValueS>|<ValueS>|\n"
      "|    1234|   457.7|\n";
  EXPECT_EQ(output.str(), expected_output);
}

TEST_F(OperatorsPrintTest, EmptyTable) {
  auto tab = Hyrise::get().storage_manager.get_table(_table_name);
  Segments empty_segments;
  auto column_1 = std::make_shared<opossum::ValueSegment<int>>();
  auto column_2 = std::make_shared<opossum::ValueSegment<pmr_string>>();
  empty_segments.push_back(column_1);
  empty_segments.push_back(column_2);
  tab->append_chunk(empty_segments);

  auto wrap = std::make_shared<TableWrapper>(tab);
  wrap->execute();

  std::ostringstream output;
  auto wrapper = PrintWrapper(wrap, output);
  wrapper.execute();

  auto expected_output =
      "=== Columns\n"
      "|column_1|column_2|\n"
      "|     int|  string|\n"
      "|    null|not null|\n"
      "=== Chunk 0 ===\n"
      "Empty chunk.\n";

  EXPECT_EQ(output.str(), expected_output);
  EXPECT_FALSE(wrapper.is_printing_mvcc_information());
}

}  // namespace opossum
