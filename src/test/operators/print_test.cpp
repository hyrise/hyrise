#include <memory>
#include <string>
#include <vector>
#include <algorithm>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorsPrintTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("col_1", DataType::Int);
    column_definitions.emplace_back("col_2", DataType::String);
    t = std::make_shared<Table>(column_definitions, TableType::Data, chunk_size);
    StorageManager::get().add_table(table_name, t);

    gt = std::make_shared<GetTable>(table_name);
    gt->execute();
  }

  std::ostringstream output;

  std::string table_name = "printTestTable";

  uint32_t chunk_size = 10;

  std::shared_ptr<GetTable>(gt);
  std::shared_ptr<Table> t = nullptr;
};

// class used to make protected methods visible without
// modifying the base class with testing code.
class PrintWrapper : public Print {
  std::shared_ptr<const Table> tab;

 public:
  explicit PrintWrapper(const std::shared_ptr<AbstractOperator> in) : Print(in), tab(in->get_output()) {}
  explicit PrintWrapper(const std::shared_ptr<AbstractOperator> in,
                        std::ostream& out, uint32_t flags)
      : Print(in, out, flags), tab(in->get_output()) {}

  std::vector<uint16_t> test_column_string_widths(uint16_t min, uint16_t max) {
    return _column_string_widths(min, max, tab);
  }

  std::string test_truncate_cell(const AllTypeVariant& cell, uint16_t max_width) {
    return _truncate_cell(cell, max_width);
  }

  uint16_t get_max_cell_width() {
    return _max_cell_width;
  }

  bool is_printing_empty_chunks() {
    return _flags & PrintIgnoreEmptyChunks;
  }

  bool is_printing_mvcc_information() {
    return _flags & PrintMvcc;
  }
};

TEST_F(OperatorsPrintTest, TableColumnDefinitions) {
  auto pr = std::make_shared<Print>(gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), t);

  auto output_string = output.str();

  // rather hard-coded tests
  EXPECT_TRUE(output_string.find("col_1") != std::string::npos);
  EXPECT_TRUE(output_string.find("col_2") != std::string::npos);
  EXPECT_TRUE(output_string.find("int") != std::string::npos);
  EXPECT_TRUE(output_string.find("string") != std::string::npos);
}

TEST_F(OperatorsPrintTest, FilledTable) {
  auto tab = StorageManager::get().get_table(table_name);
  for (size_t i = 0; i < chunk_size * 2; i++) {
    // char 97 is an 'a'
    tab->append({static_cast<int>(i % chunk_size), std::string(1, 97 + static_cast<int>(i / chunk_size))});
  }

  auto pr = std::make_shared<Print>(gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), tab);

  auto output_string = output.str();

  EXPECT_TRUE(output_string.find("Chunk 0") != std::string::npos);
  // there should not be a third chunk (at least that's the current impl)
  EXPECT_TRUE(output_string.find("Chunk 3") == std::string::npos);

  // remove spaces
  output_string.erase(remove_if(output_string.begin(), output_string.end(), isspace), output_string.end());

  EXPECT_TRUE(output_string.find("|2|a|") != std::string::npos);
  EXPECT_TRUE(output_string.find("|9|b|") != std::string::npos);
  EXPECT_TRUE(output_string.find("|10|a|") == std::string::npos);
}

TEST_F(OperatorsPrintTest, GetColumnWidths) {
  uint16_t min = 8;
  uint16_t max = 20;

  auto tab = StorageManager::get().get_table(table_name);

  auto pr_wrap = std::make_shared<PrintWrapper>(gt);
  auto print_lengths = pr_wrap->test_column_string_widths(min, max);

  // we have two columns, thus two 'lengths'
  ASSERT_EQ(print_lengths.size(), static_cast<size_t>(2));
  // with empty columns and short col names, we should see the minimal lengths
  EXPECT_EQ(print_lengths.at(0), static_cast<size_t>(min));
  EXPECT_EQ(print_lengths.at(1), static_cast<size_t>(min));

  int ten_digits_ints = 1234567890;

  tab->append({ten_digits_ints, "quite a long string with more than $max chars"});

  print_lengths = pr_wrap->test_column_string_widths(min, max);
  EXPECT_EQ(print_lengths.at(0), static_cast<size_t>(10));
  EXPECT_EQ(print_lengths.at(1), static_cast<size_t>(max));
}

TEST_F(OperatorsPrintTest, OperatorName) {
  auto pr = std::make_shared<opossum::Print>(gt, output);

  EXPECT_EQ(pr->name(), "Print");
}

TEST_F(OperatorsPrintTest, TruncateLongValue) {
  auto print_wrap = std::make_shared<PrintWrapper>(gt);

  auto cell = AllTypeVariant{"abcdefghijklmnopqrstuvwxyz"};

  auto truncated_cell_20 = print_wrap->test_truncate_cell(cell, 20);
  EXPECT_EQ(truncated_cell_20, "abcdefghijklmnopq...");

  auto truncated_cell_30 = print_wrap->test_truncate_cell(cell, 30);
  EXPECT_EQ(truncated_cell_30, "abcdefghijklmnopqrstuvwxyz");

  auto truncated_cell_10 = print_wrap->test_truncate_cell(cell, 10);
  EXPECT_EQ(truncated_cell_10, "abcdefg...");
}

TEST_F(OperatorsPrintTest, TruncateLongValueInOutput) {
  auto print_wrap = std::make_shared<PrintWrapper>(gt);
  auto tab = StorageManager::get().get_table(table_name);

  std::string cell_string = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
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

TEST_F(OperatorsPrintTest, EmptyChunkFlag) {
  auto print_wrap = PrintWrapper(gt, output, 1);
  print_wrap.execute();
  auto output_string = output.str();

  EXPECT_TRUE(print_wrap.is_printing_empty_chunks());
  EXPECT_FALSE(print_wrap.is_printing_mvcc_information());
  EXPECT_TRUE(output_string.find("MVCC") == std::string::npos);
}

TEST_F(OperatorsPrintTest, MVCCFlag) {
  auto print_wrap = PrintWrapper(gt, output, 2);
  print_wrap.execute();
  auto output_string = output.str();

  EXPECT_TRUE(output_string.find("MVCC") != std::string::npos);
  EXPECT_TRUE(output_string.find("_TID") != std::string::npos);
  EXPECT_TRUE(print_wrap.is_printing_mvcc_information());
  EXPECT_FALSE(print_wrap.is_printing_empty_chunks());
}

TEST_F(OperatorsPrintTest, AllFlags) {
  auto print_wrap = PrintWrapper(gt, output, 3);
  print_wrap.execute();

  EXPECT_TRUE(print_wrap.is_printing_empty_chunks());
  EXPECT_TRUE(print_wrap.is_printing_mvcc_information());
}

TEST_F(OperatorsPrintTest, MVCCTableLoad) {
  // per default, MVCC columns are created when loading tables
  std::shared_ptr<TableWrapper> table = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
  table->execute();

  Print::print(table, 2, output);
  auto output_string = output.str();

  // MVCC header
  EXPECT_TRUE(output_string.find("MVCC") != std::string::npos);
  EXPECT_TRUE(output_string.find("_TID") != std::string::npos);
  // chunk count of two (3 lines, chunk size 2)
  EXPECT_TRUE(output_string.find("Chunk 1") != std::string::npos);
  // MVCC tuple data (there is no '0' in the data loaded)
  EXPECT_TRUE(output_string.find("0|") != std::string::npos);
}

TEST_F(OperatorsPrintTest, DirectInstantiations) {
  Print::print(gt, 0, output);
  auto output_op_inst = output.str();
  EXPECT_TRUE(output_op_inst.find("col_1") != std::string::npos);

  Print::print(t, 0, output);
  auto output_tab_inst = output.str();
  EXPECT_TRUE(output_tab_inst.find("col_1") != std::string::npos);
}

}  // namespace opossum
