#include <memory>
#include <string>
#include <vector>

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
  std::vector<uint16_t> test_column_string_widths(uint16_t min, uint16_t max) {
    return _column_string_widths(min, max, tab);
  }

  std::string test_truncate_cell(const AllTypeVariant& cell, uint16_t max_width) {
    return _truncate_cell(cell, max_width);
  }
};

TEST_F(OperatorsPrintTest, EmptyTable) {
  auto pr = std::make_shared<Print>(gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), t);

  auto output_str = output.str();

  // rather hard-coded tests
  EXPECT_TRUE(output_str.find("col_1") != std::string::npos);
  EXPECT_TRUE(output_str.find("col_2") != std::string::npos);
  EXPECT_TRUE(output_str.find("int") != std::string::npos);
  EXPECT_TRUE(output_str.find("string") != std::string::npos);
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

  auto output_str = output.str();

  EXPECT_TRUE(output_str.find("Chunk 0") != std::string::npos);
  // there should not be a third chunk (at least that's the current impl)
  EXPECT_TRUE(output_str.find("Chunk 3") == std::string::npos);

  // remove spaces
  output_str.erase(remove_if(output_str.begin(), output_str.end(), isspace), output_str.end());

  EXPECT_TRUE(output_str.find("|2|a|") != std::string::npos);
  EXPECT_TRUE(output_str.find("|9|b|") != std::string::npos);
  EXPECT_TRUE(output_str.find("|10|a|") == std::string::npos);

  // EXPECT_TRUE(output_str.find("Empty chunk.") != std::string::npos);
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
  auto tab = StorageManager::get().get_table(table_name);

  tab->append({0, "abcdefghijklmnopqrstuvwxyz"});

  auto wrap = std::make_shared<TableWrapper>(tab);
  wrap->execute();

  auto printer = std::make_shared<Print>(wrap, output);
  printer->execute();

  auto output_str = output.str();
  EXPECT_TRUE(output_str.find("|abcdefghijklmnopq...|") != std::string::npos);
}

}  // namespace opossum
