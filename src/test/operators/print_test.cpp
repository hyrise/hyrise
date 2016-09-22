#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

class PrintOperatorTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    t = std::make_shared<opossum::Table>(opossum::Table(chunk_size));
    t->add_column("col_1", "int");
    t->add_column("col_2", "string");
    opossum::StorageManager::get().add_table(table_name, t);

    gt = std::make_shared<opossum::GetTable>(table_name);
    gt->execute();
  }

  std::ostringstream output;

  std::string table_name = "printTestTable";

  uint32_t chunk_size = 10;

  std::shared_ptr<opossum::GetTable>(gt);
  std::shared_ptr<opossum::Table> t = nullptr;
};

// class used to make protected methods visible without
// modifying the base class with testing code.
class PrintWrapper : public opossum::Print {
 public:
  explicit PrintWrapper(const std::shared_ptr<AbstractOperator> in) : opossum::Print(in) {}
  std::vector<uint16_t> test_column_string_widths(uint16_t min, uint16_t max, std::shared_ptr<opossum::Table> t) {
    return column_string_widths(min, max, t);
  }
};

TEST_F(PrintOperatorTest, check_print_output_empty_table) {
  auto pr = std::make_shared<opossum::Print>(gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), t);

  auto output_str = output.str();

  // rather hard-coded tests
  EXPECT_TRUE(output_str.find("col_1") != std::string::npos);
  EXPECT_TRUE(output_str.find("col_2") != std::string::npos);
  EXPECT_TRUE(output_str.find("[int]") != std::string::npos);
  EXPECT_TRUE(output_str.find("[string]") != std::string::npos);

  EXPECT_TRUE(output_str.find("Empty chunk.") != std::string::npos);
}

TEST_F(PrintOperatorTest, check_print_output_filled_table) {
  auto tab = opossum::StorageManager::get().get_table(table_name);
  for (size_t i = 0; i < chunk_size * 2; i++) {
    // char 97 is an 'a'
    tab->append({static_cast<int>(i % chunk_size), std::string(1, 97 + static_cast<int>(i / chunk_size))});
  }

  auto pr = std::make_shared<opossum::Print>(gt, output);
  pr->execute();

  // check if table is correctly passed
  EXPECT_EQ(pr->get_output(), tab);

  auto output_str = output.str();

  EXPECT_TRUE(output_str.find("Chunk 0") != std::string::npos);
  // there should not be a third chunk (at least that's the current impl)
  EXPECT_TRUE(output_str.find("Chunk 3") == std::string::npos);

  // remov spaces
  output_str.erase(remove_if(output_str.begin(), output_str.end(), isspace), output_str.end());

  EXPECT_TRUE(output_str.find("|2|a|") != std::string::npos);
  EXPECT_TRUE(output_str.find("|9|b|") != std::string::npos);
  EXPECT_TRUE(output_str.find("|10|a|") == std::string::npos);

  // EXPECT_TRUE(output_str.find("Empty chunk.") != std::string::npos);
}

TEST_F(PrintOperatorTest, get_column_widths) {
  uint16_t min = 8;
  uint16_t max = 20;

  auto tab = opossum::StorageManager::get().get_table(table_name);

  auto pr_wrap = std::make_shared<PrintWrapper>(gt);
  auto print_lengths = pr_wrap->test_column_string_widths(min, max, tab);

  // we have two columns, thus two 'lengths'
  ASSERT_EQ(print_lengths.size(), static_cast<size_t>(2));
  // with empty columns and short col names, we should see the minimal lengths
  EXPECT_EQ(print_lengths.at(0), static_cast<size_t>(min));
  EXPECT_EQ(print_lengths.at(1), static_cast<size_t>(min));

  int ten_digits_ints = 1234567890;

  tab->append({ten_digits_ints, "quite a long string with more than $max chars"});

  print_lengths = pr_wrap->test_column_string_widths(min, max, tab);
  EXPECT_EQ(print_lengths.at(0), static_cast<size_t>(10));
  EXPECT_EQ(print_lengths.at(1), static_cast<size_t>(max));
}
