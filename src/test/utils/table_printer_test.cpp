#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/utils/table_printer.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class TablePrinterTest : public BaseTest {
 protected:
  void SetUp() override {
    _table = std::make_shared<Table>(Table(_chunk_size));
    _table->add_column("col_1", "int");
    _table->add_column("col_2", "string");
    StorageManager::get().add_table(_table_name, _table);

    _gt = std::make_shared<GetTable>(_table_name);
    _gt->execute();
  }

  std::ostringstream _output;

  std::string _table_name = "printTestTable";

  uint32_t _chunk_size = 10;

  std::shared_ptr<GetTable>(_gt);
  std::shared_ptr<Table> _table = nullptr;
};

// class used to make protected methods visible without
// modifying the base class with testing code.
class TablePrinterWrapper : public TablePrinter {
 public:
  explicit TablePrinterWrapper(std::shared_ptr<const Table> table) : TablePrinter(table) {}
  std::vector<uint16_t> test_column_string_widths(uint16_t min, uint16_t max) {
    return _column_string_widths(min, max);
  }
};

TEST_F(TablePrinterTest, EmptyTable) {
  TablePrinter printer(_table, _output);
  printer.print();

  auto output_str = _output.str();

  // rather hard-coded tests
  EXPECT_TRUE(output_str.find("col_1") != std::string::npos);
  EXPECT_TRUE(output_str.find("col_2") != std::string::npos);
  EXPECT_TRUE(output_str.find("int") != std::string::npos);
  EXPECT_TRUE(output_str.find("string") != std::string::npos);

  EXPECT_TRUE(output_str.find("Empty chunk.") != std::string::npos);
}

TEST_F(TablePrinterTest, FilledTable) {
  auto tab = StorageManager::get().get_table(_table_name);
  for (size_t i = 0; i < _chunk_size * 2; i++) {
    // char 97 is an 'a'
    tab->append({static_cast<int>(i % _chunk_size), std::string(1, 97 + static_cast<int>(i / _chunk_size))});
  }

  TablePrinter printer(_table, _output);
  printer.print();

  auto output_str = _output.str();

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

TEST_F(TablePrinterTest, GetColumnWidths) {
  uint16_t min = 8;
  uint16_t max = 20;

  auto tab = StorageManager::get().get_table(_table_name);

  auto pr_wrap = std::make_shared<TablePrinterWrapper>(_table);
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

}  // namespace opossum
