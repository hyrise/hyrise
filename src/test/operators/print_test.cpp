#include <memory>
#include <string>

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
