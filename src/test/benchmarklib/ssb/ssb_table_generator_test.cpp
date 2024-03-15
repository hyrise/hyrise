#include "base_test.hpp"
#include "hyrise.hpp"
#include "ssb/ssb_table_generator.hpp"

namespace hyrise {

class SSBTableGeneratorTest : public BaseTest {
 protected:
  const std::string _dbgen_path{test_executable_path + "third_party/ssb-dbgen"};
  const std::string _csv_meta_path{"resources/benchmark/ssb/schema"};
};

TEST_F(SSBTableGeneratorTest, GenerateAndStoreRowCounts) {
  /**
   * Check whether all SSB tables are created by the SSBTableGenerator and added to the StorageManager. Then, check 
   * whether the row count is correct for all tables.
   */
  const auto scale_factor = 1.0f;
  const auto expected_sizes =
      std::map<std::string, uint64_t>{{"part", std::floor(200'000 * (1 + std::log2(scale_factor)))},
                                      {"supplier", std::floor(2'000 * scale_factor)},
                                      {"customer", std::floor(30'000 * scale_factor)},
                                      {"date", 2556}};

  std::filesystem::create_directories(test_data_path + "/ssb/sf-1");
  const auto data_path = std::filesystem::canonical(test_data_path + "/ssb/sf-1");

  EXPECT_EQ(Hyrise::get().storage_manager.tables().size(), 0);

  SSBTableGenerator(_dbgen_path, _csv_meta_path, data_path, scale_factor).generate_and_store();

  for (const auto& [name, size] : expected_sizes) {
    SCOPED_TRACE("checking table " + name);
    EXPECT_EQ(Hyrise::get().storage_manager.get_table(name)->row_count(), size);
  }

  const auto lineorder_cardinality =
      static_cast<float>(Hyrise::get().storage_manager.get_table("lineorder")->row_count());
  const auto expected_cardinality = 6'000'000 * scale_factor;
  const auto epsilon = 0.001;
  EXPECT_NEAR(lineorder_cardinality, expected_cardinality, expected_cardinality * epsilon);
}

TEST_F(SSBTableGeneratorTest, TableConstraints) {
  // We do not check the constraints in detail, just verify each table has a PK and the number of FKs as defined in the
  // specification.
  std::filesystem::create_directories(test_data_path + "/ssb/sf-0.01");
  const auto data_path = std::filesystem::canonical(test_data_path + "/ssb/sf-0.01");

  SSBTableGenerator(_dbgen_path, _csv_meta_path, data_path, 0.01f).generate_and_store();

  const auto& lineorder_table = Hyrise::get().storage_manager.get_table("lineorder");
  const auto& part_table = Hyrise::get().storage_manager.get_table("part");
  const auto& supplier_table = Hyrise::get().storage_manager.get_table("supplier");
  const auto& customer_table = Hyrise::get().storage_manager.get_table("customer");
  const auto& date_table = Hyrise::get().storage_manager.get_table("date");

  EXPECT_EQ(lineorder_table->soft_key_constraints().size(), 1);
  EXPECT_EQ(lineorder_table->soft_foreign_key_constraints().size(), 5);

  EXPECT_EQ(part_table->soft_key_constraints().size(), 1);
  EXPECT_TRUE(part_table->soft_foreign_key_constraints().empty());

  EXPECT_EQ(supplier_table->soft_key_constraints().size(), 1);
  EXPECT_TRUE(supplier_table->soft_foreign_key_constraints().empty());

  EXPECT_EQ(customer_table->soft_key_constraints().size(), 1);
  EXPECT_TRUE(customer_table->soft_foreign_key_constraints().empty());

  EXPECT_EQ(date_table->soft_key_constraints().size(), 1);
  EXPECT_TRUE(date_table->soft_foreign_key_constraints().empty());
}

}  // namespace hyrise
