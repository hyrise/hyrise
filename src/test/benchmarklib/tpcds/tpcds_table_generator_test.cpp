#include "base_test.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_parser.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "utils/load_table.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

namespace {

std::shared_ptr<Table> load_csv(const std::string& file_name) {
  return CsvParser::parse(
      "resources/test_data/csv/tpcds/" + file_name,
      process_csv_meta_file("resources/benchmark/tpcds/tables/" + file_name + CsvMeta::META_FILE_EXTENSION),
      Chunk::DEFAULT_SIZE);
}

}  // namespace

namespace hyrise {

class TPCDSTableGeneratorTest : public BaseTest {};

TEST_F(TPCDSTableGeneratorTest, TableContentsFirstRows) {
  /**
   * Check whether the data that TPCDSTableGenerator generates is the exact same that dsdgen generates. Since dsdgen
   * does not support very small scale factors, only generate and check first rows for each table.
   */
  const auto rows_to_check = ds_key_t{50};

  // Initialize with different params to check whether global state is correctly reset.
  TPCDSTableGenerator(10, ChunkOffset{2}, 42);

  // Run generation twice to make sure no global state (of which tpcds_dbgen has plenty :( ) from the first generation
  // process carried over into the second.
  for (auto i = 1; i <= 2; i++) {
    SCOPED_TRACE("TableContentsFirstRows iteration " + std::to_string(i));
    const auto table_generator = TPCDSTableGenerator(1, Chunk::DEFAULT_SIZE, 305);  // seed 305 includes Mrs. Null
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_call_center(rows_to_check), load_csv("call_center.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_catalog_page(rows_to_check), load_csv("catalog_page.csv"));
    const auto [catalog_sales_table, catalog_returns_table] =
        table_generator.generate_catalog_sales_and_returns(rows_to_check);
    EXPECT_TABLE_EQ_ORDERED(catalog_sales_table, load_csv("catalog_sales.csv"));
    EXPECT_TABLE_EQ_ORDERED(catalog_returns_table, load_csv("catalog_returns.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_customer_address(rows_to_check), load_csv("customer_address.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_customer(rows_to_check), load_csv("customer.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_customer_demographics(rows_to_check),
                            load_csv("customer_demographics.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_date_dim(rows_to_check), load_csv("date_dim.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_household_demographics(rows_to_check),
                            load_csv("household_demographics.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_income_band(rows_to_check), load_csv("income_band.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_inventory(rows_to_check), load_csv("inventory.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_item(rows_to_check), load_csv("item.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_promotion(rows_to_check), load_csv("promotion.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_reason(rows_to_check), load_csv("reason.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_ship_mode(rows_to_check), load_csv("ship_mode.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_store(rows_to_check), load_csv("store.csv"));
    const auto [store_sales_table, store_returns_table] =
        table_generator.generate_store_sales_and_returns(rows_to_check);
    EXPECT_TABLE_EQ_ORDERED(store_sales_table, load_csv("store_sales.csv"));
    EXPECT_TABLE_EQ_ORDERED(store_returns_table, load_csv("store_returns.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_time_dim(rows_to_check), load_csv("time_dim.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_warehouse(rows_to_check), load_csv("warehouse.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_web_page(rows_to_check), load_csv("web_page.csv"));
    const auto [web_sales_table, web_returns_table] = table_generator.generate_web_sales_and_returns(rows_to_check);
    EXPECT_TABLE_EQ_ORDERED(web_sales_table, load_csv("web_sales.csv"));
    EXPECT_TABLE_EQ_ORDERED(web_returns_table, load_csv("web_returns.csv"));
    EXPECT_TABLE_EQ_ORDERED(table_generator.generate_web_site(rows_to_check), load_csv("web_site.csv"));
  }
}

TEST_F(TPCDSTableGeneratorTest, GenerateAndStoreRowCountsAndTableConstraints) {
  EXPECT_EQ(Hyrise::get().catalog.tables().size(), 0);
  TPCDSTableGenerator{1, Chunk::DEFAULT_SIZE, 0}.generate_and_store();

  /**
   * Check that
   *    (i) all TPC-DS tables are created by the TPCDSTableGenerator and added to the StorageManager,
   *   (ii) their row counts are as expected, and
   *  (iii) they have one primary key and the expected number of foreign keys.
   */
  using TableSpec = std::tuple<uint64_t, uint32_t>;  // <row count, number of foreign keys>
  const auto expected_table_info = std::map<std::string, TableSpec>{{"call_center", {6, 2}},
                                                                    {"catalog_page", {11718, 2}},
                                                                    {"catalog_returns", {144201, 17}},
                                                                    {"catalog_sales", {1440060, 17}},
                                                                    {"customer", {100000, 6}},
                                                                    {"customer_address", {50000, 0}},
                                                                    {"customer_demographics", {1920800, 0}},
                                                                    {"date_dim", {73049, 0}},
                                                                    {"household_demographics", {7200, 1}},
                                                                    {"income_band", {20, 0}},
                                                                    {"inventory", {11745000, 3}},
                                                                    {"item", {18000, 0}},
                                                                    {"promotion", {300, 3}},
                                                                    {"reason", {35, 0}},
                                                                    {"ship_mode", {20, 0}},
                                                                    {"store", {12, 1}},
                                                                    {"store_returns", {288324, 10}},
                                                                    {"store_sales", {2879434, 9}},
                                                                    {"time_dim", {86400, 0}},
                                                                    {"warehouse", {5, 0}},
                                                                    {"web_page", {60, 3}},
                                                                    {"web_returns", {71746, 14}},
                                                                    {"web_sales", {719620, 17}},
                                                                    {"web_site", {30, 2}}};

  EXPECT_EQ(Hyrise::get().catalog.tables().size(), expected_table_info.size());
  for (const auto& [name, table_info] : expected_table_info) {
    SCOPED_TRACE("checking table " + name);
    const auto table_id = Hyrise::get().catalog.table_id(name);
    const auto table = Hyrise::get().storage_manager.get_table(table_id);
    const auto [size, foreign_key_count] = table_info;
    EXPECT_EQ(table->row_count(), size);

    // All TPC-DS table have a primary key.
    EXPECT_EQ(table->soft_key_constraints().size(), 1);
    EXPECT_EQ(table->soft_foreign_key_constraints().size(), foreign_key_count);
  }
}

}  // namespace hyrise
