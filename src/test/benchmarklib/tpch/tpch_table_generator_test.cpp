#include "base_test.hpp"
#include "hyrise.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

class TPCHTableGeneratorTest : public BaseTest {};

TEST_F(TPCHTableGeneratorTest, SmallScaleFactor) {
  /**
   * Check whether the data that TPCHTableGenerator generates with a scale factor of 0.01 is the exact same that dbgen
   * generates.
   */
  const auto dir_001 = std::string{"resources/test_data/tbl/tpch/sf-0.01/"};

  const auto chunk_size = ChunkOffset{1'000};
  auto table_info_by_name = TPCHTableGenerator(0.01f, ClusteringConfiguration::None, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("part").table, load_table(dir_001 + "part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("supplier").table, load_table(dir_001 + "supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("partsupp").table, load_table(dir_001 + "partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("customer").table, load_table(dir_001 + "customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("orders").table, load_table(dir_001 + "orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("nation").table, load_table(dir_001 + "nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("region").table, load_table(dir_001 + "region.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("lineitem").table, load_table(dir_001 + "lineitem.tbl", chunk_size));

// TODO(dey4ss): replace with macros once merged.
#if defined(__has_feature)
#if (__has_feature(thread_sanitizer) || __has_feature(address_sanitizer))
  // We verified thread and address safety above. As this is quite expensive to sanitize, do not perform the following
  // check - double parantheses mark the code as explicitly dead.
  if ((true)) {
    return;
  }
#endif
#endif

  // Run generation a second time to make sure no global state (of which tpch_dbgen has plenty :( ) from the first
  // generation process carried over into the second

  const auto dir_002 = std::string{"resources/test_data/tbl/tpch/sf-0.02/"};

  table_info_by_name = TPCHTableGenerator(0.02f, ClusteringConfiguration::None, chunk_size).generate();

  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("part").table, load_table(dir_002 + "part.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("supplier").table, load_table(dir_002 + "supplier.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("partsupp").table, load_table(dir_002 + "partsupp.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("customer").table, load_table(dir_002 + "customer.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("orders").table, load_table(dir_002 + "orders.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("nation").table, load_table(dir_002 + "nation.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("region").table, load_table(dir_002 + "region.tbl", chunk_size));
  EXPECT_TABLE_EQ_ORDERED(table_info_by_name.at("lineitem").table, load_table(dir_002 + "lineitem.tbl", chunk_size));

  for (const auto& [table_name, table_info] : table_info_by_name) {
    const auto& table = table_info.table;
    for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count() - 1; ++chunk_id) {
      EXPECT_EQ(table->get_chunk(chunk_id)->size(), 1000);
    }
    EXPECT_LE(table->last_chunk()->size(), 1000);
  }
}

TEST_F(TPCHTableGeneratorTest, RowCountsMediumScaleFactor) {
  /**
   * Mostly intended to generate coverage and trigger potential leaks in third_party/tpch_dbgen.
   */
  const auto scale_factor = 1.0f;
  const auto& table_info_by_name = TPCHTableGenerator(scale_factor, ClusteringConfiguration::None).generate();

  EXPECT_EQ(table_info_by_name.at("part").table->row_count(), std::floor(200'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("supplier").table->row_count(), std::floor(10'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("partsupp").table->row_count(), std::floor(800'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("customer").table->row_count(), std::floor(150'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("orders").table->row_count(), std::floor(1'500'000 * scale_factor));
  EXPECT_EQ(table_info_by_name.at("nation").table->row_count(), 25);
  EXPECT_EQ(table_info_by_name.at("region").table->row_count(), 5);
  const auto lineitem_cardinality = static_cast<float>(table_info_by_name.at("lineitem").table->row_count());
  const auto epsilon = 0.001;
  EXPECT_LE(lineitem_cardinality, 6'000'000 * scale_factor * (1 + epsilon));
  EXPECT_GE(lineitem_cardinality, 6'000'000 * scale_factor * (1 - epsilon));
}

TEST_F(TPCHTableGeneratorTest, GenerateAndStore) {
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("part"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("supplier"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("partsupp"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("customer"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("orders"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("nation"));
  EXPECT_FALSE(Hyrise::get().storage_manager.has_table("region"));
  ASSERT_FALSE(Hyrise::get().storage_manager.has_table("lineitem"));

  // Small scale factor
  TPCHTableGenerator(0.01f, ClusteringConfiguration::None, Chunk::DEFAULT_SIZE).generate_and_store();

  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("part"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("supplier"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("partsupp"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("customer"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("orders"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("nation"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("region"));
  EXPECT_TRUE(Hyrise::get().storage_manager.has_table("lineitem"));
}

TEST_F(TPCHTableGeneratorTest, TableConstraints) {
  // We do not check the constraints in detail, just verify each table has a PK and the number of FKs as defined in the
  // specification.
  TPCHTableGenerator{0.01f, ClusteringConfiguration::None}.generate_and_store();

  const auto& part_table = Hyrise::get().storage_manager.get_table("part");
  EXPECT_EQ(part_table->soft_key_constraints().size(), 1);
  EXPECT_TRUE(part_table->soft_foreign_key_constraints().empty());

  const auto& supplier_table = Hyrise::get().storage_manager.get_table("supplier");
  EXPECT_EQ(supplier_table->soft_key_constraints().size(), 1);
  EXPECT_EQ(supplier_table->soft_foreign_key_constraints().size(), 1);

  const auto& partsupp_table = Hyrise::get().storage_manager.get_table("partsupp");
  EXPECT_EQ(partsupp_table->soft_key_constraints().size(), 1);
  EXPECT_EQ(partsupp_table->soft_foreign_key_constraints().size(), 2);

  const auto& customer_table = Hyrise::get().storage_manager.get_table("customer");
  EXPECT_EQ(customer_table->soft_key_constraints().size(), 1);
  EXPECT_EQ(customer_table->soft_foreign_key_constraints().size(), 1);

  const auto& orders_table = Hyrise::get().storage_manager.get_table("orders");
  EXPECT_EQ(orders_table->soft_key_constraints().size(), 1);
  EXPECT_EQ(orders_table->soft_foreign_key_constraints().size(), 1);

  const auto& nation_table = Hyrise::get().storage_manager.get_table("nation");
  EXPECT_EQ(nation_table->soft_key_constraints().size(), 1);
  EXPECT_EQ(nation_table->soft_foreign_key_constraints().size(), 1);

  const auto& region_table = Hyrise::get().storage_manager.get_table("region");
  EXPECT_EQ(region_table->soft_key_constraints().size(), 1);
  EXPECT_TRUE(region_table->soft_foreign_key_constraints().empty());

  const auto& lineitem_table = Hyrise::get().storage_manager.get_table("lineitem");
  EXPECT_EQ(lineitem_table->soft_key_constraints().size(), 1);
  EXPECT_EQ(lineitem_table->soft_foreign_key_constraints().size(), 4);
}

}  // namespace hyrise
