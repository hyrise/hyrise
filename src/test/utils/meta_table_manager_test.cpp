#include "../base_test.hpp"

#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/meta_tables/meta_chunk_sort_orders_table.hpp"
#include "utils/meta_tables/meta_chunks_table.hpp"
#include "utils/meta_tables/meta_columns_table.hpp"
#include "utils/meta_tables/meta_log_table.hpp"
#include "utils/meta_tables/meta_plugins_table.hpp"
#include "utils/meta_tables/meta_segments_accurate_table.hpp"
#include "utils/meta_tables/meta_segments_table.hpp"
#include "utils/meta_tables/meta_settings_table.hpp"
#include "utils/meta_tables/meta_tables_table.hpp"

namespace opossum {

using MetaTable = std::shared_ptr<AbstractMetaTable>;
using MetaTables = std::vector<MetaTable>;
using MetaTableNames = std::vector<std::string>;

class MetaTableManagerTest : public BaseTest {
 public:
  static MetaTables meta_tables() {
    return {std::make_shared<MetaTablesTable>(),   std::make_shared<MetaColumnsTable>(),
            std::make_shared<MetaChunksTable>(),   std::make_shared<MetaChunkSortOrdersTable>(),
            std::make_shared<MetaSegmentsTable>(), std::make_shared<MetaSegmentsAccurateTable>(),
            std::make_shared<MetaPluginsTable>(),  std::make_shared<MetaSettingsTable>(),
            std::make_shared<MetaLogTable>()};
  }

  static MetaTableNames meta_table_names() {
    MetaTableNames names;
    for (auto& table : MetaTableManagerTest::meta_tables()) {
      names.push_back(table->name());
    }

    return names;
  }

  // We need this as the add method of MetaTableManager is protected.
  // Won't compile if add is not called by test class, which is a friend of MetaTableManager.
  static void add_meta_table(const MetaTable& table) { Hyrise::get().meta_table_manager._add(table); }

 protected:
  std::shared_ptr<const Table> mock_manipulation_values;

  void SetUp() {
    Hyrise::reset();

    const auto column_definitions = MetaMockTable().column_definitions();
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    table->append({pmr_string{"foo"}});
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    mock_manipulation_values = table_wrapper->get_output();
  }

TEST_F(MetaTableManagerTest, TableBasedMetaData) {
  // This tests a bunch of meta tables that are somehow related to the tables stored in the StorageManager.
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto prefix = MetaTableManager::META_PREFIX;
  const auto path = std::string{"resources/test_data/tbl/meta_tables/meta_"};
  for (const auto& meta_table_name : {"tables", "columns", "chunks", "segments", "segments_accurate"}) {
    SCOPED_TRACE(meta_table_name);

    const auto int_int = load_table("resources/test_data/tbl/int_int.tbl", 2);
    const auto int_int_int_null = load_table("resources/test_data/tbl/int_int_int_null.tbl", 100);
    ChunkEncoder::encode_chunk(int_int_int_null->get_chunk(ChunkID{0}), int_int_int_null->column_data_types(),
                               {{EncodingType::RunLength},
                                {EncodingType::Dictionary, VectorCompressionType::SimdBp128},
                                {EncodingType::Unencoded}});

    if (storage_manager.has_table("int_int")) storage_manager.drop_table("int_int");
    if (storage_manager.has_table("int_int_int_null")) storage_manager.drop_table("int_int_int_null");

    storage_manager.add_table("int_int", int_int);
    storage_manager.add_table("int_int_int_null", int_int_int_null);

    {
      const auto meta_table = storage_manager.get_table(prefix + meta_table_name);
      const auto expected_table = load_table(path + meta_table_name + ".tbl");
      EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
    }

    // Update the tables
    SQLPipelineBuilder{"UPDATE int_int SET a = a + 1000 WHERE a < 1000"}.create_pipeline().get_result_table();
    SQLPipelineBuilder{"INSERT INTO int_int_int_null (a, b, c) VALUES (NULL, 1, 2)"}
        .create_pipeline()
        .get_result_table();

    {
      const auto meta_table = storage_manager.get_table(prefix + meta_table_name);
      const auto expected_table = load_table(path + meta_table_name + "_updated.tbl");
      EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
    }
  }

  // Test meta table for chunk sort orders
  {
    const auto int_int = load_table("resources/test_data/tbl/int_int.tbl", 100);
    storage_manager.add_table("int_int_sort", int_int);

    const auto meta_table = storage_manager.get_table(prefix + "chunk_sort_orders");
    EXPECT_EQ(meta_table->row_count(), 0ul);

    storage_manager.get_table("int_int_sort")
        ->get_chunk(ChunkID{0})
        ->set_ordered_by({ColumnID{1}, OrderByMode::Ascending});
    const auto meta_table_updated = storage_manager.get_table(prefix + "chunk_sort_orders");
    EXPECT_EQ(meta_table_updated->row_count(), 1ul);
    EXPECT_EQ(meta_table_updated->get_value<int32_t>("chunk_id", 0), 0);
    EXPECT_EQ(meta_table_updated->get_value<int32_t>("column_id", 0), 1);
    EXPECT_EQ(meta_table_updated->get_value<pmr_string>("order_mode", 0), pmr_string{"AscendingNullsFirst"});
  }

  {
    // TEST SQL features on meta tables
    const auto result = SQLPipelineBuilder{"SELECT COUNT(*) FROM meta_tables WHERE table_name = 'int_int'"}
                            .create_pipeline()
                            .get_result_table();

    EXPECT_EQ(result.first, SQLPipelineStatus::Success);
    EXPECT_EQ(result.second->get_value<int64_t>(ColumnID{0}, 0), 1);
  }
}

}  // namespace opossum
