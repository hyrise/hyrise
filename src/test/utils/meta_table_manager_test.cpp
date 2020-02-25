#include "../base_test.hpp"

#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/meta_tables/meta_accurate_segments_table.hpp"
#include "utils/meta_tables/meta_chunk_orders_table.hpp"
#include "utils/meta_tables/meta_chunks_table.hpp"
#include "utils/meta_tables/meta_columns_table.hpp"
#include "utils/meta_tables/meta_mock_table.hpp"
#include "utils/meta_tables/meta_plugins_table.hpp"
#include "utils/meta_tables/meta_segments_table.hpp"
#include "utils/meta_tables/meta_tables_table.hpp"

namespace opossum {

using MetaTable = std::shared_ptr<AbstractMetaTable>;
using MetaTables = std::vector<MetaTable>;
using MetaTableNames = std::vector<std::string>;

class MetaTableManagerTest : public BaseTest {
 public:
  static MetaTables meta_tables() {
    return {std::make_shared<MetaTablesTable>(),   std::make_shared<MetaColumnsTable>(),
            std::make_shared<MetaChunksTable>(),   std::make_shared<MetaChunkOrdersTable>(),
            std::make_shared<MetaSegmentsTable>(), std::make_shared<MetaAccurateSegmentsTable>(),
            std::make_shared<MetaPluginsTable>()};
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
  static void add_meta_table(const MetaTable& table) { Hyrise::get().meta_table_manager.add(table); }

 protected:
  const std::string _test_file_path = "resources/test_data/tbl/meta_tables/meta_";
  std::shared_ptr<Table> int_int;
  std::shared_ptr<Table> int_int_int_null;

  void SetUp() {
    int_int = load_table("resources/test_data/tbl/int_int.tbl", 2);
    int_int_int_null = load_table("resources/test_data/tbl/int_int_int_null.tbl", 100);
    Hyrise::reset();
    auto& storage_manager = Hyrise::get().storage_manager;
    storage_manager.add_table("int_int", int_int);
    storage_manager.add_table("int_int_int_null", int_int_int_null);
  }
};

class MetaTableManagerMultiTablesTest : public MetaTableManagerTest, public ::testing::WithParamInterface<MetaTable> {};

auto formatter = [](const ::testing::TestParamInfo<MetaTable> info) {
  auto stream = std::stringstream{};
  stream << info.param->name();

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(MetaTable, MetaTableManagerMultiTablesTest,
                         ::testing::ValuesIn(MetaTableManagerTest::meta_tables()), formatter);

TEST_F(MetaTableManagerTest, ListAllTables) {
  auto table_names = MetaTableManagerTest::meta_table_names();
  std::sort(table_names.begin(), table_names.end());

  EXPECT_EQ(Hyrise::get().meta_table_manager.table_names(), table_names);
}

TEST_F(MetaTableManagerTest, ForwardsMethodCalls) {
  const auto mock_table = std::make_shared<MetaMockTable>();
  auto& mtm = Hyrise::get().meta_table_manager;

  MetaTableManagerTest::add_meta_table(mock_table);
  mtm.insert_into(mock_table->name(), nullptr);
  mtm.delete_from(mock_table->name(), nullptr);

  EXPECT_EQ(mock_table->insert_calls(), 1);
  EXPECT_EQ(mock_table->remove_calls(), 1);
}

TEST_P(MetaTableManagerMultiTablesTest, HasAllTables) {
  EXPECT_TRUE(Hyrise::get().meta_table_manager.has_table(GetParam()->name()));
}

TEST_P(MetaTableManagerMultiTablesTest, ForwardsMutationInfo) {
  const auto& table = GetParam();
  const auto& mtm = Hyrise::get().meta_table_manager;
  EXPECT_EQ(mtm.can_insert_into(table->name()), table->can_insert());
  EXPECT_EQ(mtm.can_delete_from(table->name()), table->can_delete());
  EXPECT_EQ(mtm.can_update(table->name()), table->can_update());
}

// TO DO: Make this nice and move to MetaTable tests. Avoid looping that can be replaced with parametrized test
TEST_F(MetaTableManagerTest, TableBasedMetaData) {
  // This tests a bunch of meta tables that are somehow related to the tables stored in the StorageManager.
  auto& storage_manager = Hyrise::get().storage_manager;

  const auto prefix = MetaTableManager::META_PREFIX;
  const auto path = std::string{"resources/test_data/tbl/meta_tables/meta_"};
  for (const auto& meta_table_name :
       std::vector<std::string>{"tables", "columns", "chunks", "segments", "segments_accurate"}) {
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

    auto lib_suffix = std::string{};
    if (meta_table_name == "segments" || meta_table_name == "segments_accurate") {
#ifdef __GLIBCXX__
      lib_suffix = "_libstdcpp";
#elif _LIBCPP_VERSION
      lib_suffix = "_libcpp";
#else
      static_assert(false, "Unknown c++ library");
#endif
    }

    {
      const auto meta_table = storage_manager.get_table(prefix + meta_table_name);
      const auto expected_table = load_table(path + meta_table_name + lib_suffix + ".tbl");
      EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
    }

    // Update the tables
    SQLPipelineBuilder{"UPDATE int_int SET a = a + 1000 WHERE a < 1000"}.create_pipeline().get_result_table();
    SQLPipelineBuilder{"INSERT INTO int_int_int_null (a, b, c) VALUES (NULL, 1, 2)"}
        .create_pipeline()
        .get_result_table();

    {
      const auto meta_table = storage_manager.get_table(prefix + meta_table_name);
      const auto expected_table = load_table(path + meta_table_name + lib_suffix + "_updated.tbl");
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
