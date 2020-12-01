#include "base_test.hpp"
#include "lib/utils/meta_tables/meta_mock_table.hpp"

#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/meta_tables/meta_chunk_sort_orders_table.hpp"
#include "utils/meta_tables/meta_chunks_table.hpp"
#include "utils/meta_tables/meta_columns_table.hpp"
#include "utils/meta_tables/meta_plugins_table.hpp"
#include "utils/meta_tables/meta_segments_accurate_table.hpp"
#include "utils/meta_tables/meta_segments_table.hpp"
#include "utils/meta_tables/meta_settings_table.hpp"
#include "utils/meta_tables/meta_tables_table.hpp"

#include "meta_mock_table.hpp"

namespace opossum {

using MetaTable = std::shared_ptr<AbstractMetaTable>;
using MetaTables = std::vector<MetaTable>;
using MetaTableNames = std::vector<std::string>;

#ifdef __GLIBCXX__
auto lib_suffix = "_libstdcpp";
#elif _LIBCPP_VERSION
auto lib_suffix = "_libcpp";
#else
static_assert(false, "Unknown c++ library");
#endif

class MetaTableTest : public BaseTest {
 public:
  static MetaTables meta_tables() {
    return {std::make_shared<MetaTablesTable>(),   std::make_shared<MetaColumnsTable>(),
            std::make_shared<MetaChunksTable>(),   std::make_shared<MetaChunkSortOrdersTable>(),
            std::make_shared<MetaSegmentsTable>(), std::make_shared<MetaSegmentsAccurateTable>()};
  }

  static MetaTableNames meta_table_names() {
    MetaTableNames names;
    for (auto& table : MetaTableTest::meta_tables()) {
      names.push_back(table->name());
    }

    return names;
  }

  const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) const {
    return table->_generate();
  }

 protected:
  const std::string test_file_path = "resources/test_data/tbl/meta_tables/meta_";
  std::shared_ptr<Table> int_int;
  std::shared_ptr<Table> int_int_int_null;
  std::shared_ptr<const Table> mock_manipulation_values;

  void SetUp() override {
    auto& storage_manager = Hyrise::get().storage_manager;

    int_int = load_table("resources/test_data/tbl/int_int.tbl", 2);
    int_int_int_null = load_table("resources/test_data/tbl/int_int_int_null.tbl", 100);

    ChunkEncoder::encode_chunk(int_int_int_null->get_chunk(ChunkID{0}), int_int_int_null->column_data_types(),
                               {SegmentEncodingSpec{EncodingType::RunLength},
                                SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128},
                                SegmentEncodingSpec{EncodingType::Unencoded}});

    storage_manager.add_table("int_int", int_int);
    storage_manager.add_table("int_int_int_null", int_int_int_null);

    const auto column_definitions = MetaMockTable().column_definitions();
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    table->append({pmr_string{"foo"}});
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    mock_manipulation_values = table_wrapper->get_output();
  }

  void TearDown() override { Hyrise::reset(); }

  void _add_meta_table(const std::shared_ptr<AbstractMetaTable>& table) {
    Hyrise::get().meta_table_manager._add(table);
  }
};

class MultiMetaTablesTest : public MetaTableTest, public ::testing::WithParamInterface<MetaTable> {};

auto meta_table_test_formatter = [](const ::testing::TestParamInfo<MetaTable> info) {
  auto stream = std::stringstream{};
  stream << info.param->name();

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(MetaTable, MultiMetaTablesTest, ::testing::ValuesIn(MetaTableTest::meta_tables()),
                         meta_table_test_formatter);

TEST_P(MultiMetaTablesTest, IsImmutable) {
  EXPECT_FALSE(GetParam()->can_insert());
  EXPECT_FALSE(GetParam()->can_update());
  EXPECT_FALSE(GetParam()->can_delete());
}

TEST_P(MultiMetaTablesTest, MetaTableGeneration) {
  std::string suffix = GetParam()->name() == "segments" || GetParam()->name() == "segments_accurate" ? lib_suffix : "";
  const auto meta_table = generate_meta_table(GetParam());
  const auto expected_table = load_table(test_file_path + GetParam()->name() + suffix + ".tbl");

  // The values in the AccessCounters depend on how the segments are accessed during the test. As such, the values in
  // meta_segments*.tbl are fragile and may become outdated.
  EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
}

TEST_P(MultiMetaTablesTest, IsDynamic) {
  std::string suffix = GetParam()->name() == "segments" || GetParam()->name() == "segments_accurate" ? lib_suffix : "";
  SQLPipelineBuilder{"UPDATE int_int SET a = a + 1000 WHERE a < 1000"}.create_pipeline().get_result_table();
  SQLPipelineBuilder{"INSERT INTO int_int_int_null (a, b, c) VALUES (NULL, 1, 2)"}.create_pipeline().get_result_table();

  if (GetParam()->name() == "chunk_sort_orders") {
    Hyrise::get()
        .storage_manager.get_table("int_int")
        ->get_chunk(ChunkID{0})
        ->set_individually_sorted_by(SortColumnDefinition(ColumnID{1}, SortMode::Ascending));
  }

  const auto expected_table = load_table(test_file_path + GetParam()->name() + suffix + "_updated.tbl");
  const auto meta_table = generate_meta_table(GetParam());

  EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
}

TEST_P(MultiMetaTablesTest, HandlesDeletedChunks) {
  // Meta tables that access stored tables without going through GetTable need to handle nullptr explicitly. We do not
  // check the actual results in order to avoid the number of test tables (that would have to be updated if the memory
  // consumption changes) low. Instead, we simply ensure that the meta table is generated without dereferencing said
  // nullptr.

  const auto int_int = Hyrise::get().storage_manager.get_table("int_int");

  SQLPipelineBuilder{"DELETE FROM int_int"}.create_pipeline().get_result_table();
  int_int->remove_chunk(ChunkID{0});

  generate_meta_table(GetParam());
}

TEST_P(MultiMetaTablesTest, SQLFeatures) {
  // TEST SQL features on meta tables
  const auto result = SQLPipelineBuilder{"SELECT COUNT(*) FROM " + MetaTableManager::META_PREFIX + GetParam()->name()}
                          .create_pipeline()
                          .get_result_table();

  EXPECT_EQ(result.first, SQLPipelineStatus::Success);
  EXPECT_EQ(result.second->row_count(), 1);
}

TEST_F(MetaTableTest, SingleGenerationInPipeline) {
  auto mock_table = std::make_shared<MetaMockTable>();
  _add_meta_table(mock_table);

  EXPECT_EQ(mock_table->generate_calls(), 0);
  SQLPipelineBuilder{"SELECT * FROM meta_mock"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 1);
  SQLPipelineBuilder{"DELETE FROM meta_mock WHERE mock='abc'"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 2);
  SQLPipelineBuilder{"INSERT INTO meta_mock VALUES('foo')"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 3);
  SQLPipelineBuilder{"UPDATE meta_mock SET mock='foo'"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 4);
}

TEST_F(MetaTableTest, IsNotCached) {
  auto mock_table = std::make_shared<MetaMockTable>();
  _add_meta_table(mock_table);
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  SQLPipelineBuilder{"SELECT * FROM meta_mock"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 1);
  SQLPipelineBuilder{"SELECT * FROM meta_mock"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 2);

  SQLPipelineBuilder{"INSERT INTO meta_mock VALUES('bar')"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 3);
  EXPECT_EQ(mock_table->insert_calls(), 1);
  SQLPipelineBuilder{"INSERT INTO meta_mock VALUES('bar')"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 4);
  EXPECT_EQ(mock_table->insert_calls(), 2);

  SQLPipelineBuilder{"DELETE FROM meta_mock WHERE mock='mock_value'"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 5);
  EXPECT_EQ(mock_table->remove_calls(), 1);
  SQLPipelineBuilder{"DELETE FROM meta_mock WHERE mock='mock_value'"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 6);
  EXPECT_EQ(mock_table->remove_calls(), 2);

  SQLPipelineBuilder{"UPDATE meta_mock SET mock='bar' WHERE mock='mock_value'"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 7);
  EXPECT_EQ(mock_table->update_calls(), 1);
  SQLPipelineBuilder{"UPDATE meta_mock SET mock='bar' WHERE mock='mock_value'"}.create_pipeline().get_result_table();
  EXPECT_EQ(mock_table->generate_calls(), 8);
  EXPECT_EQ(mock_table->update_calls(), 2);
}
}  // namespace opossum
