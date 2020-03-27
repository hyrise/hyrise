#include "base_test.hpp"

#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "utils/load_table.hpp"
#include "utils/meta_table_manager.hpp"
#include "utils/meta_tables/meta_chunk_sort_orders_table.hpp"
#include "utils/meta_tables/meta_chunks_table.hpp"
#include "utils/meta_tables/meta_columns_table.hpp"
#include "utils/meta_tables/meta_mock_table.hpp"
#include "utils/meta_tables/meta_plugins_table.hpp"
#include "utils/meta_tables/meta_segments_accurate_table.hpp"
#include "utils/meta_tables/meta_segments_table.hpp"
#include "utils/meta_tables/meta_settings_table.hpp"
#include "utils/meta_tables/meta_tables_table.hpp"

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

  void SetUp() {
    Hyrise::reset();
    auto& storage_manager = Hyrise::get().storage_manager;

    int_int = load_table("resources/test_data/tbl/int_int.tbl", 2);
    int_int_int_null = load_table("resources/test_data/tbl/int_int_int_null.tbl", 100);

    ChunkEncoder::encode_chunk(int_int_int_null->get_chunk(ChunkID{0}), int_int_int_null->column_data_types(),
                               {{EncodingType::RunLength},
                                {EncodingType::Dictionary, VectorCompressionType::SimdBp128},
                                {EncodingType::Unencoded}});

    storage_manager.add_table("int_int", int_int);
    storage_manager.add_table("int_int_int_null", int_int_int_null);

    const auto column_definitions = MetaMockTable().column_definitions();
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    table->append({pmr_string{"foo"}});
    auto table_wrapper = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper->execute();
    mock_manipulation_values = table_wrapper->get_output();
  }

  void TearDown() { Hyrise::reset(); }
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
  EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
}

TEST_P(MultiMetaTablesTest, IsDynamic) {
  std::string suffix = GetParam()->name() == "segments" || GetParam()->name() == "segments_accurate" ? lib_suffix : "";
  SQLPipelineBuilder{"UPDATE int_int SET a = a + 1000 WHERE a < 1000"}.create_pipeline().get_result_table();
  SQLPipelineBuilder{"INSERT INTO int_int_int_null (a, b, c) VALUES (NULL, 1, 2)"}.create_pipeline().get_result_table();

  Hyrise::get()
      .storage_manager.get_table("int_int")
      ->get_chunk(ChunkID{0})
      ->set_ordered_by({ColumnID{1}, OrderByMode::Ascending});

  const auto expected_table = load_table(test_file_path + GetParam()->name() + suffix + "_updated.tbl");
  const auto meta_table = generate_meta_table(GetParam());

  EXPECT_TABLE_EQ_UNORDERED(meta_table, expected_table);
}

TEST_P(MultiMetaTablesTest, SQLFeatures) {
  // TEST SQL features on meta tables
  const auto result = SQLPipelineBuilder{"SELECT COUNT(*) FROM " + MetaTableManager::META_PREFIX + GetParam()->name()}
                          .create_pipeline()
                          .get_result_table();

  EXPECT_EQ(result.first, SQLPipelineStatus::Success);
  EXPECT_EQ(result.second->row_count(), 1);
}

}  // namespace opossum
