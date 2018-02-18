#include "../../base_test.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/deprecated_dictionary_compression.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "tuning/index/column_ref.hpp"
#include "tuning/index/index_operation.hpp"

namespace opossum {

class IndexOperationTest : public BaseTest {
 protected:
  void SetUp() override { _ensure_pristine_table(); }

  // Drops existing table and creates one table for testing with default contents
  void _ensure_pristine_table() {
    _table = std::make_shared<Table>(3);
    _table->add_column("column_name", DataType::Int);
    _table->append({0});
    _table->append({1});
    _table->append({2});

    auto chunk = _table->get_chunk(ChunkID{0});
    DeprecatedDictionaryCompression::compress_chunk(_table->column_types(), chunk);

    auto& storage_manager = StorageManager::get();
    if (storage_manager.has_table("table_name")) {
      storage_manager.drop_table("table_name");
    }

    storage_manager.add_table("table_name", _table);
  }

  void TearDown() override {
    auto& storage_manager = StorageManager::get();
    if (storage_manager.has_table("table_name")) {
      storage_manager.drop_table("table_name");
    }
  }

  std::shared_ptr<Table> _table;
  ColumnRef _column_ref{"table_name", ColumnID{0}};
};

TEST_F(IndexOperationTest, GetColumRef) {
  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, true};
  EXPECT_EQ(operation.column(), _column_ref);
}

TEST_F(IndexOperationTest, GetColumnIndexType) {
  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, true};
  EXPECT_EQ(operation.type(), ColumnIndexType::GroupKey);
}

TEST_F(IndexOperationTest, GetCreate) {
  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, true};
  EXPECT_EQ(operation.create(), true);
}

TEST_F(IndexOperationTest, PrintOnStream) {
  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, true};

  std::stringstream stream;
  operation.print_on(stream);

  std::string result = stream.str();
  EXPECT_EQ(result, "IndexOperation{Create on table_name.(column_name)}");
}

TEST_F(IndexOperationTest, CreateIndex) {
  auto supported_index_types = {ColumnIndexType::GroupKey, ColumnIndexType::CompositeGroupKey,
                                ColumnIndexType::AdaptiveRadixTree};

  for (auto index_type : supported_index_types) {
    _ensure_pristine_table();

    IndexOperation operation{_column_ref, index_type, true};
    operation.execute();

    auto index_infos = _table->get_indexes();
    EXPECT_EQ(index_infos.size(), 1u);
    EXPECT_EQ(index_infos[0].type, index_type);
    EXPECT_EQ(index_infos[0].column_ids, std::vector{ColumnID{0}});
  }
}

TEST_F(IndexOperationTest, DeleteIndex) {
  auto column_ids = std::vector{ColumnID{0}};
  _table->create_index<GroupKeyIndex>(column_ids);

  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, false};
  operation.execute();

  auto index_infos = _table->get_indexes();
  EXPECT_EQ(index_infos.size(), 0u);
}

TEST_F(IndexOperationTest, ClearCacheWhenRemovingIndex) {
  auto& lqp_cache = SQLQueryCache<std::shared_ptr<AbstractLQPNode>>::get();
  auto& pqp_cache = SQLQueryCache<SQLQueryPlan>::get();

  MockNode::ColumnDefinitions column_definitions;
  column_definitions.push_back(std::make_pair(DataType::Int, "int_col"));
  lqp_cache.set("test", std::make_shared<MockNode>(column_definitions));
  pqp_cache.set("test", SQLQueryPlan{});

  EXPECT_GT(lqp_cache.size(), 0u);
  EXPECT_GT(pqp_cache.size(), 0u);

  auto column_ids = std::vector{ColumnID{0}};
  _table->create_index<GroupKeyIndex>(column_ids);

  IndexOperation operation{_column_ref, ColumnIndexType::GroupKey, false};
  operation.execute();

  EXPECT_EQ(lqp_cache.size(), 0u);
  EXPECT_EQ(pqp_cache.size(), 0u);
}

}  // namespace opossum
