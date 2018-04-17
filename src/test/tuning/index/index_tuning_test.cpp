#include "../../base_test.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "tuning/greedy_tuning_selector.hpp"
#include "tuning/index/index_tuning_evaluator.hpp"
#include "tuning/tuner.hpp"

namespace opossum {

class IndexTuningTest : public BaseTest {
 protected:
  void SetUp() override {
    _table = load_table("src/test/tables/tuning_customer.tbl", 10);
    StorageManager::get().add_table("CUSTOMER", _table);

    for (auto& chunk_ptr : _table->chunks()) {
      ChunkEncoder::encode_chunk(chunk_ptr, _table->column_data_types());
    }

    _clear_cache();
  }

  void TearDown() override {
    auto& storage_manager = StorageManager::get();
    if (storage_manager.has_table("CUSTOMER")) {
      storage_manager.drop_table("CUSTOMER");
    }
  }

  void _clear_cache() {
    SQLQueryCache<std::shared_ptr<AbstractLQPNode>>::get().clear();
    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  std::shared_ptr<Table> _table;
};

TEST_F(IndexTuningTest, EndToEndTest) {
  const std::vector<std::string> test_queries{"SELECT BALANCE FROM CUSTOMER WHERE NAME = 'Carry Wallach'",
                                              "SELECT NAME FROM CUSTOMER WHERE LEVEL = 5",
                                              "SELECT BALANCE FROM CUSTOMER WHERE NAME = 'Kimberley Seafowl'",
                                              "SELECT BALANCE FROM CUSTOMER WHERE NAME = 'Ethelda McNeely'",
                                              "SELECT NAME FROM CUSTOMER WHERE LEVEL = 3",
                                              "SELECT INTEREST FROM CUSTOMER WHERE NAME  = 'Rosemary Picardi'",
                                              "SELECT BALANCE FROM CUSTOMER WHERE NAME = 'Carry Wallach'"};

  opossum::Tuner tuner;
  tuner.add_evaluator(std::make_unique<opossum::IndexTuningEvaluator>());
  tuner.set_selector(std::make_unique<opossum::GreedyTuningSelector>());

  // Fill the cache with test queries
  for (const auto& query : test_queries) {
    SQLPipelineBuilder{query}.disable_mvcc().create_pipeline_statement().get_query_plan();
  }

  // Execute tuner
  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  // Flush cache to evict outdated query plans
  _clear_cache();

  // Check that exactly the expected indexes are built
  std::vector<IndexInfo> expected_index_infos = {
      IndexInfo{std::vector<ColumnID>{ColumnID{1}}, "", ColumnIndexType::GroupKey},
      IndexInfo{std::vector<ColumnID>{ColumnID{4}}, "", ColumnIndexType::GroupKey}};
  auto indexes = _table->get_indexes();
  for (const auto& index_info : indexes) {
    auto it = std::find(expected_index_infos.begin(), expected_index_infos.end(), index_info);
    ASSERT_NE(it, expected_index_infos.end());
    expected_index_infos.erase(it);
  }
  EXPECT_EQ(expected_index_infos.size(), 0u);
}

}  // namespace opossum
