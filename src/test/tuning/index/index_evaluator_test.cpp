#include <memory>
#include <vector>

#include "../../base_test.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "storage/table.hpp"
#include "tuning/index/index_evaluator.hpp"
#include "type_cast.hpp"

namespace opossum {

class IndexEvaluatorTest : public BaseTest {
 protected:
  void SetUp() override {
    _evaluator = std::make_shared<IndexEvaluator>();
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("col_1", DataType::Int);
    column_definitions.emplace_back("col_2", DataType::String);
    auto t = std::make_shared<Table>(column_definitions, TableType::Data, 10, UseMvcc::Yes);
    t->append({1, "1"});
    t->append({2, "2"});
    t->append({3, "3"});
    t->append({4, "4"});
    t->append({5, "5"});
    t->append({6, "6"});
    t->append({7, "7"});
    t->append({8, "8"});
    t->append({9, "9"});
    StorageManager::get().add_table("t", t);

    SQLQueryCache<std::shared_ptr<AbstractLQPNode>>::get().clear();
    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  void TearDown() override { StorageManager::get().drop_table("t"); }

  void _inspect_lqp_operator(const std::shared_ptr<const AbstractLQPNode>& op, size_t query_frequency,
                             std::vector<BaseIndexEvaluator::AccessRecord>& access_records) {
    _evaluator->_inspect_lqp_node(op, query_frequency, access_records);
  }

  std::vector<BaseIndexEvaluator::AccessRecord>& _access_records() { return _evaluator->_access_records; }

  std::shared_ptr<IndexEvaluator> _evaluator;
};

TEST_F(IndexEvaluatorTest, InspectLQPOperator) {
  auto sql_pipeline_statement =
      SQLPipelineBuilder{"select * from t where col_1 = 4"}.disable_mvcc().create_pipeline_statement();

  auto lqp_operator_root = sql_pipeline_statement.get_optimized_logical_plan();

  EXPECT_TRUE(_access_records().empty());

  _inspect_lqp_operator(lqp_operator_root, 1, _access_records());

  EXPECT_EQ(_access_records().size(), 1u);
  EXPECT_EQ(_access_records().back().column_ref.table_name, "t");
  EXPECT_EQ(_access_records().back().column_ref.column_ids, std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(_access_records().back().condition, PredicateCondition::Equals);
}

TEST_F(IndexEvaluatorTest, GenerateEvaluations) {
  // Trigger query plan generation + caching(!)
  const auto queries = {"select * from t where col_1 = 4", "select * from t where col_1 = 4",
                        "select * from t where col_1 = 5", "select * from t where col_1 = 6",
                        "select * from t where col_2 = '9'"};
  for (const auto& query : queries) {
    SQLPipelineBuilder{query}.disable_mvcc().create_pipeline_statement().get_query_plan();
  }

  std::vector<std::shared_ptr<TuningChoice>> tuning_choices;
  _evaluator->evaluate(tuning_choices);

  EXPECT_EQ(tuning_choices.size(), 2u);

  std::shared_ptr<IndexChoice> choice_col1 = std::dynamic_pointer_cast<IndexChoice>(tuning_choices[0]);
  std::shared_ptr<IndexChoice> choice_col2 = std::dynamic_pointer_cast<IndexChoice>(tuning_choices[1]);

  // Both indices aren't created yet
  EXPECT_EQ(choice_col1->is_currently_chosen(), false);
  EXPECT_EQ(choice_col2->is_currently_chosen(), false);

  // Given that col_1 is queried more often, the desirability to create an index should be higher
  EXPECT_GT(choice_col1->desirability(), choice_col2->desirability());
}

}  // namespace opossum
