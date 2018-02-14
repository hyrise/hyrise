#include "../../base_test.hpp"

#include <memory>
#include <vector>

#include "storage/table.hpp"
#include "tuning/index/index_evaluator.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"
#include "sql/sql_pipeline.hpp"
#include "type_cast.hpp"

namespace opossum {

class IndexEvaluatorTest : public BaseTest {
protected:
 void SetUp() override {
     // TODO(group01) get query cache from pipeline
     auto query_cache = std::make_shared<SQLQueryCache<std::shared_ptr<SQLQueryPlan>>>(1024);
     _evaluator = std::make_shared<IndexEvaluator>(query_cache);

   auto t = std::make_shared<Table>(10);
   t->add_column("col_1", DataType::Int);
   t->add_column("col_2", DataType::String);
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
 }

 void _inspect_lqp_operator(const std::shared_ptr<const AbstractLQPNode>& op, size_t query_frequency) {
    return _evaluator->_inspect_lqp_operator(op, query_frequency);
 }

 const std::vector<BaseIndexEvaluator::AccessRecord> & _access_records() {
     return _evaluator->_access_records;
 }

 std::shared_ptr<IndexEvaluator> _evaluator;
};

TEST_F(IndexEvaluatorTest, InspectLQPOperator) {
  SQLPipeline pipeline("select * from t where col_1 = 4", false);

  auto lqp = pipeline.get_optimized_logical_plans();

  EXPECT_TRUE(_access_records().empty());

  for (const auto& lqp_node : lqp) {
      _inspect_lqp_operator(lqp_node, 1);
  }

  EXPECT_EQ(_access_records().size(), 1u);
  EXPECT_EQ(_access_records().back().column_ref.table_name, "t");
  EXPECT_EQ(_access_records().back().column_ref.column_ids, std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(_access_records().back().condition, PredicateCondition::Equals);
}

}  // namespace opossum
