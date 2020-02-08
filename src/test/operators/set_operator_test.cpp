#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "SQLParser.h"
#include "SQLParserResult.h"

#include "hyrise.hpp"
//#include "logical_query_plan/join_node.hpp"
//#include "operators/abstract_join_operator.hpp"
//#include "operators/print.hpp"
//#include "operators/validate.hpp"
//#include "scheduler/job_task.hpp"
//#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
//#include "sql/sql_plan_cache.hpp"

namespace opossum {

    class SetOperatorIntegrationTest : public BaseTest {
    protected:
        static void SetUpTestCase() {  // called ONCE before the tests
            _table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
            _table_a->append({12,350.7f});
            _table_b = load_table("resources/test_data/tbl/int_float2.tbl", 2);
            _except_result = load_table("resources/test_data/tbl/int_float.tbl", 2);

            TableColumnDefinitions column_definitions;
            column_definitions.emplace_back("a", DataType::Int, false);
            column_definitions.emplace_back("b", DataType::Float, false);
            _intersect_result = std::make_shared<Table>(column_definitions, TableType::Data);
            _intersect_result->append({12,350.7f});
        }

        void SetUp() override {
            Hyrise::reset();

            Hyrise::get().storage_manager.add_table("table_a", _table_a);
            Hyrise::get().storage_manager.add_table("table_b", _table_b);

            _pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
        }

        // Tables not modified during test case
        inline static std::shared_ptr<Table> _table_a;
        inline static std::shared_ptr<Table> _table_b;
        inline static std::shared_ptr<Table> _intersect_result;
        inline static std::shared_ptr<Table> _except_result;

        std::shared_ptr<SQLPhysicalPlanCache> _pqp_cache;

        const std::string _intersect_query_a = "SELECT * FROM table_a INTERSECT SELECT * FROM table_b";
        const std::string _intersect_query_b = "(SELECT * FROM table_a INTERSECT SELECT * FROM table_b) INTERSECT SELECT * FROM table_b";
        const std::string _except_query_a = "SELECT * FROM table_a EXCEPT SELECT * FROM table_b";
        const std::string _multiple_set_operations_query_a = "SELECT * FROM table_a EXCEPT (SELECT * FROM table_b INTERSECT SELECT * FROM table_a)";

    };

TEST_F(SetOperatorIntegrationTest, IntersectTest) {
auto sql_pipeline = SQLPipelineBuilder{_intersect_query_a}.create_pipeline();
const auto& [pipeline_status, table] = sql_pipeline.get_result_table();

EXPECT_TABLE_EQ_UNORDERED(table, _intersect_result);
}

TEST_F(SetOperatorIntegrationTest, MultipleIntersectTest) {
auto sql_pipeline = SQLPipelineBuilder{_intersect_query_b}.create_pipeline();
const auto& [pipeline_status, table] = sql_pipeline.get_result_table();

EXPECT_TABLE_EQ_UNORDERED(table, _intersect_result);
}

TEST_F(SetOperatorIntegrationTest, ExceptTest) {
auto sql_pipeline = SQLPipelineBuilder{_except_query_a}.create_pipeline();
const auto& [pipeline_status, table] = sql_pipeline.get_result_table();

EXPECT_TABLE_EQ_UNORDERED(table, _except_result);
}

TEST_F(SetOperatorIntegrationTest, MultipleSetOperatorsTest) {
auto sql_pipeline = SQLPipelineBuilder{_multiple_set_operations_query_a}.create_pipeline();
const auto& [pipeline_status, table] = sql_pipeline.get_result_table();

EXPECT_TABLE_EQ_UNORDERED(table, _except_result);
}

}  // namespace opossum
