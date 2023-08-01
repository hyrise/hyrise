#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_positions.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class OperatorTaskTest : public BaseTest {
 protected:
  using TaskVector = std::vector<std::shared_ptr<AbstractTask>>;

  void SetUp() override {
    _test_table_a = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table("table_a", _test_table_a);

    _test_table_b = load_table("resources/test_data/tbl/int_float2.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table("table_b", _test_table_b);
  }

  void clear_successors(const std::shared_ptr<AbstractTask>& task) {
    task->_successors.clear();
  }

  std::shared_ptr<Table> _test_table_a, _test_table_b;
};

TEST_F(OperatorTaskTest, BasicTasksFromOperatorTest) {
  const auto gt = std::make_shared<GetTable>("table_a");
  const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(gt);

  ASSERT_EQ(tasks.size(), 1);
  EXPECT_NO_THROW(root_operator_task->schedule());

  EXPECT_TABLE_EQ_UNORDERED(_test_table_a, gt->get_output());
}

TEST_F(OperatorTaskTest, SingleDependencyTasksFromOperatorTest) {
  const auto gt = std::make_shared<GetTable>("table_a");
  const auto a = PQPColumnExpression::from_table(*_test_table_a, "a");
  const auto ts = std::make_shared<TableScan>(gt, equals_(a, 1234));

  const auto& [tasks, _] = OperatorTask::make_tasks_from_operator(ts);
  for (const auto& task : tasks) {
    EXPECT_NO_THROW(task->schedule());
    // We don't have to wait here, because we are running the task tests without a scheduler.
  }

  const auto expected_result = load_table("resources/test_data/tbl/int_float_filtered.tbl", ChunkOffset{2});
  EXPECT_TABLE_EQ_UNORDERED(expected_result, ts->get_output());
}

TEST_F(OperatorTaskTest, DoubleDependencyTasksFromOperatorTest) {
  const auto gt_a = std::make_shared<GetTable>("table_a");
  const auto gt_b = std::make_shared<GetTable>("table_b");
  const auto join = std::make_shared<JoinHash>(
      gt_a, gt_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});

  const auto& [tasks, _] = OperatorTask::make_tasks_from_operator(join);
  for (const auto& task : tasks) {
    EXPECT_NO_THROW(task->schedule());
    // We don't have to wait here, because we are running the task tests without a scheduler.
  }

  const auto expected_result = load_table("resources/test_data/tbl/join_operators/int_inner_join.tbl", ChunkOffset{2});
  EXPECT_TABLE_EQ_UNORDERED(expected_result, join->get_output());
}

TEST_F(OperatorTaskTest, MakeDiamondShape) {
  const auto gt_a = std::make_shared<GetTable>("table_a");
  const auto a = PQPColumnExpression::from_table(*_test_table_a, "a");
  const auto b = PQPColumnExpression::from_table(*_test_table_a, "b");
  const auto scan_a = std::make_shared<TableScan>(gt_a, greater_than_equals_(a, 1234));
  const auto scan_b = std::make_shared<TableScan>(scan_a, less_than_(b, 1000));
  const auto scan_c = std::make_shared<TableScan>(scan_a, greater_than_(b, 2000));
  const auto union_positions = std::make_shared<UnionPositions>(scan_b, scan_c);

  const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(union_positions);

  ASSERT_EQ(tasks.size(), 5u);
  const auto tasks_set = std::unordered_set<std::shared_ptr<AbstractTask>>(tasks.begin(), tasks.end());
  EXPECT_TRUE(tasks_set.contains(gt_a->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(scan_a->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(scan_b->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(scan_c->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(union_positions->get_or_create_operator_task()));

  EXPECT_EQ(gt_a->get_or_create_operator_task()->successors(), TaskVector{scan_a->get_or_create_operator_task()});
  const auto scan_a_successors =
      TaskVector{scan_b->get_or_create_operator_task(), scan_c->get_or_create_operator_task()};
  EXPECT_EQ(scan_a->get_or_create_operator_task()->successors(), scan_a_successors);
  EXPECT_EQ(scan_b->get_or_create_operator_task()->successors(),
            TaskVector{union_positions->get_or_create_operator_task()});
  EXPECT_EQ(scan_c->get_or_create_operator_task()->successors(),
            TaskVector{union_positions->get_or_create_operator_task()});
  EXPECT_EQ(union_positions->get_or_create_operator_task()->successors(), TaskVector{});

  for (const auto& task : tasks) {
    EXPECT_NO_THROW(task->schedule());
    // We don't have to wait here, because we are running the task tests without a scheduler.
  }
}

TEST_F(OperatorTaskTest, UncorrelatedSubqueries) {
  // Uncorrelated subqueries in the predicates of TableScan and Projection operators should be wrapped in tasks
  // together with the rest of the PQP. Thus, the subqueries are scheduled accordingly and not created and executed
  // multiple times. The query plan used in this test conforms to the following query:
  // SELECT 1 + (SELECT  AVG(table_a.a) FROM table_a WHERE table_a.a > (SELECT MIN(table_b.a) FROM table_b));

  // SELECT MIN(table_b.a) FROM table_b
  const auto gt_b = std::make_shared<GetTable>("table_b", std::vector<ChunkID>{}, std::vector<ColumnID>{0});
  const auto b_a = PQPColumnExpression::from_table(*_test_table_b, "a");
  using WindowFunctionExpressions = std::vector<std::shared_ptr<WindowFunctionExpression>>;
  const auto aggregate_a =
      std::make_shared<AggregateHash>(gt_b, WindowFunctionExpressions{min_(b_a)}, std::vector<ColumnID>{});

  // SELECT AVG(table_a.a) FROM table_a WHERE table_a.a > <subquery_result>
  const auto gt_a = std::make_shared<GetTable>("table_a");
  const auto a_a = PQPColumnExpression::from_table(*_test_table_a, "a");
  const auto scan =
      std::make_shared<TableScan>(gt_a, greater_than_(a_a, pqp_subquery_(aggregate_a, DataType::Int, false)));
  auto aggregate_b =
      std::make_shared<AggregateHash>(scan, WindowFunctionExpressions{avg_(a_a)}, std::vector<ColumnID>{});

  // SELECT 1 + <subquery_result>
  const auto table_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  const auto projection = std::make_shared<Projection>(
      table_wrapper, expression_vector(add_(value_(1), pqp_subquery_(aggregate_b, DataType::Double, false))));

  const auto& [tasks, root_operator_task] = OperatorTask::make_tasks_from_operator(projection);

  ASSERT_EQ(tasks.size(), 7);
  const auto tasks_set = std::unordered_set<std::shared_ptr<AbstractTask>>(tasks.begin(), tasks.end());
  EXPECT_TRUE(tasks_set.contains(gt_a->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(gt_b->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(aggregate_a->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(scan->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(aggregate_b->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(table_wrapper->get_or_create_operator_task()));
  EXPECT_TRUE(tasks_set.contains(projection->get_or_create_operator_task()));

  EXPECT_EQ(gt_b->get_or_create_operator_task()->successors(), TaskVector{aggregate_a->get_or_create_operator_task()});

  EXPECT_EQ(gt_a->get_or_create_operator_task()->successors(), TaskVector{scan->get_or_create_operator_task()});
  EXPECT_EQ(aggregate_a->get_or_create_operator_task()->successors(), TaskVector{scan->get_or_create_operator_task()});

  EXPECT_EQ(scan->get_or_create_operator_task()->successors(), TaskVector{aggregate_b->get_or_create_operator_task()});

  EXPECT_EQ(aggregate_b->get_or_create_operator_task()->successors(),
            TaskVector{projection->get_or_create_operator_task()});
  EXPECT_EQ(table_wrapper->get_or_create_operator_task()->successors(),
            TaskVector{projection->get_or_create_operator_task()});

  EXPECT_EQ(root_operator_task, projection->get_or_create_operator_task());
  EXPECT_TRUE(projection->get_or_create_operator_task()->successors().empty());

  for (const auto& task : tasks) {
    EXPECT_NO_THROW(task->schedule());
    // We don't have to wait here, because we are running the task tests without a scheduler.
  }
}

TEST_F(OperatorTaskTest, DetectCycles) {
  // Ensure that we cannot create tasks that have cyclic dependencies and, thus, would end up in a deadlock during
  // execution. In this test case, we achieve this with an invalid PQP that consists of one cycle. During task creation,
  // it is more likely to create cycles by incorrectly setting the tasks' predecessors. This test ensures that we notice
  // when this happens.
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  // Declare a MockOperator class that allows us to set an input operator after instantiation.
  class MockOperator : public AbstractReadOnlyOperator {
   public:
    explicit MockOperator(const std::shared_ptr<const AbstractOperator>& input_operator)
        : AbstractReadOnlyOperator{OperatorType::Mock, input_operator} {}

    const std::string& name() const override {
      static const auto name = std::string{"MockOperator"};
      return name;
    }

    void set_input(const std::shared_ptr<const AbstractOperator>& input_operator) {
      _left_input = input_operator;
    }

    std::shared_ptr<AbstractTask> get_task() {
      return _operator_task.lock();
    }

   protected:
    std::shared_ptr<const Table> _on_execute() override {
      return nullptr;
    }

    std::shared_ptr<AbstractOperator> _on_deep_copy(
        const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
        const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
        std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override {
      return nullptr;
    }

    void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& /*parameters*/) override {}
  };

  // Create some operators that are an input of the next one.
  const auto& mock_operator_a = std::make_shared<MockOperator>(nullptr);
  const auto& mock_operator_b = std::make_shared<MockOperator>(mock_operator_a);
  const auto& mock_operator_c = std::make_shared<MockOperator>(mock_operator_b);
  const auto& mock_operator_d = std::make_shared<MockOperator>(mock_operator_c);

  // Set the last operator as input of the first one. Now, we have a cycle.
  mock_operator_a->set_input(mock_operator_d);

  EXPECT_THROW(OperatorTask::make_tasks_from_operator(mock_operator_a), std::logic_error);

  // Clear the successors of the created tasks and the operator inputs. Since the tasks hold shared pointers to their
  // successors and the operators a pointer to their input, the cyclic graph leaks memory.
  for (const auto& op : {mock_operator_a, mock_operator_b, mock_operator_c, mock_operator_d}) {
    op->set_input(nullptr);
    const auto& task = op->get_task();
    if (task) {
      clear_successors(task);
    }
  }
}

TEST_F(OperatorTaskTest, SkipOperatorTask) {
  const auto table = std::make_shared<GetTable>("table_a");
  table->execute();

  const auto task = std::make_shared<OperatorTask>(table);
  task->skip_operator_task();
  EXPECT_TRUE(task->is_done());
}

TEST_F(OperatorTaskTest, NotExecutedOperatorTaskCannotBeSkipped) {
  const auto table = std::make_shared<GetTable>("table_a");

  const auto task = std::make_shared<OperatorTask>(table);
  EXPECT_THROW(task->skip_operator_task(), std::logic_error);
}

TEST_F(OperatorTaskTest, DoNotSkipOperatorTaskWithMultiOwners) {
  const auto table = std::make_shared<GetTable>("table_a");
  table->execute();

  const auto task = std::make_shared<OperatorTask>(table);
  const auto another_task_pointer = task;
  EXPECT_EQ(task.use_count(), 2);
  EXPECT_THROW(task->skip_operator_task(), std::logic_error);
}

}  // namespace hyrise
