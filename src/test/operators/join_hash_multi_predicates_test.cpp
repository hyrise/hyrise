#include "base_test.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

class JoinHashMultiplePredicatesTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_1_size = 1'000;
    _table_2_size = 1'000;

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int);
    column_definitions.emplace_back("b", DataType::Int);

    _table_1 = std::make_shared<Table>(column_definitions, TableType::Data, _table_1_size);
    _table_2 = std::make_shared<Table>(column_definitions, TableType::Data, _table_2_size);

    for (auto index = int{0}; index < _table_1_size; ++index) {
      _table_1->append({index % 2, index % 8});
    }
    for (auto index = int{0}; index < _table_2_size; ++index) {
      _table_2->append({index % 4, index % 16});
    }

    _table_1_wrapper = std::make_shared<TableWrapper>(_table_1);
    _table_1_wrapper->execute();

    _table_2_wrapper = std::make_shared<TableWrapper>(_table_2);
    _table_2_wrapper->execute();
  }

  void SetUp() override {}

  // Accumulates the RowIDs hidden behind the iterator element (hash map stores PosLists, not RowIDs)
  template <typename Iter>
  size_t get_row_count(Iter begin, Iter end) {
    size_t row_count = 0;
    for (Iter it = begin; it != end; ++it) {
      row_count += it->second.size();
    }
    return row_count;
  }

  inline static int _table_1_size = 0;
  inline static int _table_2_size = 0;
  inline static std::shared_ptr<Table> _table_1;
  inline static std::shared_ptr<Table> _table_2;
  inline static std::shared_ptr<TableWrapper> _table_1_wrapper;
  inline static std::shared_ptr<TableWrapper> _table_2_wrapper;

  void execute_multi_predicate_join(const std::shared_ptr<const AbstractOperator>& left,
                                    const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                    const std::vector<JoinPredicate>& join_predicates) {
    // execute join for the first join predicate
    std::shared_ptr<AbstractOperator> latest_operator = std::make_shared<JoinHash>(
        left, right, mode, join_predicates[0].column_id_pair, join_predicates[0].predicate_condition);
    latest_operator->execute();

    // execute table scans for the following predicates (ColumnVsColumnTableScan)
    for (size_t index = 1; index < join_predicates.size(); ++index) {
      const auto left_column_expr =
          PQPColumnExpression::from_table(*left->get_output(), join_predicates[index].column_id_pair.first);
      const auto right_column_expr =
          PQPColumnExpression::from_table(*right->get_output(), join_predicates[index].column_id_pair.second);
      const auto predicate = std::make_shared<BinaryPredicateExpression>(join_predicates[index].predicate_condition,
                                                                         left_column_expr, right_column_expr);
      latest_operator = std::make_shared<TableScan>(latest_operator, predicate);
      latest_operator->execute();
    }
  }
};

TEST_F(JoinHashMultiplePredicatesTest, MultiPredicateOperatorChain) {
  std::vector<JoinPredicate> join_predicates;
  join_predicates.emplace_back(JoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join_predicates.emplace_back(JoinPredicate{ColumnIDPair{ColumnID{1}, ColumnID{1}}, PredicateCondition::Equals});

  std::chrono::high_resolution_clock::time_point time_point_1 = std::chrono::high_resolution_clock::now();
  // When the implementation of the multi predicate hash join is finished, the following line has to be
  // replaces by the execution of the multi predicate hash join
  execute_multi_predicate_join(_table_1_wrapper, _table_2_wrapper, JoinMode::Inner, join_predicates);
  std::chrono::high_resolution_clock::time_point time_point_2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> time_span =
      std::chrono::duration_cast<std::chrono::duration<double>>(time_point_2 - time_point_1);

  std::cout << "It took me " << time_span.count() << " seconds." << std::endl;
  // TODO(anyone) write the time somewhere
}

}  // namespace opossum
