#include <base_test.hpp>
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

    for (auto index = size_t{0}; index < _table_1_size; ++index) {
      _table_1->append({static_cast<int>(index % 2), static_cast<int>(index % 8)});
    }
    for (auto index = size_t{0}; index < _table_1_size; ++index) {
      _table_2->append({static_cast<int>(index % 4), static_cast<int>(index % 16)});
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

  inline static size_t _table_1_size = 0;
  inline static size_t _table_2_size = 0;
  inline static std::shared_ptr<Table> _table_1;
  inline static std::shared_ptr<Table> _table_2;
  inline static std::shared_ptr<TableWrapper> _table_1_wrapper;
  inline static std::shared_ptr<TableWrapper> _table_2_wrapper;
};

TEST_F(JoinHashMultiplePredicatesTest, ChronoTest) {
  std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

  std::cout << "printing out 1000 stars...\n";
  for (int i = 0; i < 1000; ++i) std::cout << "*";
  std::cout << std::endl;

  std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);

  std::cout << "It took me " << time_span.count() << " seconds.";
  std::cout << std::endl;
}

TEST_F(JoinHashMultiplePredicatesTest, MultiPredicateOperatorChain) {
  auto join = std::make_shared<JoinHash>(_table_1_wrapper, _table_2_wrapper, JoinMode::Inner,
                                         ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);

  join->execute();
  // preparation for table scan
  const auto equals = PredicateCondition::Equals;
  // build column expression based on result table of the hash join
  const auto left_operand = get_column_expression(join, ColumnID{0});
  const auto right_operand = PQPColumnExpression::from_table(*_table_2, ColumnID{1});
  const auto predicate = std::make_shared<BinaryPredicateExpression>(equals, left_operand, right_operand);

  auto table_scan = std::make_shared<TableScan>(join->get_output(), predicate);
}

}  // namespace opossum
