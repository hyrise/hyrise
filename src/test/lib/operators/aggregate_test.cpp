#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/aggregate_expression.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
void test_output(const std::shared_ptr<AbstractOperator> in,
                 const std::vector<std::pair<ColumnID, AggregateFunction>>& aggregate_definitions,
                 const std::vector<ColumnID>& groupby_column_ids, const std::string& file_name,
                 const bool test_aggregate_on_reference_table = true) {
  // Load expected results from file.
  std::shared_ptr<Table> expected_result = load_table(file_name);

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{};
  const auto& table = in->get_output();
  for (const auto& [column_id, aggregate_function] : aggregate_definitions) {
    if (column_id != INVALID_COLUMN_ID) {
      aggregates.emplace_back(std::make_shared<AggregateExpression>(
          aggregate_function, pqp_column_(column_id, table->column_data_type(column_id),
                                          table->column_is_nullable(column_id), table->column_name(column_id))));
    } else {
      aggregates.emplace_back(std::make_shared<AggregateExpression>(
          aggregate_function, pqp_column_(column_id, DataType::Long, false, "*")));
    }
  }

  {
    // Test the Aggregate on stored table data.
    auto aggregate = std::make_shared<T>(in, aggregates, groupby_column_ids);
    aggregate->execute();
    EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
  }

  if (test_aggregate_on_reference_table) {
    // Perform a TableScan to create a reference table
    const auto table_scan = std::make_shared<TableScan>(in, greater_than_(get_column_expression(in, ColumnID{0}), 0));
    table_scan->execute();

    // Perform the Aggregate on a reference table
    const auto aggregate = std::make_shared<T>(table_scan, aggregates, groupby_column_ids);
    aggregate->execute();
    EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
  }
}

template <typename T>
class OperatorsAggregateTest : public BaseTest {
 public:
  static void SetUpTestCase() {  // called ONCE before the tests
    _table_wrapper_1_0 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/input.tbl", 2));
    _table_wrapper_1_0->execute();

    _table_wrapper_1_0_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", 2));
    _table_wrapper_1_0_null->execute();

    _table_wrapper_1_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2));
    _table_wrapper_1_1->execute();

    _table_wrapper_1_1_large = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_large.tbl", 2));
    _table_wrapper_1_1_large->execute();

    _table_wrapper_1_1_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 2));
    _table_wrapper_1_1_null->execute();

    _table_wrapper_1_2 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/input.tbl", 2));
    _table_wrapper_1_2->execute();

    _table_wrapper_2_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/input.tbl", 2));
    _table_wrapper_2_1->execute();

    _table_wrapper_2_2 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/input.tbl", 2));
    _table_wrapper_2_2->execute();

    _table_wrapper_2_0_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_0agg/input_null.tbl", 2));
    _table_wrapper_2_0_null->execute();

    _table_wrapper_3_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/input.tbl", 2));
    _table_wrapper_3_1->execute();

    _table_wrapper_3_0_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_3gb_0agg/input_null.tbl", 2));
    _table_wrapper_3_0_null->execute();

    _table_wrapper_1_1_string = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/input.tbl", 2));
    _table_wrapper_1_1_string->execute();

    _table_wrapper_1_1_string_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/input_null.tbl", 2));
    _table_wrapper_1_1_string_null->execute();

    _table_wrapper_join_1 = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int4.tbl", 1));
    _table_wrapper_join_1->execute();

    _table_wrapper_join_2 = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int.tbl", 1));
    _table_wrapper_join_2->execute();

    _table_wrapper_2_0_a = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/join_2gb_0agg/input_a.tbl", 2));
    _table_wrapper_2_0_a->execute();

    _table_wrapper_2_o_b = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/join_2gb_0agg/input_b.tbl", 2));
    _table_wrapper_2_o_b->execute();

    auto test_table = load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    ChunkEncoder::encode_all_chunks(test_table);

    _table_wrapper_1_1_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_dict->execute();

    test_table = load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", 2);
    ChunkEncoder::encode_all_chunks(test_table);

    _table_wrapper_1_1_null_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_null_dict->execute();

    _table_wrapper_int_int = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int.tbl", 2));
    _table_wrapper_int_int->execute();
  }

 protected:
  void SetUp() override {}

  inline static std::shared_ptr<TableWrapper> _table_wrapper_1_0, _table_wrapper_1_0_null, _table_wrapper_1_1,
      _table_wrapper_1_1_null, _table_wrapper_1_1_large, _table_wrapper_join_1, _table_wrapper_join_2,
      _table_wrapper_1_2, _table_wrapper_2_1, _table_wrapper_2_2, _table_wrapper_2_0_null, _table_wrapper_3_1,
      _table_wrapper_3_2, _table_wrapper_3_0_null, _table_wrapper_1_1_string, _table_wrapper_1_1_string_null,
      _table_wrapper_1_1_dict, _table_wrapper_1_1_null_dict, _table_wrapper_2_0_a, _table_wrapper_2_o_b,
      _table_wrapper_int_int;
};

using AggregateTypes = ::testing::Types<AggregateHash, AggregateSort>;
TYPED_TEST_SUITE(OperatorsAggregateTest, AggregateTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(OperatorsAggregateTest, OperatorName) {
  const auto table = this->_table_wrapper_1_1->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      max_(pqp_column_(ColumnID{1}, table->column_data_type(ColumnID{1}), table->column_is_nullable(ColumnID{1}),
                       table->column_name(ColumnID{1})))};
  auto aggregate =
      std::make_shared<TypeParam>(this->_table_wrapper_1_1, aggregate_expressions, std::vector<ColumnID>{ColumnID{0}});

  if constexpr (std::is_same_v<TypeParam, AggregateHash>) {
    EXPECT_EQ(aggregate->name(), "AggregateHash");
  } else if constexpr (std::is_same_v<TypeParam, AggregateSort>) {
    EXPECT_EQ(aggregate->name(), "AggregateSort");
  } else {
    Fail("Unknown aggregate type");
  }
}

TYPED_TEST(OperatorsAggregateTest, CannotSumStringColumns) {
  const auto table = this->_table_wrapper_1_1_string->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      sum_(pqp_column_(ColumnID{0}, table->column_data_type(ColumnID{0}), table->column_is_nullable(ColumnID{0}),
                       table->column_name(ColumnID{0})))};
  auto aggregate = std::make_shared<TypeParam>(this->_table_wrapper_1_1_string, aggregate_expressions,
                                               std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TYPED_TEST(OperatorsAggregateTest, CannotAvgStringColumns) {
  const auto table = this->_table_wrapper_1_1_string->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      avg_(pqp_column_(ColumnID{0}, table->column_data_type(ColumnID{0}), table->column_is_nullable(ColumnID{0}),
                       table->column_name(ColumnID{0})))};
  auto aggregate = std::make_shared<TypeParam>(this->_table_wrapper_1_1_string, aggregate_expressions,
                                               std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TYPED_TEST(OperatorsAggregateTest, CannotStandardDeviationSampleStringColumns) {
  const auto table = this->_table_wrapper_1_1_string->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      standard_deviation_sample_(pqp_column_(ColumnID{0}, table->column_data_type(ColumnID{0}),
                                             table->column_is_nullable(ColumnID{0}), table->column_name(ColumnID{0})))};
  auto aggregate = std::make_shared<TypeParam>(this->_table_wrapper_1_1_string, aggregate_expressions,
                                               std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

// The ANY aggregation is a special case which is used to obtain "any value" of a group of which we know that each
// value in this group is the same (for most cases, the group will have a size of one). This can be the case, when
// the aggregated column is functionally dependent on the group-by columns.
TYPED_TEST(OperatorsAggregateTest, AnyOnGroupWithMultipleEntries) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_2_2, equals_(get_column_expression(this->_table_wrapper_2_2, ColumnID{0}), 123));
  filtered->execute();

  const auto table = this->_table_wrapper_2_2->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      any_(pqp_column_(ColumnID{2}, table->column_data_type(ColumnID{2}), table->column_is_nullable(ColumnID{2}),
                       table->column_name(ColumnID{2})))};

  auto aggregate =
      std::make_shared<TypeParam>(filtered, aggregate_expressions, std::vector<ColumnID>{ColumnID{0}, ColumnID{1}});
  aggregate->execute();

  // Column 2 stores the value 20 twice for the remaining group.
  EXPECT_EQ(aggregate->get_output()->template get_value<int>(ColumnID{2}, 0u), 20);
}

// Use ANY() on a column with NULL values.
TYPED_TEST(OperatorsAggregateTest, AnyAndNulls) {
  test_output<TypeParam>(this->_table_wrapper_1_0_null, {{ColumnID{0}, AggregateFunction::Any}}, {ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/result_any_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, CanCountStringColumns) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count_str.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateSum) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSampleLarge) {
  test_output<TypeParam>(this->_table_wrapper_1_1_large, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_large.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCount) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCountDistinct) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::CountDistinct}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_distinct.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStringMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Max}}, {},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/max_str.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStringMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{0}, AggregateFunction::Min}}, {},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/min_str.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateSum) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/stddev_samp_null.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateCount) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateSum) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateCount) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateAvgMax) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, AggregateFunction::Max}, {ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/max_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMaxStandardDeviationSample) {
  test_output<TypeParam>(
      this->_table_wrapper_1_2,
      {{ColumnID{1}, AggregateFunction::Max}, {ColumnID{2}, AggregateFunction::StandardDeviationSample}}, {ColumnID{0}},
      "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/max_stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, AggregateFunction::Min}, {ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinStandardDeviationSample) {
  test_output<TypeParam>(
      this->_table_wrapper_1_2,
      {{ColumnID{1}, AggregateFunction::Min}, {ColumnID{2}, AggregateFunction::StandardDeviationSample}}, {ColumnID{0}},
      "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinMax) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, AggregateFunction::Min}, {ColumnID{2}, AggregateFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateAvgAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, AggregateFunction::Avg}, {ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/avg_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateStandardDeviationSampleStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, AggregateFunction::StandardDeviationSample},
                          {ColumnID{2}, AggregateFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/stddev_samp_stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumSum) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumCount) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Count}},
                         {ColumnID{0}}, "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyMax) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Max}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyMin) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Min}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbySum) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Sum}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAvg) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Avg}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::StandardDeviationSample}},
                         {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/stddev_samp_null.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyCount) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, AggregateFunction::Count}},
                         {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyMax) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Max}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyMin) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Min}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbySum) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Sum}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyAvg) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Avg}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::StandardDeviationSample}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/stddev_samp_null.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyCount) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, AggregateFunction::Count}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMaxAvg) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Max}, {ColumnID{3}, AggregateFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvg) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinMax) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Max}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumAvg) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateStandardDeviationSampleAvg) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2,
      {{ColumnID{2}, AggregateFunction::StandardDeviationSample}, {ColumnID{3}, AggregateFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/stddev_samp_avg.tbl",
      1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateStandardDeviationSampleStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_2_2,
                         {{ColumnID{2}, AggregateFunction::StandardDeviationSample},
                          {ColumnID{3}, AggregateFunction::StandardDeviationSample}},
                         {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/stddev_samp_stddev_samp.tbl",
                         1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumSum) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Sum}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumCount) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, AggregateFunction::Sum}, {ColumnID{3}, AggregateFunction::Count}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Max}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Min}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateSum) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Sum}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Avg}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateCount) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, AggregateFunction::Count}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyAndNoAggregate) {
  test_output<TypeParam>(this->_table_wrapper_1_0, {}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/result.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndNoAggregate) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_0agg/result.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbyAndNoAggregate) {
  EXPECT_THROW(
      std::make_shared<TypeParam>(this->_table_wrapper_1_1_string, std::vector<std::shared_ptr<AggregateExpression>>{},
                                  std::vector<ColumnID>{}),
      std::logic_error);
}

/**
 * Tests for NULL values
 */
TYPED_TEST(OperatorsAggregateTest, CanCountStringColumnsWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string_null, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count_str_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMaxWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMinWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateSumWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateAvgWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSampleWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCountWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyAndNoAggregateWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_0_null, {}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/result_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyCountStar) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{INVALID_COLUMN_ID, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_star.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyCountStarWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{INVALID_COLUMN_ID, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_star_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyCountStar) {
  test_output<TypeParam>(this->_table_wrapper_2_0_null, {{INVALID_COLUMN_ID, AggregateFunction::Count}},
                         {ColumnID{0}, ColumnID{2}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_0agg/count_star.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyCountStar) {
  test_output<TypeParam>(this->_table_wrapper_3_0_null, {{INVALID_COLUMN_ID, AggregateFunction::Count}},
                         {ColumnID{0}, ColumnID{2}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_0agg/count_star.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMaxWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMinWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateSumWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateAvgWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateStandardDeviationSampleWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict,
                         {{ColumnID{1}, AggregateFunction::StandardDeviationSample}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateCountWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, AggregateFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", false);
}

/**
 * Tests for empty tables
 */

TYPED_TEST(OperatorsAggregateTest, TwoAggregateEmptyTable) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), 0));
  filtered->execute();
  test_output<TypeParam>(filtered,
                         {{ColumnID{1}, AggregateFunction::Max},
                          {ColumnID{2}, AggregateFunction::Count},
                          {INVALID_COLUMN_ID, AggregateFunction::Count}},
                         {}, "resources/test_data/tbl/aggregateoperator/0gb_3agg/max_count_count_empty.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateEmptyTableGrouped) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), 0));
  filtered->execute();
  test_output<TypeParam>(filtered,
                         {{ColumnID{1}, AggregateFunction::Max},
                          {ColumnID{2}, AggregateFunction::Count},
                          {INVALID_COLUMN_ID, AggregateFunction::Count}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_3agg/max_count_count_empty.tbl");
}

/**
 * Tests for ReferenceSegments
 */

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMaxOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_1, less_than_(get_column_expression(this->_table_wrapper_1_1, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_2_2, less_than_(get_column_expression(this->_table_wrapper_2_2, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{2}, AggregateFunction::Min}, {ColumnID{3}, AggregateFunction::Avg}},
                         {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_avg_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbySumOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_2_1, less_than_(get_column_expression(this->_table_wrapper_2_1, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{2}, AggregateFunction::Sum}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/sum_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, AggregateFunction::Sum}, {ColumnID{2}, AggregateFunction::Avg}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_avg_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateStandardDeviationSampleAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(
      filtered, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}, {ColumnID{2}, AggregateFunction::Avg}},
      {ColumnID{0}}, "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/stddev_samp_avg_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMinOnRef) {
  auto filtered =
      std::make_shared<TableScan>(this->_table_wrapper_1_1_dict,
                                  less_than_(get_column_expression(this->_table_wrapper_1_1_dict, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateAnyOnRef) {
  auto filtered =
      std::make_shared<TableScan>(this->_table_wrapper_1_1_dict,
                                  less_than_(get_column_expression(this->_table_wrapper_1_1_dict, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, AggregateFunction::Any}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/any_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateStandardDeviationSampleOnRef) {
  auto filtered =
      std::make_shared<TableScan>(this->_table_wrapper_1_1_dict,
                                  less_than_(get_column_expression(this->_table_wrapper_1_1_dict, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, AggregateFunction::StandardDeviationSample}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, JoinThenAggregate) {
  auto join = std::make_shared<JoinHash>(
      this->_table_wrapper_2_0_a, this->_table_wrapper_2_o_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
  join->execute();

  test_output<TypeParam>(join, {}, {ColumnID{0}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/join_2gb_0agg/result.tbl");
}

TYPED_TEST(OperatorsAggregateTest, OuterJoinThenAggregate) {
  auto join =
      std::make_shared<JoinNestedLoop>(this->_table_wrapper_join_1, this->_table_wrapper_join_2, JoinMode::FullOuter,
                                       OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThan});
  join->execute();

  test_output<TypeParam>(join, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/outer_join.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, StringVariations) {
  // Check that different strings in the GROUP BY column are treated correctly even in the presence of optimizations.
  // Not using a tbl file as expressing edge cases like "\0" feels safer in C++ code than in tbl files.
  const auto values = pmr_vector<pmr_string>{"",
                                             {"\0", 1},
                                             {"\0\0", 2},
                                             {"\0\0\0", 3},
                                             {"\0\0\0\0", 4},
                                             "a",
                                             {"a\0", 2},
                                             "aa",
                                             "ab",
                                             {"a\0\0", 3},
                                             {"a\0b", 3},
                                             {"aa\0", 3},
                                             {"ab\0", 3},
                                             "abc",
                                             "aaa",
                                             {"a\0\0\0", 4},
                                             {"a\0b\0", 4},
                                             {"abc\0", 4},
                                             "abcd",
                                             "aaaa",
                                             {"\xff", 1},
                                             {"\xff\xff", 2},
                                             {"\xff\xff\xff", 3},
                                             {"\xff\xff\xff\xff", 4},
                                             {"\0\0\0\0\0", 5},
                                             {"\xff\xff\xff\xff\xff", 5},
                                             "hello",
                                             {"abcd\0", 5},
                                             "alongstring",
                                             "anotherlongstring"};

  auto values_copy = values;
  const auto value_segment = std::make_shared<ValueSegment<pmr_string>>(std::move(values_copy));

  const auto table_definitions = TableColumnDefinitions{{"a", DataType::String, true}};
  const auto table = std::make_shared<Table>(table_definitions, TableType::Data);
  table->append_chunk({value_segment});

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  // No aggregate expressions, i.e., aggregate acts as DISTINCT
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{};
  const auto aggregate =
      std::make_shared<TypeParam>(table_wrapper, aggregate_expressions, std::vector<ColumnID>{ColumnID{0}});
  aggregate->execute();

  const auto& result = aggregate->get_output();

  auto result_values = std::vector<pmr_string>{};
  for (auto row_number = size_t{0}; row_number < result->row_count(); ++row_number) {
    result_values.emplace_back(*result->template get_value<pmr_string>(ColumnID{0}, row_number));
  }

  const auto values_sorted = std::set<pmr_string>(values.begin(), values.end());
  const auto result_values_sorted = std::set<pmr_string>(result_values.begin(), result_values.end());
  EXPECT_EQ(values_sorted, result_values_sorted);
}

}  // namespace opossum
