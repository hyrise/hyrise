#include <cstddef>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "concurrency/transaction_context.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/aggregate_dyod.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/mvcc_data.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "testing_assert.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

class OperatorsAggregateHashTest : public BaseTest {};

TEST_F(OperatorsAggregateHashTest, EmptyHash) {
  // Notably, both the `EmptyAggregateKey` and the empty `AggregateKeySmallVector` have the same hash, so a hash
  // comparison is consistent with comparing all contained keys.
  EXPECT_EQ(std::hash<EmptyAggregateKey>()(EmptyAggregateKey{}), 0);
  EXPECT_EQ(std::hash<AggregateKeySmallVector>()(AggregateKeySmallVector{}), 0);
}

template <typename T>
void test_output(const std::shared_ptr<AbstractOperator> in,
                 const std::vector<std::pair<ColumnID, WindowFunction>>& aggregate_definitions,
                 const std::vector<ColumnID>& groupby_column_ids, const std::string& file_name,
                 const bool test_aggregate_on_reference_table = true) {
  in->never_clear_output();

  // Load expected results from file.
  std::shared_ptr<Table> expected_result = load_table(file_name);

  auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{};
  const auto& table = in->get_output();
  for (const auto& [column_id, aggregate_function] : aggregate_definitions) {
    if (column_id != INVALID_COLUMN_ID) {
      aggregates.emplace_back(std::make_shared<WindowFunctionExpression>(
          aggregate_function,
          pqp_column_(column_id, table->column_data_type(column_id), table->column_name(column_id))));
    } else {
      aggregates.emplace_back(
          std::make_shared<WindowFunctionExpression>(aggregate_function, pqp_column_(column_id, DataType::Long, "*")));
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
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/input.tbl", ChunkOffset{2}));
    _table_wrapper_1_0->never_clear_output();
    _table_wrapper_1_0->execute();

    _table_wrapper_1_0_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/input_null.tbl", ChunkOffset{2}));
    _table_wrapper_1_0_null->never_clear_output();
    _table_wrapper_1_0_null->execute();

    _table_wrapper_1_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input.tbl", ChunkOffset{2}));
    _table_wrapper_1_1->never_clear_output();
    _table_wrapper_1_1->execute();

    _table_wrapper_1_1_large = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_large.tbl", ChunkOffset{2}));
    _table_wrapper_1_1_large->never_clear_output();
    _table_wrapper_1_1_large->execute();

    _table_wrapper_1_1_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", ChunkOffset{2}));
    _table_wrapper_1_1_null->never_clear_output();
    _table_wrapper_1_1_null->execute();

    _table_wrapper_1_2 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/input.tbl", ChunkOffset{2}));
    _table_wrapper_1_2->never_clear_output();
    _table_wrapper_1_2->execute();

    _table_wrapper_2_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/input.tbl", ChunkOffset{2}));
    _table_wrapper_2_1->never_clear_output();
    _table_wrapper_2_1->execute();

    _table_wrapper_2_2 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/input.tbl", ChunkOffset{2}));
    _table_wrapper_2_2->never_clear_output();
    _table_wrapper_2_2->execute();

    _table_wrapper_2_0_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_2gb_0agg/input_null.tbl", ChunkOffset{2}));
    _table_wrapper_2_0_null->never_clear_output();
    _table_wrapper_2_0_null->execute();

    _table_wrapper_3_1 = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/input.tbl", ChunkOffset{2}));
    _table_wrapper_3_1->never_clear_output();
    _table_wrapper_3_1->execute();

    _table_wrapper_3_0_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_3gb_0agg/input_null.tbl", ChunkOffset{2}));
    _table_wrapper_3_0_null->never_clear_output();
    _table_wrapper_3_0_null->execute();

    _table_wrapper_1_1_string = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/input.tbl", ChunkOffset{2}));
    _table_wrapper_1_1_string->never_clear_output();
    _table_wrapper_1_1_string->execute();

    _table_wrapper_1_1_string_null = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/input_null.tbl", ChunkOffset{2}));
    _table_wrapper_1_1_string_null->never_clear_output();
    _table_wrapper_1_1_string_null->execute();

    _table_wrapper_join_1 =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int4.tbl", ChunkOffset{1}));
    _table_wrapper_join_1->never_clear_output();
    _table_wrapper_join_1->execute();

    _table_wrapper_join_2 =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int.tbl", ChunkOffset{1}));
    _table_wrapper_join_2->never_clear_output();
    _table_wrapper_join_2->execute();

    _table_wrapper_2_0_a = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/join_2gb_0agg/input_a.tbl", ChunkOffset{2}));
    _table_wrapper_2_0_a->never_clear_output();
    _table_wrapper_2_0_a->execute();

    _table_wrapper_2_o_b = std::make_shared<TableWrapper>(
        load_table("resources/test_data/tbl/aggregateoperator/join_2gb_0agg/input_b.tbl", ChunkOffset{2}));
    _table_wrapper_2_o_b->never_clear_output();
    _table_wrapper_2_o_b->execute();

    auto test_table =
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input.tbl", ChunkOffset{2});
    ChunkEncoder::encode_all_chunks(test_table);

    _table_wrapper_1_1_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_dict->never_clear_output();
    _table_wrapper_1_1_dict->execute();

    test_table =
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_null.tbl", ChunkOffset{2});
    ChunkEncoder::encode_all_chunks(test_table);

    _table_wrapper_1_1_null_dict = std::make_shared<TableWrapper>(std::move(test_table));
    _table_wrapper_1_1_null_dict->never_clear_output();
    _table_wrapper_1_1_null_dict->execute();

    _table_wrapper_int_int =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int.tbl", ChunkOffset{2}));
    _table_wrapper_int_int->never_clear_output();
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

using AggregateTypes = ::testing::Types<AggregateDYOD, AggregateHash, AggregateSort>;
TYPED_TEST_SUITE(OperatorsAggregateTest, AggregateTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(OperatorsAggregateTest, OperatorName) {
  const auto table = this->_table_wrapper_1_1->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      max_(pqp_column_(ColumnID{1}, table->column_data_type(ColumnID{1}), table->column_name(ColumnID{1})))};
  auto aggregate =
      std::make_shared<TypeParam>(this->_table_wrapper_1_1, aggregate_expressions, std::vector<ColumnID>{ColumnID{0}});

  if constexpr (std::is_same_v<TypeParam, AggregateHash>) {
    EXPECT_EQ(aggregate->name(), "AggregateHash");
  } else if constexpr (std::is_same_v<TypeParam, AggregateSort>) {
    EXPECT_EQ(aggregate->name(), "AggregateSort");
  } else if constexpr (std::is_same_v<TypeParam, AggregateDYOD>) {
    EXPECT_EQ(aggregate->name(), "AggregateDYOD");
  } else {
    Fail("Unknown aggregate type");
  }
}

TYPED_TEST(OperatorsAggregateTest, OperatorDescription) {
  const auto table = this->_table_wrapper_1_1->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      max_(pqp_column_(ColumnID{1}, table->column_data_type(ColumnID{1}), table->column_name(ColumnID{1})))};
  const auto aggregate =
      std::make_shared<TypeParam>(this->_table_wrapper_1_1, aggregate_expressions, std::vector<ColumnID>{ColumnID{0}});
  EXPECT_EQ(aggregate->description(DescriptionMode::SingleLine), aggregate->name() + " GroupBy {Column #0} MAX(b)");
  EXPECT_EQ(aggregate->description(DescriptionMode::MultiLine), aggregate->name() + "\nGroupBy {Column #0}\nMAX(b)");
}

TYPED_TEST(OperatorsAggregateTest, CannotSumStringColumns) {
  const auto table = this->_table_wrapper_1_1_string->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      sum_(pqp_column_(ColumnID{0}, table->column_data_type(ColumnID{0}), table->column_name(ColumnID{0})))};
  auto aggregate = std::make_shared<TypeParam>(this->_table_wrapper_1_1_string, aggregate_expressions,
                                               std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TYPED_TEST(OperatorsAggregateTest, CannotAvgStringColumns) {
  const auto table = this->_table_wrapper_1_1_string->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      avg_(pqp_column_(ColumnID{0}, table->column_data_type(ColumnID{0}), table->column_name(ColumnID{0})))};
  auto aggregate = std::make_shared<TypeParam>(this->_table_wrapper_1_1_string, aggregate_expressions,
                                               std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate->execute(), std::logic_error);
}

TYPED_TEST(OperatorsAggregateTest, CannotStandardDeviationSampleStringColumns) {
  const auto table = this->_table_wrapper_1_1_string->get_output();
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{standard_deviation_sample_(
      pqp_column_(ColumnID{0}, table->column_data_type(ColumnID{0}), table->column_name(ColumnID{0})))};
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
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      any_(pqp_column_(ColumnID{2}, table->column_data_type(ColumnID{2}), table->column_name(ColumnID{2})))};

  auto aggregate =
      std::make_shared<TypeParam>(filtered, aggregate_expressions, std::vector<ColumnID>{ColumnID{0}, ColumnID{1}});
  aggregate->execute();

  // Column 2 stores the value 20 twice for the remaining group.
  EXPECT_EQ(aggregate->get_output()->template get_value<int>(ColumnID{2}, 0u), 20);
}

// Use ANY() on a column with NULL values.
TYPED_TEST(OperatorsAggregateTest, AnyAndNulls) {
  test_output<TypeParam>(this->_table_wrapper_1_0_null, {{ColumnID{0}, WindowFunction::Any}}, {ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/result_any_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, CanCountStringColumns) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{0}, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count_str.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateSum) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSampleLarge) {
  test_output<TypeParam>(this->_table_wrapper_1_1_large, {{ColumnID{1}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_large.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCount) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCountDistinct) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::CountDistinct}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_distinct.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, WindowFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, WindowFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStringMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{0}, WindowFunction::Max}}, {},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/max_str.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStringMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{0}, WindowFunction::Min}}, {},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/min_str.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateSum) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, WindowFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/stddev_samp_null.tbl");
}

TYPED_TEST(OperatorsAggregateTest, StringSingleAggregateCount) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string, {{ColumnID{1}, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, WindowFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, WindowFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateSum) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, WindowFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateCount) {
  test_output<TypeParam>(this->_table_wrapper_1_1_dict, {{ColumnID{1}, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateAvgMax) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Max}, {ColumnID{2}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/max_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMaxStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Max}, {ColumnID{2}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/max_stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Min}, {ColumnID{2}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Min}, {ColumnID{2}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateMinMax) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Min}, {ColumnID{2}, WindowFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/min_max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateAvgAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Avg}, {ColumnID{2}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/avg_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateStandardDeviationSampleStandardDeviationSample) {
  test_output<TypeParam>(
      this->_table_wrapper_1_2,
      {{ColumnID{1}, WindowFunction::StandardDeviationSample}, {ColumnID{2}, WindowFunction::StandardDeviationSample}},
      {ColumnID{0}}, "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/stddev_samp_stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Sum}, {ColumnID{2}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumSum) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Sum}, {ColumnID{2}, WindowFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumCount) {
  test_output<TypeParam>(this->_table_wrapper_1_2,
                         {{ColumnID{1}, WindowFunction::Sum}, {ColumnID{2}, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyMax) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, WindowFunction::Max}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyMin) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, WindowFunction::Min}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbySum) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, WindowFunction::Sum}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAvg) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, WindowFunction::Avg}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/stddev_samp_null.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyCount) {
  test_output<TypeParam>(this->_table_wrapper_2_1, {{ColumnID{2}, WindowFunction::Count}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyMax) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, WindowFunction::Max}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyMin) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, WindowFunction::Min}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbySum) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, WindowFunction::Sum}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyAvg) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, WindowFunction::Avg}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/stddev_samp_null.tbl");
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyCount) {
  test_output<TypeParam>(this->_table_wrapper_3_1, {{ColumnID{2}, WindowFunction::Count}},
                         {ColumnID{0}, ColumnID{1}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_1agg/count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMaxAvg) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, WindowFunction::Max}, {ColumnID{3}, WindowFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/max_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvg) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, WindowFunction::Min}, {ColumnID{3}, WindowFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinMax) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, WindowFunction::Min}, {ColumnID{3}, WindowFunction::Max}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumAvg) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, WindowFunction::Sum}, {ColumnID{3}, WindowFunction::Avg}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateStandardDeviationSampleAvg) {
  test_output<TypeParam>(this->_table_wrapper_2_2,
                         {{ColumnID{2}, WindowFunction::StandardDeviationSample}, {ColumnID{3}, WindowFunction::Avg}},
                         {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/stddev_samp_avg.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateStandardDeviationSampleStandardDeviationSample) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2,
      {{ColumnID{2}, WindowFunction::StandardDeviationSample}, {ColumnID{3}, WindowFunction::StandardDeviationSample}},
      {ColumnID{0}, ColumnID{1}},
      "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/stddev_samp_stddev_samp.tbl", 1);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumSum) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, WindowFunction::Sum}, {ColumnID{3}, WindowFunction::Sum}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateSumCount) {
  test_output<TypeParam>(
      this->_table_wrapper_2_2, {{ColumnID{2}, WindowFunction::Sum}, {ColumnID{3}, WindowFunction::Count}},
      {ColumnID{0}, ColumnID{1}}, "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/sum_count.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateMax) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Max}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/max.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateMin) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Min}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/min.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateSum) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Sum}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/sum.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateAvg) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Avg}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/avg.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateStandardDeviationSample) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::StandardDeviationSample}}, {},
                         "resources/test_data/tbl/aggregateoperator/0gb_1agg/stddev_samp.tbl");
}

TYPED_TEST(OperatorsAggregateTest, NoGroupbySingleAggregateCount) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{ColumnID{1}, WindowFunction::Count}}, {},
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
      std::make_shared<TypeParam>(this->_table_wrapper_1_1_string,
                                  std::vector<std::shared_ptr<WindowFunctionExpression>>{}, std::vector<ColumnID>{}),
      std::logic_error);
}

/**
 * Tests for NULL values
 */
TYPED_TEST(OperatorsAggregateTest, CanCountStringColumnsWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_string_null, {{ColumnID{1}, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_string_1gb_1agg/count_str_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMaxWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, WindowFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateMinWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, WindowFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateSumWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, WindowFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateAvgWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateStandardDeviationSampleWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, SingleAggregateCountWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{ColumnID{1}, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyAndNoAggregateWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_0_null, {}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_0agg/result_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyCountStar) {
  test_output<TypeParam>(this->_table_wrapper_1_1, {{INVALID_COLUMN_ID, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_star.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, OneGroupbyCountStarWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null, {{INVALID_COLUMN_ID, WindowFunction::Count}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/count_star_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyCountStar) {
  test_output<TypeParam>(this->_table_wrapper_2_0_null, {{INVALID_COLUMN_ID, WindowFunction::Count}},
                         {ColumnID{0}, ColumnID{2}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_0agg/count_star.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, ThreeGroupbyCountStar) {
  test_output<TypeParam>(this->_table_wrapper_3_0_null, {{INVALID_COLUMN_ID, WindowFunction::Count}},
                         {ColumnID{0}, ColumnID{2}, ColumnID{3}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_3gb_0agg/count_star.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMaxWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, WindowFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMinWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, WindowFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateSumWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, WindowFunction::Sum}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateAvgWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, WindowFunction::Avg}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateStandardDeviationSampleWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, WindowFunction::StandardDeviationSample}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/stddev_samp_null.tbl", false);
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateCountWithNull) {
  test_output<TypeParam>(this->_table_wrapper_1_1_null_dict, {{ColumnID{1}, WindowFunction::Count}}, {ColumnID{0}},
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
                         {{ColumnID{1}, WindowFunction::Max},
                          {ColumnID{2}, WindowFunction::Count},
                          {INVALID_COLUMN_ID, WindowFunction::Count}},
                         {}, "resources/test_data/tbl/aggregateoperator/0gb_3agg/max_count_count_empty.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateEmptyTableGrouped) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), 0));
  filtered->execute();
  test_output<TypeParam>(filtered,
                         {{ColumnID{1}, WindowFunction::Max},
                          {ColumnID{2}, WindowFunction::Count},
                          {INVALID_COLUMN_ID, WindowFunction::Count}},
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

  test_output<TypeParam>(filtered, {{ColumnID{1}, WindowFunction::Max}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbyAndTwoAggregateMinAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_2_2, less_than_(get_column_expression(this->_table_wrapper_2_2, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{2}, WindowFunction::Min}, {ColumnID{3}, WindowFunction::Avg}},
                         {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_2agg/min_avg_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoGroupbySumOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_2_1, less_than_(get_column_expression(this->_table_wrapper_2_1, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{2}, WindowFunction::Sum}}, {ColumnID{0}, ColumnID{1}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_2gb_1agg/sum_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateSumAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, WindowFunction::Sum}, {ColumnID{2}, WindowFunction::Avg}},
                         {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/sum_avg_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, TwoAggregateStandardDeviationSampleAvgOnRef) {
  auto filtered = std::make_shared<TableScan>(
      this->_table_wrapper_1_2, less_than_(get_column_expression(this->_table_wrapper_1_2, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(
      filtered, {{ColumnID{1}, WindowFunction::StandardDeviationSample}, {ColumnID{2}, WindowFunction::Avg}},
      {ColumnID{0}}, "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_2agg/stddev_samp_avg_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateMinOnRef) {
  auto filtered =
      std::make_shared<TableScan>(this->_table_wrapper_1_1_dict,
                                  less_than_(get_column_expression(this->_table_wrapper_1_1_dict, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, WindowFunction::Min}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateAnyOnRef) {
  auto filtered =
      std::make_shared<TableScan>(this->_table_wrapper_1_1_dict,
                                  less_than_(get_column_expression(this->_table_wrapper_1_1_dict, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, WindowFunction::Any}}, {ColumnID{0}},
                         "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/any_filtered.tbl");
}

TYPED_TEST(OperatorsAggregateTest, DictionarySingleAggregateStandardDeviationSampleOnRef) {
  auto filtered =
      std::make_shared<TableScan>(this->_table_wrapper_1_1_dict,
                                  less_than_(get_column_expression(this->_table_wrapper_1_1_dict, ColumnID{0}), "100"));
  filtered->execute();

  test_output<TypeParam>(filtered, {{ColumnID{1}, WindowFunction::StandardDeviationSample}}, {ColumnID{0}},
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

  test_output<TypeParam>(join, {{ColumnID{1}, WindowFunction::Min}}, {ColumnID{0}},
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
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{};
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

TYPED_TEST(OperatorsAggregateTest, DYODStringGroupByWithNullsAndLongStrings) {
  // Regression test: a NULL in a string GROUP BY column must not desync the materialized string pointers of the
  // long (> PREFIX_LENGTH) strings that follow it. The long strings additionally share their inline length and
  // prefix, so distinguishing them also exercises the full-string equality path. NULL forms its own group.
  const auto values = pmr_vector<pmr_string>{
      "longstringprefix_a", "", "longstringprefix_b", "", "longstringprefix_a", "short", "", "longstringprefix_c"};
  const auto nulls = pmr_vector<bool>{false, true, false, true, false, false, true, false};

  auto values_copy = values;
  auto nulls_copy = nulls;
  const auto value_segment = std::make_shared<ValueSegment<pmr_string>>(std::move(values_copy), std::move(nulls_copy));

  const auto table_definitions = TableColumnDefinitions{{"a", DataType::String, true}};
  const auto table = std::make_shared<Table>(table_definitions, TableType::Data);
  table->append_chunk({value_segment});

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  // No aggregate expressions, i.e., aggregate acts as DISTINCT.
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{};
  const auto aggregate =
      std::make_shared<TypeParam>(table_wrapper, aggregate_expressions, std::vector<ColumnID>{ColumnID{0}});
  aggregate->execute();

  const auto& result = aggregate->get_output();

  auto distinct_non_null = std::set<pmr_string>{};
  auto null_group_count = size_t{0};
  for (auto row_number = size_t{0}; row_number < result->row_count(); ++row_number) {
    const auto value = result->template get_value<pmr_string>(ColumnID{0}, row_number);
    if (value) {
      distinct_non_null.insert(*value);
    } else {
      ++null_group_count;
    }
  }

  const auto expected = std::set<pmr_string>{"longstringprefix_a", "longstringprefix_b", "longstringprefix_c", "short"};
  EXPECT_EQ(distinct_non_null, expected);
  EXPECT_EQ(null_group_count, size_t{1});
}

TYPED_TEST(OperatorsAggregateTest, DYODLowCardinalityMultiColumnGroupBy) {
  auto int_values = pmr_vector<int32_t>{1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2};
  auto string_values = pmr_vector<pmr_string>{
      "longstringprefix_a", "longstringprefix_a", "longstringprefix_a", "longstringprefix_a", "", "", "", "",
      "longstringprefix_b", "longstringprefix_b", "longstringprefix_b", "longstringprefix_b"};
  auto string_nulls = pmr_vector<bool>{false, false, false, false, true, true, true, true, false, false, false, false};

  const auto int_segment = std::make_shared<ValueSegment<int32_t>>(std::move(int_values));
  const auto string_segment =
      std::make_shared<ValueSegment<pmr_string>>(std::move(string_values), std::move(string_nulls));
  const auto table = std::make_shared<Table>(
      TableColumnDefinitions{{"i", DataType::Int, false}, {"s", DataType::String, true}}, TableType::Data);
  table->append_chunk({int_segment, string_segment});

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  const auto aggregate =
      std::make_shared<TypeParam>(table_wrapper, std::vector<std::shared_ptr<WindowFunctionExpression>>{},
                                  std::vector<ColumnID>{ColumnID{0}, ColumnID{1}});
  aggregate->execute();

  auto groups = std::set<std::pair<int32_t, pmr_string>>{};
  const auto& result = aggregate->get_output();
  for (auto row = size_t{0}; row < result->row_count(); ++row) {
    const auto value = result->template get_value<pmr_string>(ColumnID{1}, row);
    groups.emplace(*result->template get_value<int32_t>(ColumnID{0}, row), value.value_or(pmr_string{"<NULL>"}));
  }

  const auto expected =
      std::set<std::pair<int32_t, pmr_string>>{{1, "longstringprefix_a"}, {1, "<NULL>"}, {2, "longstringprefix_b"}};
  EXPECT_EQ(groups, expected);
}

TYPED_TEST(OperatorsAggregateTest, DYODNonNullableGroupByDropsNullBitmap) {
  // When no GROUP BY column is nullable, the materialized rows omit the null bitmap entirely. Verify that grouping,
  // hashing, and equality still work across the shorter row layout, including long (> PREFIX_LENGTH) strings that
  // share their inline length and prefix (forcing the full-string equality path).
  const auto int_values = pmr_vector<int32_t>{1, 2, 1, 2, 1, 3};
  const auto str_values = pmr_vector<pmr_string>{"longstringprefix_a", "longstringprefix_b", "longstringprefix_a",
                                                 "longstringprefix_b", "longstringprefix_a", "short"};
  const auto int_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{int_values});
  const auto str_segment = std::make_shared<ValueSegment<pmr_string>>(pmr_vector<pmr_string>{str_values});

  const auto table_definitions = TableColumnDefinitions{{"i", DataType::Int, false}, {"s", DataType::String, false}};
  const auto table = std::make_shared<Table>(table_definitions, TableType::Data);
  table->append_chunk({int_segment, str_segment});

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  // No aggregate expressions, i.e., aggregate acts as DISTINCT.
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{};
  const auto aggregate = std::make_shared<TypeParam>(table_wrapper, aggregate_expressions,
                                                     std::vector<ColumnID>{ColumnID{0}, ColumnID{1}});
  aggregate->execute();

  const auto& result = aggregate->get_output();

  auto groups = std::set<std::pair<int32_t, pmr_string>>{};
  for (auto row_number = size_t{0}; row_number < result->row_count(); ++row_number) {
    groups.emplace(*result->template get_value<int32_t>(ColumnID{0}, row_number),
                   *result->template get_value<pmr_string>(ColumnID{1}, row_number));
  }

  const auto expected =
      std::set<std::pair<int32_t, pmr_string>>{{1, "longstringprefix_a"}, {2, "longstringprefix_b"}, {3, "short"}};
  EXPECT_EQ(groups, expected);
}

TYPED_TEST(OperatorsAggregateTest, FilteredDictionary) {
  const auto table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}},
                              TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);

  table->append({int32_t{0}, int32_t{0}});
  table->append({int32_t{0}, int32_t{1}});
  table->append({int32_t{0}, int32_t{2}});
  table->append({int32_t{1}, int32_t{0}});
  table->append({int32_t{1}, int32_t{1}});
  table->append({int32_t{1}, int32_t{2}});

  table->last_chunk()->set_immutable();
  table->last_chunk()->mvcc_data()->set_end_cid(ChunkOffset{0}, CommitID{1});
  table->last_chunk()->mvcc_data()->set_end_cid(ChunkOffset{2}, CommitID{1});
  table->last_chunk()->mvcc_data()->set_end_cid(ChunkOffset{3}, CommitID{1});
  table->last_chunk()->mvcc_data()->set_end_cid(ChunkOffset{5}, CommitID{1});
  table->last_chunk()->mvcc_data()->max_end_cid = CommitID{0};
  table->last_chunk()->increase_invalid_row_count(ChunkOffset{4});

  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  const auto validate = std::make_shared<Validate>(table_wrapper);
  const auto transaction_context = std::make_shared<TransactionContext>(TransactionID{1}, CommitID{2}, AutoCommit::Yes);
  validate->set_transaction_context(transaction_context);
  validate->never_clear_output();
  table_wrapper->execute();
  validate->execute();

  const auto a = PQPColumnExpression::from_table(*validate->get_output(), "a");
  const auto b = PQPColumnExpression::from_table(*validate->get_output(), "b");
  const auto aggregate_expressions = std::vector<std::shared_ptr<WindowFunctionExpression>>{min_(b), max_(b)};

  {
    // Case i: No grouping.
    const auto expected_result = std::make_shared<Table>(
        TableColumnDefinitions{{"MIN(b)", DataType::Int, true}, {"MAX(b)", DataType::Int, true}}, TableType::Data);
    expected_result->append({int32_t{1}, int32_t{1}});

    const auto aggregate = std::make_shared<TypeParam>(validate, aggregate_expressions, std::vector<ColumnID>{});
    aggregate->execute();
    EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
  }

  {
    // Case ii: Grouping.
    const auto expected_result = std::make_shared<Table>(
        TableColumnDefinitions{
            {"a", DataType::Int, false}, {"MIN(b)", DataType::Int, true}, {"MAX(b)", DataType::Int, true}},
        TableType::Data);
    expected_result->append({int32_t{0}, int32_t{1}, int32_t{1}});
    expected_result->append({int32_t{1}, int32_t{1}, int32_t{1}});

    const auto aggregate =
        std::make_shared<TypeParam>(validate, aggregate_expressions, std::vector<ColumnID>{ColumnID{0}});
    aggregate->execute();
    EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
  }
}

}  // namespace hyrise
