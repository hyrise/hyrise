#include "base_test.hpp"
#include "gtest/gtest.h"
#include "operators/table_scan/sorted_segment_search.hpp"
#include "storage/segment_iterate.hpp"

namespace opossum {

using TestData = std::tuple<std::string, PredicateCondition, int32_t, std::vector<int32_t>>;
using Params = std::tuple<TestData, OrderByMode, bool>;

namespace {

auto formatter = [](const ::testing::TestParamInfo<Params> info) {
  return order_by_mode_to_string.at(std::get<1>(info.param)) + std::get<0>(std::get<0>(info.param)) +
         (std::get<2>(info.param) ? "WithNulls" : "WithoutNulls");
};

}  // namespace

class OperatorsTableScanSortedSegmentSearchTest : public BaseTest, public ::testing::WithParamInterface<Params> {
 protected:
  void SetUp() override {
    bool nullable;
    TestData test_data;
    std::tie(test_data, _order_by, nullable) = GetParam();
    std::tie(std::ignore, _predicate_condition, _search_value, _expected) = test_data;

    const bool ascending = _order_by == OrderByMode::Ascending || _order_by == OrderByMode::AscendingNullsLast;
    const bool nulls_first = _order_by == OrderByMode::Ascending || _order_by == OrderByMode::Descending;

    if (!ascending) {
      std::reverse(_expected.begin(), _expected.end());
    }

    _segment = std::make_unique<ValueSegment<int32_t>>(nullable);

    if (nullable && nulls_first) {
      _segment->append(NULL_VALUE);
      _segment->append(NULL_VALUE);
      _segment->append(NULL_VALUE);
    }

    const auto table_size = 10;
    for (int32_t row = 0; row < table_size; ++row) {
      _segment->append(ascending ? row : table_size - row - 1);
    }

    if (nullable && !nulls_first) {
      _segment->append(NULL_VALUE);
      _segment->append(NULL_VALUE);
      _segment->append(NULL_VALUE);
    }
  }

  std::unique_ptr<ValueSegment<int32_t>> _segment;
  PredicateCondition _predicate_condition;
  int32_t _search_value;
  std::vector<int32_t> _expected;
  OrderByMode _order_by;
};

INSTANTIATE_TEST_CASE_P(
    Predicates, OperatorsTableScanSortedSegmentSearchTest,
    ::testing::Combine(
        ::testing::Values(
            // The following rows specify the different testcases.
            // All of them run on a table that consists of the values from 0 to 9.
            // The parameters work like this:
            // 1. predicate condition to use
            // 2. value to compare
            // 3. expected result
            //
            // Each row is tested with all four sorted orders and nullable or non-nullable segments. For descending
            // sort orders, the segments contain the values from 9 to 0 and the expected result is reversed, so that
            // you only need to specify the expected result in ascending order.
            TestData("Equals", PredicateCondition::Equals, 5, {5}),
            TestData("NotEqualsAllMatch", PredicateCondition::NotEquals, 42, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
            TestData("NotEquals2Ranges", PredicateCondition::NotEquals, 5, {0, 1, 2, 3, 4, 6, 7, 8, 9}),
            TestData("NotEqualsOnlyFirstRange", PredicateCondition::NotEquals, 9, {0, 1, 2, 3, 4, 5, 6, 7, 8}),
            TestData("NotEqualsOnlySecondRange", PredicateCondition::NotEquals, 0, {1, 2, 3, 4, 5, 6, 7, 8, 9}),
            TestData("LessThan", PredicateCondition::LessThan, 5, {0, 1, 2, 3, 4}),
            TestData("LessThanEquals", PredicateCondition::LessThanEquals, 5, {0, 1, 2, 3, 4, 5}),
            TestData("GreaterThan", PredicateCondition::GreaterThan, 5, {6, 7, 8, 9}),
            TestData("GreaterThanEquals", PredicateCondition::GreaterThanEquals, 5, {5, 6, 7, 8, 9})),
        ::testing::Values(OrderByMode::Ascending, OrderByMode::AscendingNullsLast, OrderByMode::Descending,
                          OrderByMode::DescendingNullsLast),
        ::testing::Bool()),  // nullable
    formatter);

TEST_P(OperatorsTableScanSortedSegmentSearchTest, ScanSortedSegment) {
  const auto iterable = create_iterable_from_segment(*_segment);
  iterable.with_iterators([&](auto input_begin, auto input_end) {
    scan_sorted_segment(input_begin, input_end, _order_by, _predicate_condition, _search_value,
                        [&](auto output_begin, auto output_end) {
                          ASSERT_EQ(std::distance(output_begin, output_end), _expected.size());

                          size_t index = 0;
                          for (; output_begin < output_end; ++output_begin, ++index) {
                            ASSERT_FALSE(output_begin->is_null()) << "row " << index << " is null";
                            ASSERT_EQ(output_begin->value(), _expected[index]) << "row " << index << " is invalid";
                          }
                        });
  });
}

}  // namespace opossum
