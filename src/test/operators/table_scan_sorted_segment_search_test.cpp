#include "base_test.hpp"

#include "operators/table_scan/sorted_segment_search.hpp"
#include "storage/segment_iterate.hpp"

namespace {

struct TestData {
  std::string predicate_condition_string;
  opossum::PredicateCondition predicate_condition;
  int32_t search_value;
  std::vector<int32_t> expected;
};

using Params = std::tuple<TestData, opossum::OrderByMode, bool>;

auto table_scan_sorted_segment_search_test_formatter = [](const ::testing::TestParamInfo<Params> info) {
  return opossum::order_by_mode_to_string.left.at(std::get<1>(info.param)) +
         std::get<0>(info.param).predicate_condition_string + (std::get<2>(info.param) ? "WithNulls" : "WithoutNulls");
};

}  // namespace

namespace opossum {

class OperatorsTableScanSortedSegmentSearchTest : public BaseTest, public ::testing::WithParamInterface<Params> {
 protected:
  void SetUp() override {
    bool nullable;
    TestData test_data;
    std::tie(test_data, _order_by, nullable) = GetParam();
    _predicate_condition = test_data.predicate_condition;
    _search_value = test_data.search_value;
    _expected = test_data.expected;

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

    const auto table_size = 5;
    for (int32_t row = 0; row < table_size; ++row) {
      _segment->append(ascending ? row : table_size - row - 1);
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

// clang-format off
INSTANTIATE_TEST_SUITE_P(
    Predicates, OperatorsTableScanSortedSegmentSearchTest,
    ::testing::Combine(
        ::testing::Values(
            // The following rows specify the different testcases.
            // All of them run on a table that consists of the values 0, 0, 1, 1, 2, 2, 3, 3, 4, 4.
            // The parameters work like this:
            // 1. predicate condition to use
            // 2. value to compare
            // 3. expected result
            //
            // Each row is tested with all four sorted orders and nullable or non-nullable segments. For descending
            // sort orders, the segments contain the values from 9 to 0 and the expected result is reversed, so that
            // you only need to specify the expected result in ascending order.
            TestData{"Equals", PredicateCondition::Equals, 2, {2, 2}},

            TestData{"NotEquals2Ranges", PredicateCondition::NotEquals, 2, {0, 0, 1, 1, 3, 3, 4, 4}},
            TestData{"NotEqualsRangeMinimum", PredicateCondition::NotEquals, 4, {0, 0, 1, 1, 2, 2, 3, 3}},
            TestData{"NotEqualsRangeMaximum", PredicateCondition::NotEquals, 0, {1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"NotEqualsAboveRange", PredicateCondition::NotEquals, 5, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"NotEqualsBelowRange", PredicateCondition::NotEquals, -1, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},

            TestData{"LessThanBelowRange", PredicateCondition::LessThan, -1, {}},
            TestData{"LessThanRangeMinimum", PredicateCondition::LessThan, 0, {}},
            TestData{"LessThan", PredicateCondition::LessThan, 2, {0, 0, 1, 1}},
            TestData{"LessThanAboveRange", PredicateCondition::LessThan, 5, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"LessThanAboveMaximum", PredicateCondition::LessThan, 4, {0, 0, 1, 1, 2, 2, 3, 3}},

            TestData{"LessThanEqualsBelowRange", PredicateCondition::LessThanEquals, -1, {}},
            TestData{"LessThanEqualsRangeMinimum", PredicateCondition::LessThanEquals, 0, {0, 0}},
            TestData{"LessThanEquals", PredicateCondition::LessThanEquals, 2, {0, 0, 1, 1, 2, 2}},
            TestData{"LessThanEqualsAboveRange", PredicateCondition::LessThanEquals, 5, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"LessThanEqualsRangeMaximum", PredicateCondition::LessThanEquals, 4, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT

            TestData{"GreaterThanBelowRange", PredicateCondition::GreaterThan, -1, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"GreaterThanRangeMinimum", PredicateCondition::GreaterThan, 0, {1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"GreaterThan", PredicateCondition::GreaterThan, 2, {3, 3, 4, 4}},
            TestData{"GreaterThanAboveRange", PredicateCondition::GreaterThan, 5, {}},
            TestData{"GreaterThanRangeMaximum", PredicateCondition::GreaterThan, 4, {}},

            TestData{"GreaterThanEqualsBelowRange", PredicateCondition::GreaterThanEquals, -1, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"GreaterThanEqualsRangeMinimum", PredicateCondition::GreaterThanEquals, 0, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"GreaterThanEquals", PredicateCondition::GreaterThanEquals, 2, {2, 2, 3, 3, 4, 4}},
            TestData{"GreaterThanEqualsAboveRange", PredicateCondition::GreaterThanEquals, 5, {}},  // NOLINT
            TestData{"GreaterThanEqualsRangeMaximum", PredicateCondition::GreaterThanEquals, 4, {4, 4}}),  // NOLINT

        ::testing::Values(OrderByMode::Ascending, OrderByMode::AscendingNullsLast, OrderByMode::Descending,
                          OrderByMode::DescendingNullsLast),
        ::testing::Bool()),  // nullable
    table_scan_sorted_segment_search_test_formatter);
// clang-format on

TEST_P(OperatorsTableScanSortedSegmentSearchTest, ScanSortedSegment) {
  const auto iterable = create_iterable_from_segment(*_segment);
  iterable.with_iterators([&](auto input_begin, auto input_end) {
    auto sorted_segment_search =
        SortedSegmentSearch(input_begin, input_end, _order_by, _predicate_condition, _search_value);
    sorted_segment_search.scan_sorted_segment([&](auto output_begin, auto output_end) {
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
