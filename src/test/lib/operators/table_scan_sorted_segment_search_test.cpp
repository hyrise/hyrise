#include "base_test.hpp"

#include <magic_enum.hpp>

#include "operators/table_scan/sorted_segment_search.hpp"
#include "storage/segment_iterate.hpp"

namespace {

struct TestData {
  std::string predicate_condition_string;
  hyrise::PredicateCondition predicate_condition;
  int32_t search_value;
  std::optional<int32_t> second_search_value;
  std::vector<int32_t> expected;
};

enum class NullValueUsage { WithNulls, WithoutNulls, OnlyNulls };

using Params = std::tuple<TestData, hyrise::SortMode, NullValueUsage>;

auto table_scan_sorted_segment_search_test_formatter = [](const ::testing::TestParamInfo<Params> info) {
  return std::string{magic_enum::enum_name(std::get<1>(info.param))} +
         std::get<0>(info.param).predicate_condition_string +
         std::string{magic_enum::enum_name(std::get<2>(info.param))};
};

}  // namespace

namespace hyrise {

class OperatorsTableScanSortedSegmentSearchTest : public BaseTest, public ::testing::WithParamInterface<Params> {
 protected:
  void SetUp() override {
    TestData test_data;
    NullValueUsage null_value_usage;
    std::tie(test_data, _sorted_by, null_value_usage) = GetParam();
    _predicate_condition = test_data.predicate_condition;
    _search_value = test_data.search_value;
    _second_search_value = test_data.second_search_value;
    _expected = test_data.expected;
    _nullable = null_value_usage != NullValueUsage::WithoutNulls;
    _all_values_null = null_value_usage == NullValueUsage::OnlyNulls;

    const bool ascending = _sorted_by == SortMode::Ascending;

    if (!ascending) {
      std::reverse(_expected.begin(), _expected.end());
    }

    _segment = std::make_unique<ValueSegment<int32_t>>(_nullable);

    if (_nullable) {
      _segment->append(NULL_VALUE);
      _segment->append(NULL_VALUE);
      _segment->append(NULL_VALUE);
    }

    if (_all_values_null) {
      return;
    }

    const auto table_size = 5;
    for (int32_t row = 0; row < table_size; ++row) {
      _segment->append(ascending ? row : table_size - row - 1);
      _segment->append(ascending ? row : table_size - row - 1);
    }
  }

  std::unique_ptr<ValueSegment<int32_t>> _segment;
  PredicateCondition _predicate_condition;
  bool _nullable;
  bool _all_values_null;
  int32_t _search_value;
  std::optional<int32_t> _second_search_value;
  std::vector<int32_t> _expected;
  SortMode _sorted_by;
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
            TestData{"Equals", PredicateCondition::Equals, 2, {}, {2, 2}},

            TestData{"NotEquals2Ranges", PredicateCondition::NotEquals, 2, {}, {0, 0, 1, 1, 3, 3, 4, 4}},
            TestData{"NotEqualsRangeMinimum", PredicateCondition::NotEquals, 4, {}, {0, 0, 1, 1, 2, 2, 3, 3}},
            TestData{"NotEqualsRangeMaximum", PredicateCondition::NotEquals, 0, {}, {1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"NotEqualsAboveRange", PredicateCondition::NotEquals, 5, {}, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"NotEqualsBelowRange", PredicateCondition::NotEquals, -1, {}, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},

            TestData{"LessThanBelowRange", PredicateCondition::LessThan, -1, {}, {}},
            TestData{"LessThanRangeMinimum", PredicateCondition::LessThan, 0, {}, {}},
            TestData{"LessThan", PredicateCondition::LessThan, 2, {}, {0, 0, 1, 1}},
            TestData{"LessThanAboveRange", PredicateCondition::LessThan, 5, {}, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"LessThanAboveMaximum", PredicateCondition::LessThan, 4, {}, {0, 0, 1, 1, 2, 2, 3, 3}},

            TestData{"LessThanEqualsBelowRange", PredicateCondition::LessThanEquals, -1, {}, {}},
            TestData{"LessThanEqualsRangeMinimum", PredicateCondition::LessThanEquals, 0, {}, {0, 0}},
            TestData{"LessThanEquals", PredicateCondition::LessThanEquals, 2, {}, {0, 0, 1, 1, 2, 2}},
            TestData{"LessThanEqualsAboveRange", PredicateCondition::LessThanEquals, 5, {}, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"LessThanEqualsRangeMaximum", PredicateCondition::LessThanEquals, 4, {}, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT

            TestData{"GreaterThanBelowRange", PredicateCondition::GreaterThan, -1, {}, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"GreaterThanRangeMinimum", PredicateCondition::GreaterThan, 0, {}, {1, 1, 2, 2, 3, 3, 4, 4}},
            TestData{"GreaterThan", PredicateCondition::GreaterThan, 2, {}, {3, 3, 4, 4}},
            TestData{"GreaterThanAboveRange", PredicateCondition::GreaterThan, 5, {}, {}},
            TestData{"GreaterThanRangeMaximum", PredicateCondition::GreaterThan, 4, {}, {}},

            TestData{"GreaterThanEqualsBelowRange", PredicateCondition::GreaterThanEquals, -1, {}, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"GreaterThanEqualsRangeMinimum", PredicateCondition::GreaterThanEquals, 0, {}, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"GreaterThanEquals", PredicateCondition::GreaterThanEquals, 2, {}, {2, 2, 3, 3, 4, 4}},
            TestData{"GreaterThanEqualsAboveRange", PredicateCondition::GreaterThanEquals, 5, {}, {}},
            TestData{"GreaterThanEqualsRangeMaximum", PredicateCondition::GreaterThanEquals, 4, {}, {4, 4}},

            TestData{"BetweenInclusiveWiderRange", PredicateCondition::BetweenInclusive, -1, 5, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenInclusiveExactRange", PredicateCondition::BetweenInclusive, 0, 4, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenInclusiveRangeMinimum", PredicateCondition::BetweenInclusive, 0, 3, {0, 0, 1, 1, 2, 2, 3, 3}},  // NOLINT
            TestData{"BetweenInclusiveRangeMaximum", PredicateCondition::BetweenInclusive, 2, 4, {2, 2, 3, 3, 4, 4}},
            TestData{"BetweenInclusive", PredicateCondition::BetweenInclusive, 1, 3, {1, 1, 2, 2, 3, 3}},
            TestData{"BetweenInclusiveAboveRange", PredicateCondition::BetweenInclusive, 5, 10, {}},
            TestData{"BetweenInclusiveBelowRange", PredicateCondition::BetweenInclusive, -5, -1, {}},

            TestData{"BetweenExclusiveWiderRange", PredicateCondition::BetweenExclusive, -2, 6, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenExclusiveExactRange", PredicateCondition::BetweenExclusive, -1, 5, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenExclusiveRangeMinimum", PredicateCondition::BetweenExclusive, -1, 4, {0, 0, 1, 1, 2, 2, 3, 3}},  // NOLINT
            TestData{"BetweenExclusiveRangeMaximum", PredicateCondition::BetweenExclusive, 1, 5, {2, 2, 3, 3, 4, 4}},
            TestData{"BetweenExclusive", PredicateCondition::BetweenExclusive, 0, 4, {1, 1, 2, 2, 3, 3}},
            TestData{"BetweenExclusiveAboveRange", PredicateCondition::BetweenExclusive, 4, 10, {}},
            TestData{"BetweenExclusiveBelowRange", PredicateCondition::BetweenExclusive, -5, 0, {}},

            TestData{"BetweenLowerExclusiveWiderRange", PredicateCondition::BetweenLowerExclusive, -2, 4, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenLowerExclusiveExactRange", PredicateCondition::BetweenLowerExclusive, -1, 4, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenLowerExclusiveRangeMinimum", PredicateCondition::BetweenLowerExclusive, -1, 3, {0, 0, 1, 1, 2, 2, 3, 3}},  // NOLINT
            TestData{"BetweenLowerExclusiveRangeMaximum", PredicateCondition::BetweenLowerExclusive, 1, 4, {2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenLowerExclusive", PredicateCondition::BetweenLowerExclusive, 0, 3, {1, 1, 2, 2, 3, 3}},
            TestData{"BetweenLowerExclusiveAboveRange", PredicateCondition::BetweenLowerExclusive, 4, 10, {}},
            TestData{"BetweenLowerExclusiveBelowRange", PredicateCondition::BetweenLowerExclusive, -5, -1, {}},

            TestData{"BetweenUpperExclusiveWiderRange", PredicateCondition::BetweenUpperExclusive, -1, 6, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenUpperExclusiveExactRange", PredicateCondition::BetweenUpperExclusive, 0, 5, {0, 0, 1, 1, 2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenUpperExclusiveRangeMinimum", PredicateCondition::BetweenUpperExclusive, 0, 4, {0, 0, 1, 1, 2, 2, 3, 3}},  // NOLINT
            TestData{"BetweenUpperExclusiveRangeMaximum", PredicateCondition::BetweenUpperExclusive, 2, 5, {2, 2, 3, 3, 4, 4}},  // NOLINT
            TestData{"BetweenUpperExclusive", PredicateCondition::BetweenUpperExclusive, 1, 4, {1, 1, 2, 2, 3, 3}},
            TestData{"BetweenUpperExclusiveAboveRange", PredicateCondition::BetweenUpperExclusive, 5, 10, {}},
            TestData{"BetweenUpperExclusiveBelowRange", PredicateCondition::BetweenUpperExclusive, -5, 0, {}}),

        ::testing::Values(SortMode::Ascending, SortMode::Descending),
        ::testing::Values(NullValueUsage::WithoutNulls, NullValueUsage::WithNulls, NullValueUsage::OnlyNulls)),
    table_scan_sorted_segment_search_test_formatter);

// clang-format on

TEST_P(OperatorsTableScanSortedSegmentSearchTest, ScanSortedSegment) {
  const auto iterable = create_iterable_from_segment(*_segment);
  iterable.with_iterators([&](auto input_begin, auto input_end) {
    auto sorted_segment_search =
        _second_search_value
            ? SortedSegmentSearch(input_begin, input_end, _sorted_by, _nullable, _predicate_condition, _search_value,
                                  *_second_search_value)
            : SortedSegmentSearch(input_begin, input_end, _sorted_by, _nullable, _predicate_condition, _search_value);
    auto matches = RowIDPosList{};
    const auto initial_chunk_id = ChunkID{0};
    sorted_segment_search.scan_sorted_segment(initial_chunk_id, matches, nullptr);

    if (matches.empty()) {
      EXPECT_TRUE(sorted_segment_search.no_rows_matching);
    } else if (matches.size() == _segment->size()) {
      EXPECT_TRUE(sorted_segment_search.all_rows_matching);
    }

    if (_all_values_null) {
      EXPECT_TRUE(matches.empty());
      return;
    }

    ASSERT_EQ(matches.size(), _expected.size());

    for (auto index = size_t{0}; index < _expected.size(); ++index) {
      const auto& [chunk_id, chunk_offset] = matches[index];
      EXPECT_EQ(chunk_id, initial_chunk_id);
      EXPECT_FALSE(_segment->is_null(chunk_offset)) << "row " << index << " is null";
      EXPECT_EQ(_segment->get(chunk_offset), _expected[index]) << "row " << index << " is invalid";
    }
  });
}

}  // namespace hyrise
