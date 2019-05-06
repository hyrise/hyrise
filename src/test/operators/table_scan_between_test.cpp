#include <tuple>

#include "operators/operator_scan_predicate.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "typed_operator_base_test.hpp"

namespace opossum {

class TableScanBetweenTest : public TypedOperatorBaseTest {
 protected:
  std::shared_ptr<AbstractOperator> _data_table_wrapper;

  void SetUp() override {
    // For the test, we create a table with the data type that is to be scanned as the first column and a control int
    // in the second column:
    //
    // a<DataType>  b<int>
    // 10.25         0
    // 12.25         1
    // 14.25 / NULL  2       (each third row is nulled if the table is marked as nullable)
    // 16.25         3
    // ...
    // 30.25         10
    //
    // As the first column is TYPE CASTED, it contains 10 for an int column, the string "10.25" for a string column etc.
    // We chose .25 because that can be exactly expressed in a float.

    const auto& [data_type, encoding, nullable] = GetParam();

    auto column_definitions = TableColumnDefinitions{{"a", data_type, nullable}, {"b", DataType::Int, nullable}};

    const auto data_table = std::make_shared<Table>(column_definitions, TableType::Data, 6);

    // `nullable=nullable` is a dirty hack to work around C++ defect 2313.
    resolve_data_type(data_type, [&, nullable = nullable](const auto type) {
      using Type = typename decltype(type)::type;
      for (auto i = 0; i <= 10; ++i) {
        auto double_value = 10.25 + i * 2.0;

        if (nullable && i % 3 == 2) {
          data_table->append({NullValue{}, i});
        } else {
          if constexpr (std::is_same_v<pmr_string, Type>) {
            data_table->append({pmr_string{std::to_string(double_value)}, i});
          } else {
            data_table->append({static_cast<Type>(double_value), i});
          }
        }
      }
    });

    // We have two full chunks and one open chunk, we only encode the full chunks
    for (auto chunk_id = ChunkID{0}; chunk_id < 2; ++chunk_id) {
      ChunkEncoder::encode_chunk(data_table->get_chunk(chunk_id), {data_type, DataType::Int},
                                 {encoding, EncodingType::Unencoded});
    }

    _data_table_wrapper = std::make_shared<TableWrapper>(data_table);
    _data_table_wrapper->execute();
  }

  // This is a helper function, which runs a between table scan on the data defined in SetUp above.
  // It takes the boundaries and the expected result index positions from the given tests parameter.
  // To test all functionality, it is also necessary to consider the left/right inclusiveness. It cannot be done
  // automatically, because the results differ depending on the chosen inclusiveness.
  // Standard SQL only supports double-inclusiveness, therefore this is the default.
  // a sample tests structure looks like this:
  // std::vector<std::tuple<AllTypeVariant, AllTypeVariant, std::vector<int>>>{
  //    {12.25, 16.25, {1, 2, 3}},
  //    {12.0, 16.25, {1, 2, 3}},
  //    {12.0, 16.75, {1, 2, 3}},
  // }
  void _test_between_scan(std::vector<std::tuple<AllTypeVariant, AllTypeVariant, std::vector<int>>>& tests,
                          PredicateCondition predicate_condition) {
    const auto& [data_type, encoding, nullable] = GetParam();
    std::ignore = encoding;
    resolve_data_type(data_type, [&, nullable = nullable](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      for (const auto& [left, right, expected_with_null] : tests) {
        SCOPED_TRACE(std::string("BETWEEN ") + std::to_string(boost::get<double>(left)) +
                     (is_lower_inclusive_between(predicate_condition) ? " (inclusive)" : " (exclusive)") + " AND " +
                     std::to_string(boost::get<double>(right)) +
                     (is_upper_inclusive_between(predicate_condition) ? " (inclusive)" : " (exclusive)"));

        auto left_casted = ColumnDataType{};
        auto right_casted = ColumnDataType{};

        // Float-with-String comparison not supported. We have to manually convert all floats to Strings if we're
        // scanning on a String column.
        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          left_casted = pmr_string{std::to_string(boost::get<double>(left))};
          right_casted = pmr_string{std::to_string(boost::get<double>(right))};
        } else {
          left_casted = static_cast<ColumnDataType>(boost::get<double>(left));
          right_casted = static_cast<ColumnDataType>(boost::get<double>(right));
        }

        auto scan =
            create_between_table_scan(_data_table_wrapper, ColumnID{0}, left_casted, right_casted, predicate_condition);
        scan->execute();

        const auto& result_table = *scan->get_output();
        auto result_ints = std::vector<int>{};
        for (const auto& chunk : result_table.chunks()) {
          const auto segment_b = chunk->get_segment(ColumnID{1});
          for (auto offset = ChunkOffset{0}; offset < segment_b->size(); ++offset) {
            result_ints.emplace_back(boost::get<int>((*segment_b)[offset]));
          }
        }
        std::sort(result_ints.begin(), result_ints.end());

        auto expected = expected_with_null;
        if (nullable) {
          // Remove the positions that should not be included because they are meant to be NULL
          // In this case, remove approximately every third value.
          expected.erase(std::remove_if(expected.begin(), expected.end(), [](int x) { return x % 3 == 2; }),
                         expected.end());
        }

        ASSERT_EQ(result_ints, expected);
      }
    });
  }
};

TEST_P(TableScanBetweenTest, Inclusive) {
  auto inclusive_tests = std::vector<std::tuple<AllTypeVariant, AllTypeVariant, std::vector<int>>>{
      {12.25, 16.25, {1, 2, 3}},                          // Both boundaries exact match
      {12.0, 16.25, {1, 2, 3}},                           // Left boundary open match
      {12.25, 16.75, {1, 2, 3}},                          // Right boundary open match
      {12.0, 16.75, {1, 2, 3}},                           // Both boundaries open match
      {0.0, 16.75, {0, 1, 2, 3}},                         // Left boundary before first value
      {16.0, 50.75, {3, 4, 5, 6, 7, 8, 9, 10}},           // Right boundary after last value
      {13.0, 16.25, {2, 3}},                              // Left boundary after first value
      {12.25, 15.0, {1, 2}},                              // Right boundary before last value
      {0.25, 50.75, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},  // Matching all values
      {0.25, 0.75, {}},                                   // Matching no value
  };

  _test_between_scan(inclusive_tests, PredicateCondition::BetweenInclusive);
}

TEST_P(TableScanBetweenTest, LowerExclusive) {
  auto left_exclusive_tests = std::vector<std::tuple<AllTypeVariant, AllTypeVariant, std::vector<int>>>{
      {11.0, 16.25, {1, 2, 3}},  // Left boundary open match
      {12.25, 16.25, {2, 3}},    // Both boundaries exact match
      {13.0, 16.25, {2, 3}},     // Left boundary inner value
  };

  _test_between_scan(left_exclusive_tests, PredicateCondition::BetweenLowerExclusive);
}

TEST_P(TableScanBetweenTest, UpperExclusive) {
  auto right_exclusive_tests = std::vector<std::tuple<AllTypeVariant, AllTypeVariant, std::vector<int>>>{
      {12.25, 17.0, {1, 2, 3}},  // Right boundary open match
      {12.25, 16.25, {1, 2}},    // Both boundaries exact match
      {12.25, 15.0, {1, 2}},     // Right boundary inner value
  };

  _test_between_scan(right_exclusive_tests, PredicateCondition::BetweenUpperExclusive);
}

TEST_P(TableScanBetweenTest, Exclusive) {
  auto exclusive_tests = std::vector<std::tuple<AllTypeVariant, AllTypeVariant, std::vector<int>>>{
      {12.25, 16.25, {2}},      // Both boundaries exact match
      {11.0, 16.25, {1, 2}},    // Left boundary open match
      {12.25, 17.0, {2, 3}},    // Right boundary open match
      {11.0, 17.0, {1, 2, 3}},  // Both boundaries open match
      {13.0, 16.25, {2}},       // Left boundary inner value
      {12.25, 15.0, {2}},       // Right boundary inner value
      {13.0, 15.0, {2}},        // Both boundaries inner value
  };

  _test_between_scan(exclusive_tests, PredicateCondition::BetweenExclusive);
}

INSTANTIATE_TEST_CASE_P(TableScanBetweenTestInstances, TableScanBetweenTest, testing::ValuesIn(create_test_params()),
                        TypedOperatorBaseTest::format);

}  // namespace opossum
