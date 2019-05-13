#include "operators/aggregate_verification.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"
#include "utils/make_bimap.hpp"
#include "base_operator_test_runner.hpp"

/**
 * This file contains the main tests for Hyrise's aggregate operators.
 * Testing is done by comparing the result of any given join operator with that of the AggregateVerification for a
 * number of configurations (i.e. aggregates, group by columns, data types, ...)
 */

using namespace std::string_literals;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

class BaseAggregateOperatorFactory;

struct AggregateTestConfiguration {
  InputTableConfiguration input;
  std::vector<AggregateColumnDefinition> aggregate_column_definitions;
  std::vector<ColumnID> group_by_column_ids;
  std::shared_ptr<BaseAggregateOperatorFactory> aggregate_operator_factory;

  auto to_tuple() const {
    return std::tie(input, aggregate_column_definitions, group_by_column_ids);
  }
};

bool operator<(const AggregateTestConfiguration& l, const AggregateTestConfiguration& r) { return l.to_tuple() < r.to_tuple(); }
bool operator==(const AggregateTestConfiguration& l, const AggregateTestConfiguration& r) { return l.to_tuple() == r.to_tuple(); }

// Virtual interface to create a join operator
class BaseAggregateOperatorFactory {
 public:
  virtual ~BaseAggregateOperatorFactory() = default;
  virtual std::shared_ptr<AbstractAggregateOperator> create_operator(const std::shared_ptr<AbstractOperator>& in,
                                                                     const AggregateTestConfiguration& configuration) = 0;
};

template <typename AggregateOperator>
class AggregateOperatorFactory : public BaseAggregateOperatorFactory {
  std::shared_ptr<AbstractAggregateOperator> create_operator(const std::shared_ptr<AbstractOperator>& in,
                                                            const AggregateTestConfiguration& configuration) override {
    return std::make_shared<AggregateOperator>(in, configuration.aggregate_column_definitions,
                                               configuration.group_by_column_ids);
  }
};
  
//// Order of columns in the input tables
//const std::unordered_map<DataType, size_t> data_type_order = {
//    {DataType::Int, 0u}, {DataType::Float, 1u}, {DataType::Double, 2u}, {DataType::Long, 3u}, {DataType::String, 4u},
//};

}  // namespace

namespace opossum {

class AggregateTestRunner : public BaseOperatorTestRunner<AggregateTestConfiguration> {
 public:
  AggregateTestRunner():
  BaseOperatorTestRunner<AggregateTestConfiguration>("resources/test_data/tbl/aggregate_test_runner/") {}

  template <typename AggregateOperator>
  static std::vector<AggregateTestConfiguration> create_configurations() {
    auto configurations = std::vector<AggregateTestConfiguration>{};

    auto default_configuration = AggregateTestConfiguration{
      {InputSide::Left, 3, 10, BaseOperatorTestRunner<AggregateTestConfiguration>::all_input_table_types.front(), BaseOperatorTestRunner<AggregateTestConfiguration>::all_encoding_types.front()},
      {},
      {{ColumnID{0}}},
      std::make_shared<AggregateOperatorFactory<AggregateOperator>>()
    };

    configurations.emplace_back(default_configuration);

    std::sort(configurations.begin(), configurations.end());
    configurations.erase(std::unique(configurations.begin(), configurations.end()), configurations.end());

    return configurations;
  }

};  // namespace opossum

TEST_P(AggregateTestRunner, TestAggregate) {
  const auto configuration = GetParam();

  const auto input_table = get_table(configuration.input);
  const auto input_operator = std::make_shared<TableWrapper>(input_table);

  const auto aggregate_operator = configuration.aggregate_operator_factory->create_operator(input_operator, configuration);

  const auto aggregate_verification =
      std::make_shared<AggregateVerification>(input_operator, configuration.aggregate_column_definitions, configuration.group_by_column_ids);

  input_operator->execute();

  auto actual_table = std::shared_ptr<const Table>{};
  auto expected_table = std::shared_ptr<const Table>{};
  auto table_difference_message = std::optional<std::string>{};

  const auto print_configuration_info = [&]() {
    std::cout << "==================== AggregateOperator ======================" << std::endl;
    std::cout << aggregate_operator->description(DescriptionMode::MultiLine) << std::endl;
    std::cout << "======================= Input Table ========================" << std::endl;
    Print::print(input_table, PrintFlags::IgnoreChunkBoundaries);
    std::cout << "Chunk size: " << configuration.input.chunk_size << std::endl;
    std::cout << "Table type: " << input_table_type_to_string.at(configuration.input.table_type) << std::endl;
    std::cout << get_table_path(configuration.input) << std::endl;
    std::cout << std::endl;
    std::cout << "==================== Actual Output Table ===================" << std::endl;
    if (aggregate_operator->get_output()) {
      Print::print(aggregate_operator->get_output(), PrintFlags::IgnoreChunkBoundaries);
      std::cout << std::endl;
    } else {
      std::cout << "No Table produced by the aggregate operator under test" << std::endl;
    }
    std::cout << "=================== Expected Output Table ==================" << std::endl;
    if (aggregate_verification->get_output()) {
      Print::print(aggregate_verification->get_output(), PrintFlags::IgnoreChunkBoundaries);
      std::cout << std::endl;
    } else {
      std::cout << "No Table produced by the reference join operator" << std::endl;
    }
    std::cout << "======================== Difference ========================" << std::endl;
    std::cout << *table_difference_message << std::endl;
    std::cout << "============================================================" << std::endl;
  };

  auto expected_output_table_iter = expected_output_tables.find(configuration);
  try {
    // Cache reference table to avoid redundant computation of the same
    if (expected_output_table_iter == expected_output_tables.end()) {
      aggregate_verification->execute();
      const auto expected_output_table = aggregate_verification->get_output();
      expected_output_table_iter = expected_output_tables.emplace(configuration, expected_output_table).first;
    }
    aggregate_operator->execute();
  } catch (...) {
    // If an error occurred in the join operator under test, we still want to see the test configuration
    print_configuration_info();
    throw;
  }

  actual_table = aggregate_operator->get_output();
  expected_table = expected_output_table_iter->second;

  table_difference_message = check_table_equal(actual_table, expected_table, OrderSensitivity::No, TypeCmpMode::Strict,
                                               FloatComparisonMode::AbsoluteDifference);
  if (table_difference_message) {
    print_configuration_info();
    FAIL();
  }
}

// clang-format off
INSTANTIATE_TEST_CASE_P(AggregateHash, AggregateTestRunner, testing::ValuesIn(AggregateTestRunner::create_configurations<AggregateHash>()), );  // NOLINT
INSTANTIATE_TEST_CASE_P(AggregateSort, AggregateTestRunner, testing::ValuesIn(AggregateTestRunner::create_configurations<AggregateSort>()), );  // NOLINT
// clang-format on

}  // namespace opossum
