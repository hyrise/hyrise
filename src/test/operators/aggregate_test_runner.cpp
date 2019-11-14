#include "base_operator_test_runner.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_hashsort.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/verification/aggregate_verification.hpp"
#include "utils/load_table.hpp"
#include "utils/make_bimap.hpp"

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

  // Only relevant for AggregateHashSort
  std::optional<AggregateHashSortConfig> aggregate_hashsort_config;

  auto to_tuple() const { return std::tie(input, aggregate_column_definitions, group_by_column_ids); }
};

bool operator<(const AggregateTestConfiguration& l, const AggregateTestConfiguration& r) {
  return l.to_tuple() < r.to_tuple();
}
bool operator==(const AggregateTestConfiguration& l, const AggregateTestConfiguration& r) {
  return l.to_tuple() == r.to_tuple();
}

// Virtual interface to create a join operator
class BaseAggregateOperatorFactory {
 public:
  virtual ~BaseAggregateOperatorFactory() = default;
  virtual std::shared_ptr<AbstractAggregateOperator> create_operator(
      const std::shared_ptr<AbstractOperator>& in, const AggregateTestConfiguration& configuration) = 0;
};

template <typename AggregateOperator>
class AggregateOperatorFactory : public BaseAggregateOperatorFactory {
  std::shared_ptr<AbstractAggregateOperator> create_operator(const std::shared_ptr<AbstractOperator>& in,
                                                             const AggregateTestConfiguration& configuration) override {
    return std::make_shared<AggregateOperator>(in, configuration.aggregate_column_definitions,
                                               configuration.group_by_column_ids);
  }
};

template <>
class AggregateOperatorFactory<AggregateHashSort> : public BaseAggregateOperatorFactory {
  std::shared_ptr<AbstractAggregateOperator> create_operator(const std::shared_ptr<AbstractOperator>& in,
                                                             const AggregateTestConfiguration& configuration) override {
    return std::make_shared<AggregateHashSort>(in, configuration.aggregate_column_definitions,
                                               configuration.group_by_column_ids, configuration.aggregate_hashsort_config);
  }
};

// Order/DataTypes of columns in the input table
const std::unordered_map<DataType, ColumnID> aggregate_column_ids = {
    {DataType::Int, ColumnID{11}},  {DataType::Float, ColumnID{12}},  {DataType::Double, ColumnID{13}},
    {DataType::Long, ColumnID{14}}, {DataType::String, ColumnID{15}},
};

const std::unordered_map<DataType, ColumnID> group_by_column_ids = {
    {DataType::Int, ColumnID{0}},  {DataType::Float, ColumnID{2}},  {DataType::Double, ColumnID{4}},
    {DataType::Long, ColumnID{6}}, {DataType::String, ColumnID{8}},
};

const auto column_data_types = std::vector{DataType::Int,    DataType::Int,    DataType::Int,    DataType::Long,
                                           DataType::Long,   DataType::Float,  DataType::Float,  DataType::Double,
                                           DataType::Double, DataType::String, DataType::String, DataType::Int,
                                           DataType::Long,   DataType::Float,  DataType::Double, DataType::String};

}  // namespace

namespace opossum {

class AggregateTestRunner : public BaseOperatorTestRunner<AggregateTestConfiguration> {
 public:
  AggregateTestRunner()
      : BaseOperatorTestRunner<AggregateTestConfiguration>("resources/test_data/tbl/aggregate_test_runner/") {}

  template <typename AggregateOperator>
  static std::vector<AggregateTestConfiguration> create_configurations() {
    auto configurations = std::vector<AggregateTestConfiguration>{};

    auto all_data_types = std::vector<DataType>{};
    hana::for_each(data_type_pairs, [&](auto pair) {
      const DataType d = hana::first(pair);
      all_data_types.emplace_back(d);
    });

    const auto all_aggregate_functions =
        std::vector{AggregateFunction::Min,          AggregateFunction::Max,       AggregateFunction::Sum,
                    AggregateFunction::Avg,          AggregateFunction::CountRows, AggregateFunction::CountNonNull,
                    AggregateFunction::CountDistinct};

    const auto all_table_sizes = std::vector<size_t>{0, 10};

    const auto default_configuration =
        AggregateTestConfiguration{{InputSide::Left, 3, 10, all_input_table_types.front(), all_encoding_types.front()},
                                   {},
                                   {},
                                   std::make_shared<AggregateOperatorFactory<AggregateOperator>>(), {}};

    const auto add_configuration_if_supported = [&](const auto& configuration) {
      const auto supported = std::all_of(
          configuration.aggregate_column_definitions.begin(), configuration.aggregate_column_definitions.end(),
          [&](const auto& aggregate_column_definition) {
            auto column_data_type = std::optional<DataType>{};
            if (aggregate_column_definition.column) {
              column_data_type = column_data_types.at(*aggregate_column_definition.column);
            }
            return AggregateColumnDefinition::supported(column_data_type, aggregate_column_definition.function);
          });

      if (supported) {
        configurations.emplace_back(configuration);
      }
    };

    // Test single-column group-by for all DataTypes and on nullable/non-nullable columns.
    for (const auto data_type : all_data_types) {
      for (const auto nullable : {false, true}) {
        auto configuration = default_configuration;
        configuration.group_by_column_ids = {
            ColumnID{group_by_column_ids.at(data_type) + (nullable ? 1 : 0)},
        };
        configuration.aggregate_column_definitions = {};

        add_configuration_if_supported(configuration);
      }
    }

    // Test multi-column group-by with a number of DataType/nullable combinations. With and without aggregate column.
    auto group_by_column_sets = std::vector{
        std::vector<std::pair<DataType, bool>>{{DataType::Int, false}, {DataType::Float, true}},
        std::vector<std::pair<DataType, bool>>{{DataType::Long, false}, {DataType::Float, false}},
        std::vector<std::pair<DataType, bool>>{{DataType::Long, true}, {DataType::Double, false}},
        std::vector<std::pair<DataType, bool>>{
            {DataType::String, false}, {DataType::Int, false}, {DataType::Long, false}},
        std::vector<std::pair<DataType, bool>>{
            {DataType::String, true}, {DataType::Int, false}, {DataType::Long, false}},
        std::vector<std::pair<DataType, bool>>{{DataType::Int, false}, {DataType::Int, false}, {DataType::Int, false}},
        std::vector<std::pair<DataType, bool>>{{DataType::Int, false}, {DataType::Int, true}, {DataType::Int, false}},
    };

    for (const auto& group_by_column_set : group_by_column_sets) {
      for (const auto with_aggregate_column : {false, true}) {
        auto configuration = default_configuration;

        for (const auto& [data_type, nullable] : group_by_column_set) {
          configuration.group_by_column_ids.emplace_back(group_by_column_ids.at(data_type) + (nullable ? 1 : 0));
        }

        if constexpr (std::is_same_v<AggregateOperator, AggregateHashSort>) {
          auto aggregate_hashsort_config = AggregateHashSortConfig{};
          aggregate_hashsort_config.hash_table_size = 4;
          aggregate_hashsort_config.hash_table_max_load_factor = 0.25f;
          aggregate_hashsort_config.max_partitioning_counter = 2;
          aggregate_hashsort_config.buffer_flush_threshold = 2;
          aggregate_hashsort_config.group_prefetch_threshold = 4;
          configuration.aggregate_hashsort_config = aggregate_hashsort_config;
        }

        if (with_aggregate_column) {
          configuration.aggregate_column_definitions = {
              {aggregate_column_ids.at(DataType::Int), AggregateFunction::Min},
          };
        }

        add_configuration_if_supported(configuration);
      }
    }

    // Test that - even if non-sensical - aggregating a group-by column works.
    for (const auto data_type : all_data_types) {
      for (const auto aggregate_function : all_aggregate_functions) {
        if (aggregate_function == AggregateFunction::CountRows) {
          continue;
        }

        for (const auto nullable : {false, true}) {
          auto configuration = default_configuration;

          const auto column_id = ColumnID{group_by_column_ids.at(data_type) + (nullable ? 1 : 0)};
          configuration.group_by_column_ids = {
              column_id,
          };

          configuration.aggregate_column_definitions = {
          {column_id, aggregate_function},
          };

          add_configuration_if_supported(configuration);
        }
      }
    }

    std::sort(configurations.begin(), configurations.end());
    configurations.erase(std::unique(configurations.begin(), configurations.end()), configurations.end());

    return configurations;
  }

};  // namespace opossum

TEST_P(AggregateTestRunner, TestAggregate) {
  const auto configuration = GetParam();

  const auto input_table = get_table(configuration.input);
  const auto input_operator = std::make_shared<TableWrapper>(input_table);

  const auto aggregate_operator =
      configuration.aggregate_operator_factory->create_operator(input_operator, configuration);

  const auto aggregate_verification = std::make_shared<AggregateVerification>(
      input_operator, configuration.aggregate_column_definitions, configuration.group_by_column_ids);

  input_operator->execute();

  auto actual_table = std::shared_ptr<const Table>{};
  auto expected_table = std::shared_ptr<const Table>{};
  auto table_difference_message = std::optional<std::string>{};

  auto expected_output_table_iter = expected_output_tables.find(configuration);

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
    if (expected_output_table_iter->second) {
      Print::print(expected_output_table_iter->second, PrintFlags::IgnoreChunkBoundaries);
      std::cout << std::endl;
    } else {
      std::cout << "No Table produced by the reference aggregate operator" << std::endl;
    }
    std::cout << "======================== Difference ========================" << std::endl;
    std::cout << *table_difference_message << std::endl;
    std::cout << "============================================================" << std::endl;
  };
  print_configuration_info();
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
INSTANTIATE_TEST_CASE_P(AggregateHashSort, AggregateTestRunner, testing::ValuesIn(AggregateTestRunner::create_configurations<AggregateHashSort>()), );  // NOLINT
// clang-format on

}  // namespace opossum
