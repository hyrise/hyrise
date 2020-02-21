#include <memory>
#include <numeric>
#include <random>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "constant_mappings.hpp"
#include "expression/expression_functional.hpp"
#include "micro_benchmark_utils.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

namespace {

const auto ROWS = 1'000'000;
const auto CHUNK_SIZE = Chunk::DEFAULT_SIZE;
const auto STRING_SIZE = 512;

opossum::TableColumnDefinitions create_column_definitions(const opossum::DataType data_type) {
  auto table_column_definitions = opossum::TableColumnDefinitions();
  table_column_definitions.emplace_back("a", data_type, true);
  return table_column_definitions;
}

pmr_string pad_string(const std::string& str, const size_t size) {
  return pmr_string{std::string(size - str.length(), '0').append(str)};
}

template <typename Type>
std::vector<Type> generate_values(const size_t table_size);

template <>
std::vector<int32_t> generate_values<int32_t>(const size_t table_size) {
  auto values = std::vector<int32_t>(table_size);
  std::iota(values.begin(), values.end(), 0);
  return values;
}

template <>
std::vector<pmr_string> generate_values<pmr_string>(const size_t table_size) {
  auto values = std::vector<pmr_string>(table_size);
  for (size_t row_index = 0; row_index < table_size; ++row_index) {
    values[row_index] = pad_string(std::to_string(row_index), STRING_SIZE);
  }
  return values;
}

template <typename Type, typename ValueGenerator>
std::shared_ptr<TableWrapper> create_table(const DataType data_type, const int table_size,
                                           const ValueGenerator value_generator, const EncodingType encoding_type,
                                           const std::string& mode) {
  std::shared_ptr<TableWrapper> table_wrapper;

  const auto table_column_definitions = create_column_definitions(data_type);
  std::shared_ptr<Table> table;

  table = std::make_shared<Table>(table_column_definitions, TableType::Data);
  auto values = value_generator(table_size);

  std::random_device random_device;
  std::mt19937 generator(random_device());

  if (mode == "Shuffled") {
    std::shuffle(values.begin(), values.end(), generator);
  }

  for (auto chunk_index = 0u; chunk_index < table_size / CHUNK_SIZE; ++chunk_index) {
    const auto first = values.cbegin() + CHUNK_SIZE * chunk_index;
    const auto last = values.cbegin() + CHUNK_SIZE * (chunk_index + 1);
    auto value_vector = pmr_vector<Type>(first, last);
    if (mode == "SortedDescending") {
      std::reverse(value_vector.begin(), value_vector.end());
    }

    pmr_vector<bool> null_values(value_vector.size(), false);
    // setting 10% of the values NULL
    auto null_elements = static_cast<int>(std::round(value_vector.size() * 0.1));
    std::fill(null_values.begin(), null_values.begin() + null_elements, true);
    if (mode == "Shuffled") {
      std::shuffle(null_values.begin(), null_values.end(), generator);
    }

    const auto value_segment = std::make_shared<ValueSegment<Type>>(std::move(value_vector), std::move(null_values));
    table->append_chunk({value_segment});
    table->last_chunk()->finalize();
  }

  if (encoding_type != EncodingType::Unencoded) {
    // chunks are already finalized
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec(encoding_type));
  }

  if (mode == "Sorted") {
    const auto chunk_count = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      chunk->set_ordered_by(std::make_pair(ColumnID(0), OrderByMode::Ascending));
    }
  }
  if (mode == "SortedDescending") {
    const auto chunk_count = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      chunk->set_ordered_by(std::make_pair(ColumnID{0}, OrderByMode::Descending));
    }
  }

  table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  return table_wrapper;
}

}  // namespace

void BM_TableScanSorted(
    benchmark::State& state, const int table_size, const double selectivity, const EncodingType encoding_type,
    const std::string& mode, const bool is_between_scan,
    const std::function<std::shared_ptr<TableWrapper>(const EncodingType, const std::string)> table_creator) {
  micro_benchmark_clear_cache();

  // The benchmarks all run with different selectivities (ratio of values in the output to values in the input).
  // At this point the search value is selected in a way that our results correspond to the chosen selectivity.

  const auto table_wrapper = table_creator(encoding_type, mode);

  const auto table_column_definitions = table_wrapper->get_output()->column_definitions();

  const auto column_index = ColumnID(0);

  const auto column_definition = table_column_definitions.at(column_index);

  std::shared_ptr<AbstractPredicateExpression> predicate;
  const auto column_expression =
      pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

  resolve_data_type(column_definition.data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    if (is_between_scan) {
      auto left_search_value = AllTypeVariant{};
      auto right_search_value = AllTypeVariant{};
      const int left_raw_value = static_cast<int>(table_size * (0.5 - selectivity / 2.0));
      const int right_raw_value = static_cast<int>(table_size * (0.5 + selectivity / 2.0));
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        left_search_value = pad_string(std::to_string(left_raw_value), STRING_SIZE);
        right_search_value = pad_string(std::to_string(right_raw_value), STRING_SIZE);
      } else {
        left_search_value = static_cast<ColumnDataType>(left_raw_value);
        right_search_value = static_cast<ColumnDataType>(right_raw_value);
      }

      predicate = std::make_shared<BetweenExpression>(PredicateCondition::BetweenUpperExclusive, column_expression,
                                                      value_(left_search_value), value_(right_search_value));
    } else {
      auto search_value = AllTypeVariant{};
      const double raw_value = table_size * selectivity;
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        search_value = pad_string(std::to_string(raw_value), STRING_SIZE);
      } else {
        search_value = static_cast<ColumnDataType>(raw_value);
      }
      predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, column_expression,
                                                              value_(search_value));
    }
  });

  auto warm_up = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up->execute();
  for (auto _ : state) {
    auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

namespace {

void registerTableScanSortedBenchmarks() {
  using EncodingAndSupportedDataTypes = std::pair<EncodingType, std::vector<std::string>>;
  const std::map<std::string, EncodingAndSupportedDataTypes> encoding_types{
      {"None", EncodingAndSupportedDataTypes(EncodingType::Unencoded, {"Int", "String"})},
      {"Dictionary", EncodingAndSupportedDataTypes(EncodingType::Dictionary, {"Int", "String"})},
      {"FixedStringDictionary", EncodingAndSupportedDataTypes(EncodingType::FixedStringDictionary, {"String"})},
      {"FrameOfReference", EncodingAndSupportedDataTypes(EncodingType::FrameOfReference, {"Int"})},
      {"RunLength", EncodingAndSupportedDataTypes(EncodingType::RunLength, {"Int", "String"})},
      {"LZ4", EncodingAndSupportedDataTypes(EncodingType::LZ4, {"Int", "String"})}};

  const std::vector<double> selectivities{0.001, 0.01, 0.1, 0.3, 0.5, 0.7, 0.8, 0.9, 0.99};

  const std::map<std::string, std::function<std::shared_ptr<TableWrapper>(const EncodingType, const std::string)>>
      table_types{{"Int",
                   [&](const EncodingType encoding_type, const std::string mode) {
                     return create_table<int32_t>(DataType::Int, ROWS, generate_values<int32_t>, encoding_type, mode);
                   }},
                  {"String", [&](const EncodingType encoding_type, const std::string mode) {
                     return create_table<pmr_string>(DataType::String, ROWS, generate_values<pmr_string>, encoding_type,
                                                     mode);
                   }}};

  const std::vector<std::string> modes{"Sorted", "SortedDescending", "SortedNotMarked", "Shuffled"};
  const std::vector<bool> between_modes{false, true};

  for (const auto& encoding : encoding_types) {
    const auto encoding_name = encoding.first;
    const auto encoding_type = encoding.second.first;
    const auto supported_data_types = encoding.second.second;

    for (const auto selectivity : selectivities) {
      for (const auto& data_type : supported_data_types) {
        for (const auto& mode : modes) {
          for (const auto is_between_scan : between_modes) {
            const auto& table_generator = table_types.at(data_type);

            const std::string between_label = is_between_scan ? "Between" : "";
            benchmark::RegisterBenchmark(("BM_Table" + between_label + "ScanSorted/" + encoding_name + "/" +
                                          std::to_string(selectivity) + "/" + data_type + "/" + mode)
                                             .c_str(),
                                         BM_TableScanSorted, ROWS, selectivity, encoding_type, mode, is_between_scan,
                                         table_generator);
          }
        }
      }
    }
  }
}

// We need to call the registerTableScanSortedBenchmarks() to register the benchmarks. We could call it inside the
// micro_benchmark_main.cpp::main() method, but then these benchmarks would also be included when building the
// hyriseBenchmarkPlayground. Instead, we create a global object whose sole purpose is to register the benchmarks in its
// constructor.
class StartUp {
 public:
  StartUp() { registerTableScanSortedBenchmarks(); }
};
StartUp startup;

}  // namespace

}  // namespace opossum
