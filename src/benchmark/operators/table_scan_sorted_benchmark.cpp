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
  table_column_definitions.emplace_back("a", data_type, false);
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

  if (mode == "Shuffled") {
    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::shuffle(values.begin(), values.end(), generator);
  }

  for (auto chunk_index = 0u; chunk_index < table_size / CHUNK_SIZE; ++chunk_index) {
    const auto first = values.cbegin() + CHUNK_SIZE * chunk_index;
    const auto last = values.cbegin() + CHUNK_SIZE * (chunk_index + 1);
    const auto value_segment = std::make_shared<ValueSegment<Type>>(std::vector(first, last));
    table->append_chunk({value_segment});
  }

  if (encoding_type != EncodingType::Unencoded) {
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec(encoding_type));
  }

  if (mode == "Sorted") {
    const auto chunk_count = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      chunk->set_ordered_by(std::make_pair(ColumnID(0), OrderByMode::Ascending));
    }
  }

  table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();
  return table_wrapper;
}

}  // namespace

void BM_TableScanSorted(
    benchmark::State& state, const int table_size, const double selectivity, const EncodingType encoding_type,
    const std::string& mode,
    std::function<std::shared_ptr<TableWrapper>(const EncodingType, const std::string)> table_creator) {
  micro_benchmark_clear_cache();

  // The benchmarks all run with different selectivities (ratio of values in the output to values in the input).
  // At this point the search value is selected in a way that our results correspond to the chosen selectivity.
  auto search_value = AllTypeVariant{};

  const auto table_wrapper = table_creator(encoding_type, mode);
  const auto table_column_definitions = table_wrapper->get_output()->column_definitions();

  const auto column_index = ColumnID(0);

  const auto column_definition = table_column_definitions.at(column_index);

  resolve_data_type(column_definition.data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      search_value = pad_string(std::to_string(table_size * selectivity), STRING_SIZE);
    } else {
      search_value = static_cast<ColumnDataType>(table_size * selectivity);
    }
  });

  const auto column_expression =
      pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

  auto predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, column_expression,
                                                               value_(search_value));
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

  const std::vector<std::string> modes{"Sorted", "SortedNotMarked", "Shuffled"};

  for (const auto& encoding : encoding_types) {
    const auto encoding_name = encoding.first;
    const auto encoding_type = encoding.second.first;
    const auto supported_data_types = encoding.second.second;

    for (const auto selectivity : selectivities) {
      for (const auto& data_type : supported_data_types) {
        for (const auto& mode : modes) {
          const auto& table_generator = table_types.at(data_type);

          benchmark::RegisterBenchmark(
              ("BM_TableScanSorted/" + encoding_name + "/" + std::to_string(selectivity) + "/" + data_type + "/" + mode)
                  .c_str(),
              BM_TableScanSorted, ROWS, selectivity, encoding_type, mode, table_generator);
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
