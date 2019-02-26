#include <memory>
#include <numeric>
#include <random>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "expression/expression_functional.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
#include "type_cast.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {

int CHUNK_SIZE = 100'000;

std::shared_ptr<opossum::TableColumnDefinitions> create_column_definitions(const opossum::DataType data_type) {
  auto table_column_definitions = std::make_shared<opossum::TableColumnDefinitions>();

  // TODO(cmfcmf): Benchmark nullable segments
  table_column_definitions->emplace_back("a", data_type, false);

  return table_column_definitions;
}

}  // namespace

namespace opossum {

const int string_size = 512;

void _clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

std::shared_ptr<TableWrapper> create_int_table(const int table_size,
                                               const EncodingType encoding_type = EncodingType::Unencoded,
                                               const std::optional<OrderByMode> order_by = std::nullopt) {
  const auto table_column_definitions = create_column_definitions(DataType::Int);
  std::shared_ptr<Table> table;
  std::shared_ptr<TableWrapper> table_wrapper;

  table = std::make_shared<Table>(*table_column_definitions, TableType::Data);

  auto values = std::vector<int>(table_size);

  if (order_by.value_or(OrderByMode::Ascending) == OrderByMode::Ascending ||
      order_by.value() == OrderByMode::AscendingNullsLast) {
    std::iota(values.begin(), values.end(), 0);
  } else {
    std::iota(values.rbegin(), values.rend(), 0);
  }

  if (!order_by.has_value()) {
    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::shuffle(values.begin(), values.end(), generator);
  }

  for (int i = 0; i < table_size / CHUNK_SIZE; ++i) {
    const auto first = values.cbegin() + CHUNK_SIZE * i;
    const auto last = values.cbegin() + CHUNK_SIZE * (i + 1);
    const auto value_segment = std::make_shared<ValueSegment<int>>(std::vector(first, last));
    table->append_chunk({value_segment});
  }

  if (encoding_type != EncodingType::Unencoded) {
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec(encoding_type));
  }

  if (order_by.has_value()) {
    for (auto& chunk : table->chunks()) {
      chunk->get_segment(ColumnID(0))->set_sort_order(order_by.value());
    }
  }

  table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  return table_wrapper;
}

std::shared_ptr<TableWrapper> create_string_table(const int table_size, const int string_length,
                                                  const EncodingType encoding_type = EncodingType::Unencoded,
                                                  const std::optional<OrderByMode> order_by = std::nullopt) {
  const auto table_column_definitions = create_column_definitions(DataType::String);
  std::shared_ptr<Table> table;
  std::shared_ptr<TableWrapper> table_wrapper;

  table = std::make_shared<Table>(*table_column_definitions, TableType::Data);

  auto values = std::vector<std::string>(table_size);

  if (order_by.value() == OrderByMode::Ascending || order_by.value() == OrderByMode::AscendingNullsLast) {
    for (int i = 0; i < table_size; i++) {
      auto str = std::to_string(i);
      values[i] = std::string(string_length - str.length(), '0').append(str);
    }
  } else {
    for (int i = 0; i < table_size; i++) {
      auto str = std::to_string(table_size - i - 1);
      values[i] = std::string(string_length - str.length(), '0').append(str);
    }
  }

  if (!order_by.has_value()) {
    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::shuffle(values.begin(), values.end(), generator);
  }

  for (int i = 0; i < table_size / CHUNK_SIZE; ++i) {
    const auto first = values.cbegin() + CHUNK_SIZE * i;
    const auto last = values.cbegin() + CHUNK_SIZE * (i + 1);
    const auto value_segment = std::make_shared<ValueSegment<std::string>>(std::vector(first, last));
    table->append_chunk({value_segment});
  }

  if (encoding_type != EncodingType::Unencoded) {
    ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec(encoding_type));
  }

  if (order_by.has_value()) {
    for (auto& chunk : table->chunks()) {
      chunk->get_segment(ColumnID(0))->set_sort_order(order_by.value());
    }
  }

  table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  return table_wrapper;
}

void BM_TableScanSorted(
    benchmark::State& state, const int table_size, const double selectivity,
    const PredicateCondition predicate_condition, const EncodingType encoding_type,
    const std::optional<OrderByMode> order_by,
    std::function<std::shared_ptr<TableWrapper>(const int, const EncodingType, const OrderByMode)> table_creator) {
  _clear_cache();

  AllTypeVariant search_value;
  switch (predicate_condition) {
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::LessThan:
      search_value = static_cast<int>(table_size * selectivity);
      break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      search_value = static_cast<int>(table_size - table_size * selectivity);
      break;
    default:
      // TODO(cmfcmf) Improve logic for other predicates.
      Fail("Unsupported predicate condition");
  }
  const auto table_wrapper = table_creator(table_size, encoding_type, order_by.value_or(OrderByMode::Ascending));
  const auto table_column_definitions = table_wrapper->get_output()->column_definitions();

  const auto column_index = ColumnID(0);

  const auto column_definition = table_column_definitions.at(column_index);

  if (column_definition.data_type == DataType::String) {
    const auto str = std::to_string(type_cast_variant<int>(search_value));
    search_value = std::string(string_size - str.length(), '0').append(str);
  }

  const auto column_expression =
      pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

  auto predicate =
      std::make_shared<BinaryPredicateExpression>(predicate_condition, column_expression, value_(search_value));

  auto warm_up = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up->execute();
  for (auto _ : state) {
    auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

std::shared_ptr<TableWrapper> sorted_int_table(const int table_size, const EncodingType encoding_type,
                                               const OrderByMode order_by) {
  return create_int_table(table_size, encoding_type, std::make_optional(order_by));
}

std::shared_ptr<TableWrapper> unsorted_int_table(const int table_size, const EncodingType encoding_type,
                                                 const OrderByMode order_by) {
  return create_int_table(table_size, encoding_type);
}

std::shared_ptr<TableWrapper> sorted_string_table(const int table_size, const EncodingType encoding_type,
                                                  const OrderByMode order_by, const int string_length) {
  return create_string_table(table_size, string_length, encoding_type, std::make_optional(order_by));
}

std::shared_ptr<TableWrapper> unsorted_string_table(const int table_size, const EncodingType encoding_type,
                                                    const OrderByMode order_by, const int string_length) {
  return create_string_table(table_size, string_length, encoding_type);
}

void registerTableScanSortedBenchmarks() {
  const auto rows = 1'000'000;

  const std::vector<double> selectivities{0.001, 0.01, 0.1, 0.3, 0.5, 0.7, 0.8, 0.9, 0.99};

  const std::map<std::string, PredicateCondition> predicates{
      {"LessThanEquals", PredicateCondition::LessThanEquals},
      // {"Equals", PredicateCondition::Equals},
      // {"GreaterThan", PredicateCondition::GreaterThan}
  };

  const std::map<
      std::string,
      std::pair<std::function<std::shared_ptr<TableWrapper>(const int, const EncodingType, const OrderByMode)>,
                std::function<std::shared_ptr<TableWrapper>(const int, const EncodingType, const OrderByMode)>>>
      table_types{{"Int", std::make_pair(sorted_int_table, unsorted_int_table)},
                  {"String", std::make_pair(std::bind(sorted_string_table, std::placeholders::_1, std::placeholders::_2,
                                                      std::placeholders::_3, string_size),
                                            std::bind(unsorted_string_table, std::placeholders::_1,
                                                      std::placeholders::_2, std::placeholders::_3, string_size))}};

  const std::map<std::string, OrderByMode> order_bys{
      {"AscendingNullsFirst", OrderByMode::Ascending},
      {"AscendingNullsLast", OrderByMode::AscendingNullsLast},
      {"DescendingNullsFirst", OrderByMode::Descending},
      {"DescendingNullsLast", OrderByMode::DescendingNullsLast},
  };

  const std::map<std::string, EncodingType> encoding_types{{"None", EncodingType::Unencoded},
                                                           {"Dictionary", EncodingType::Dictionary},
                                                           {"RunLength", EncodingType::RunLength}};

  for (const auto& table_type : table_types) {
    const auto data_type = table_type.first;
    const auto sorted_table = table_type.second.first;
    const auto unsorted_table = table_type.second.second;

    for (const auto& predicate : predicates) {
      const auto predicate_name = predicate.first;
      const auto predicate_condition = predicate.second;

      for (const auto& order_by : order_bys) {
        const auto order_by_name = order_by.first;
        const auto order_by_mode = order_by.second;

        for (const auto& encoding : encoding_types) {
          const auto encoding_name = encoding.first;
          const auto encoding_type = encoding.second;

          for (const auto selectivity : selectivities) {
            benchmark::RegisterBenchmark(
                ("BM_TableScanSorted/" + data_type + "/" + predicate_name + "/" + order_by_name + "/" + encoding_name +
                 "/" + std::to_string(selectivity) + "/" + "Sorted")
                    .c_str(),
                BM_TableScanSorted, rows, selectivity, predicate_condition, encoding_type, order_by_mode, sorted_table);
            benchmark::RegisterBenchmark(
                ("BM_TableScanSorted/" + data_type + "/" + predicate_name + "/" + order_by_name + "/" + encoding_name +
                 "/" + std::to_string(selectivity) + "/" + "Unsorted")
                    .c_str(),
                BM_TableScanSorted, rows, selectivity, predicate_condition, encoding_type, std::nullopt,
                unsorted_table);
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

}  // namespace opossum
