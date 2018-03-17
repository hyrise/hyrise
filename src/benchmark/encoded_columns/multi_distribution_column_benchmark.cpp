#include "multi_distribution_column_benchmark.hpp"

#include <algorithm>
#include <ctime>
#include <cmath>
#include <iostream>
#include <fstream>
#include <sstream>
#include <set>

#include "utils/assert.hpp"
#include "storage/column_encoding_utils.hpp"
#include "storage/base_column_encoder.hpp"
#include "storage/value_column/value_column_iterable.hpp"

#include "from_string.hpp"

namespace opossum {

MultiDistributionColumnBenchmark::MultiDistributionColumnBenchmark(CalibrationType calibration_type,
                                                                   nlohmann::json description)
    : _calibration_type(calibration_type), _description(std::move(description)), _context(_description["context"]) {}

void MultiDistributionColumnBenchmark::run() {
  std::cout << "Benchmark started: " << to_string(_calibration_type) << std::endl;

  auto remaining_count = _description["benchmarks"].size();
  for (auto& benchmark : _description["benchmarks"]) {
    std::cout << "Benchmark: " << benchmark << std::endl;
    std::cout << "Remaining: " << remaining_count-- << std::endl;

    const auto row_count = _value_or_default(benchmark, "row_count").get<int>();
    const auto max_value = _value_or_default(benchmark, "max_value").get<int>();
    const auto sorted = _value_or_default(benchmark, "sorted").get<bool>();
    const auto null_fraction = _value_or_default(benchmark, "null_fraction").get<float>();
    const auto distr_info = DistributionInfo{
        static_cast<uint32_t>(row_count),
        static_cast<uint32_t>(max_value),
        sorted,
        null_fraction};

    const auto generator = get_generator(distr_info);
    const auto value_column = generator();

    _generate_statistics(benchmark, value_column);

    const auto encoding_type_str = _value_or_default(benchmark, "encoding_type").get<std::string>();
    const auto encoding_type = from_string<EncodingType>(encoding_type_str);

    const auto encoded_column = [&]() -> std::shared_ptr<BaseColumn> {
      if (encoding_type == EncodingType::Unencoded) {
        return value_column;
      }

      auto encoder = create_encoder(encoding_type);

      if (encoder->uses_vector_compression()) {
        const auto vector_compression_type_str =
            _value_or_default(benchmark, "vector_compression_type").get<std::string>();

        const auto vector_compression_type = from_string<VectorCompressionType>(vector_compression_type_str);

        encoder->set_vector_compression(vector_compression_type);
      }

      return encoder->encode(value_column, DataType::Int);
    }();

    // Get any value that is not NULL
    const auto select_eq = [&]() {
      for (auto row_id = 0u; row_id < value_column->size(); ++row_id) {
        if (!value_column->is_null(row_id))
          return value_column->get(row_id);
      }

      Fail("Ups");
    }();

    const auto point_access_factor = _value_or_default(benchmark, "point_access_factor").get<float>();

    auto benchmark_state = [&]() {
      switch (_calibration_type) {
        case CalibrationType::CompleteTableScan:
          return benchmark_table_scan(encoded_column, select_eq);
        case CalibrationType::FilteredTableScan:
          return benchmark_table_scan(encoded_column, select_eq, point_access_factor);
        case CalibrationType::Materialization:
          return benchmark_materialize(encoded_column, point_access_factor);
        default:
          Fail("Unrecognized type.");
      }
    }();

    auto results_in_ms = to_ms(benchmark_state.results());

    benchmark["runtime"] = std::move(results_in_ms);

    std::cout << "Iterations: " << benchmark_state.num_iterations() << std::endl;
  }

  _output_as_json(_description);
}

void MultiDistributionColumnBenchmark::_output_as_json(nlohmann::json& data) {
  /**
   * Generate YY-MM-DD hh:mm::ss
   */
  auto current_time = std::time(nullptr);
  auto local_time = *std::localtime(&current_time);
  std::stringstream timestamp_stream;
  timestamp_stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");

  auto& context = data["context"];
  context["date"] = timestamp_stream.str();
  context["build_type"] = IS_DEBUG ? "debug" : "release";

  std::stringstream file_name;
  file_name << "results_" << std::put_time(&local_time, "%Y-%m-%d_%H-%M-%S") << ".json";

  auto output_file = std::ofstream(file_name.str());
  output_file << std::setw(2) << data << std::endl;
}

void MultiDistributionColumnBenchmark::_output_as_csv(const nlohmann::json& data) {
  auto benchmarks_out = data["benchmarks"];

  for (auto& benchmark : benchmarks_out) {
    // Copy values from context into benchmark if missing
    if (!_context.is_null()) {
      for (const auto& [key, value] : _context.get<nlohmann::json::object_t>()) {
        if (benchmark.find(key) == benchmark.end()) {
          benchmark[key] = value;
        }
      }
    }

    const auto& encoding_type_it = benchmark.find("encoding_type");
    const auto& vector_compression_type_it = benchmark.find("vector_compression_type");

    // Merge encoding and vector compression type
    if (vector_compression_type_it != benchmark.end()) {
      std::stringstream encoding;
      encoding << encoding_type_it->get<std::string>() << " (" << vector_compression_type_it->get<std::string>() << ")";
      benchmark["encoding"] = encoding.str();
      benchmark.erase(vector_compression_type_it);
    } else {
      benchmark["encoding"] = *encoding_type_it;
    }
    benchmark.erase(encoding_type_it);
  }

  /**
   * Generate YY-MM-DD hh:mm::ss
   */
  auto current_time = std::time(nullptr);
  auto local_time = *std::localtime(&current_time);

  std::stringstream file_name;
  file_name << to_string(_calibration_type) << '_' << std::put_time(&local_time, "%Y-%m-%d_%H-%M-%S") << ".csv";

  auto output_file = std::ofstream(file_name.str());

  // Write header
  const auto& first_benchmark = benchmarks_out.front().get<nlohmann::json::object_t>();
  for (auto benchmark_it = first_benchmark.cbegin(); benchmark_it != first_benchmark.cend(); ++benchmark_it) {
    const auto& key = benchmark_it->first;

    output_file << key;

    auto next_benchmark_it = benchmark_it;
    if (++next_benchmark_it != first_benchmark.cend()) {
      output_file << ',';
    } else {
      output_file << std::endl;
    }
  }

  // Write rows
  for (const auto& benchmark : benchmarks_out) {
    const auto& benchmark_object = benchmark.get<nlohmann::json::object_t>();

    const auto benchmark_runtimes_it = benchmark_object.find("runtime");
    const auto& runtimes = benchmark_runtimes_it->second;

    for (auto runtime_it = runtimes.cbegin(); runtime_it != runtimes.cend(); ++runtime_it) {
      for (auto benchmark_it = benchmark_object.cbegin(); benchmark_it != benchmark_object.cend(); ++benchmark_it) {

        const auto& value = [&]() {
          if (benchmark_it == benchmark_runtimes_it) {
            return *runtime_it;
          } else {
            return benchmark_it->second;
          }
        }();

        output_file << value;

        auto next_benchmark_it = benchmark_it;
        if (++next_benchmark_it != benchmark_object.cend()) {
          output_file << ',';
        } else {
          output_file << std::endl;
        }
      }
    }

  }
}

void MultiDistributionColumnBenchmark::_generate_statistics(nlohmann::json& benchmark,
                                                            std::shared_ptr<ValueColumn<int32_t>> value_column) {
  auto run_count = 0;
  auto value_set = std::set<AllTypeVariant>{};

  auto current_value = variant_is_null((*value_column)[0]) ? AllTypeVariant{13} : NULL_VALUE;
  for (auto row_id = 0u; row_id < value_column->size(); ++row_id) {
    const auto previous_value = current_value;
    current_value = (*value_column)[row_id];

    value_set.insert(current_value);

    if (variant_is_null(current_value) && variant_is_null(previous_value)) continue;

    if (current_value != previous_value) {
      ++run_count;
    }
  }

  const auto unique_count = value_set.size();
  const auto selectivity = 1.0 / unique_count;

  benchmark["run_count"] = run_count;
  benchmark["unique_count"] = unique_count;
  benchmark["selectivity"] = selectivity;
}

nlohmann::json MultiDistributionColumnBenchmark::_value_or_default(const nlohmann::json& benchmark,
                                                                   const std::string& key) const {
  const auto benchmark_it = benchmark.find(key);
  if (benchmark_it == benchmark.end()) {
    const auto context_it = _context.find(key);
    Assert(context_it != _context.end(), "Key \"" + key + "\" must be in either benchmark or context");
    return *context_it;
  }
  return *benchmark_it;
}

}  // namespace opossum
