#include "single_distribution_benchmark.hpp"

#include <iostream>
#include <fstream>
#include <sstream>

#include "json.hpp"

#include "to_string.hpp"

namespace opossum {

void SingleDistributionBenchmark::run() {
  const auto distr_info = DistributionInfo{row_count, max_value, sorted, null_fraction};
  auto generator = get_generator(distr_info);

  std::cout << "Begin Encoding Type: Unencoded" << std::endl;

  auto [value_column, allocated_memory] = memory_consumption([g = generator]() { return g(); });

  _generate_statistics(*value_column);

  auto benchmark_state = benchmark_table_scan(value_column, max_value / 2);

  auto results = benchmark_state.results();
  auto num_iterations = benchmark_state.num_iterations();
  _result_sets.push_back({{EncodingType::Unencoded}, num_iterations, allocated_memory, to_ms(results)});

  for (const auto& encoding_spec : _encoding_specs()) {

    std::cout << "Begin Encoding Type: " << to_string(encoding_spec) << std::endl;

    auto encoder = create_encoder(encoding_spec.encoding_type);

    if (encoding_spec.vector_compression_type) {
      encoder->set_vector_compression(*encoding_spec.vector_compression_type);
    }

    auto [encoded_column, allocated_memory] = memory_consumption(
        [&, vc = value_column]() { return encoder->encode(vc, DataType::Int); });

    auto benchmark_state = benchmark_table_scan(encoded_column, max_value / 2);

    auto results = benchmark_state.results();
    auto num_iterations = benchmark_state.num_iterations();

    _result_sets.push_back({encoding_spec, num_iterations, allocated_memory, to_mis(results, row_count)});
  }

  _create_report();
}

void SingleDistributionBenchmark::_create_report() const {
  nlohmann::json benchmarks;

  for (const auto& result_set : _result_sets) {
    nlohmann::json benchmark{
      {"encoding_spec", to_string(result_set.encoding_spec)},
      {"iterations", result_set.num_iterations},
      {"allocated_memory", result_set.allocated_memory},
      {"results", result_set.results_in_mis}};

    benchmarks.push_back(std::move(benchmark));
  }

  /**
   * Generate YY-MM-DD hh:mm::ss
   */
  auto current_time = std::time(nullptr);
  auto local_time = *std::localtime(&current_time);
  std::stringstream timestamp_stream;
  timestamp_stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");

  nlohmann::json context{
      {"date", timestamp_stream.str()},
      {"build_type", IS_DEBUG ? "debug" : "release"},
      {"distribution", name},
      {"row_count", row_count},
      {"run_count", _run_count},
      {"max_value", max_value},
      {"null_fraction", null_fraction},
      {"sorted", sorted},
      {"comment", comment}};

  nlohmann::json report{{"context", context}, {"benchmarks", benchmarks}};

  std::stringstream file_name;
  file_name << "single_distribution_" << std::put_time(&local_time, "%Y-%m-%d_%H-%M-%S") << ".json";

  auto output_file = std::ofstream(file_name.str());
  output_file << std::setw(2) << report << std::endl;
}

void SingleDistributionBenchmark::_generate_statistics(const ValueColumn<int32_t>& value_column) {
  auto iterable = ValueColumnIterable<int32_t>{value_column};

  _run_count = 0u;

  iterable.with_iterators([this](auto it, auto end) {
    auto prev_value = int32_t{it->value() + 1};

    for (; it != end; ++it) {
      auto column_value = *it;

      if (column_value.value() != prev_value) {
        prev_value = column_value.value();
        ++_run_count;
      }
    }
  });
}

}  // namespace opossum
