#include "generate_benchmark_scenarios.hpp"

#include <vector>
#include <map>

#include "utils/assert.hpp"

namespace opossum {

nlohmann::json generate_benchmark_scenarios() {
  const auto context = nlohmann::json{
    {"sorted", false},
    {"null_fraction", 0.0},
    {"row_count", 1'000'000}};

  // const auto max_values = [&]() {
  //   auto v = std::vector<int>(21u);
  //   std::generate(v.begin(), v.end(), [n=0u]() mutable {
  //     const auto max_value = (1u << n) - 1u;
  //     n += 1;
  //     return max_value;
  //   });
  //   return v;
  // }();

  const auto max_values = [&]() {
    auto v = std::vector<int>(18u);
    std::generate(v.begin(), v.end(), [n=0u]() mutable {
      const auto max_value = (1u << n);
      n += 1;
      return max_value;
    });
    return v;
  }();

  static const auto encodings = std::vector<nlohmann::json>{
    {{"encoding_type", "Unencoded"}},
    {{"encoding_type", "Dictionary"}, {"vector_compression_type", "FSBA"}},
    {{"encoding_type", "Dictionary"}, {"vector_compression_type", "SIMD-BP128"}},
    {{"encoding_type", "FOR"}, {"vector_compression_type", "FSBA"}},
    {{"encoding_type", "FOR"}, {"vector_compression_type", "SIMD-BP128"}},
    {{"encoding_type", "Run Length"}}};

  nlohmann::json benchmarks;

  for (auto encoding : encodings)
    for (auto max_value : max_values) {
          auto benchmark = nlohmann::json{{"max_value", max_value}};

          benchmark.insert(encoding.begin(), encoding.end());
          benchmarks.push_back(benchmark);
        }

  return {{"context", context}, {"benchmarks", benchmarks}};
}

}  // namespace opossum
