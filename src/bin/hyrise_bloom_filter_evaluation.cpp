#include <cassert>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include <boost/functional/hash.hpp>

#include "utils/bloom_filter.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

struct BenchmarkResult {
  size_t vector_size;
  size_t hitrate;
  double distinctiveness;
  uint8_t filter_size;
  uint8_t k;
  uint8_t hash_function;
  int64_t build_time_us;
  int64_t probe_time_us;
};

std::vector<int32_t> generate_data(const int32_t vector_size, const double distinctiveness,
                                   const int32_t range_min = 0) {
  Assert(vector_size > 0, "vector_size must be greater than 0");
  Assert(distinctiveness > 0, "distinctiveness must be greater than 0");
  Assert(range_min >= 0, "range_min must be non-negative");

  const auto range_max = static_cast<double>(vector_size) * distinctiveness + static_cast<double>(range_min);
  Assert(range_max <= static_cast<double>(INT32_MAX), "range_max exceeds INT32_MAX");

  std::cout << "Generating vector of size: " << vector_size << " and distinctiveness: " << distinctiveness << "\n";
  std::cout << "Range: [" << range_min << ", " << static_cast<int32_t>(range_max) << "]\n";

  std::mt19937 gen(4615968);
  std::uniform_int_distribution<int32_t> dis(range_min, static_cast<int32_t>(range_max));

  std::vector<int32_t> result;
  result.reserve(vector_size);

  for (int32_t i = 0; i < vector_size; ++i) {
    result.emplace_back(dis(gen));
  }

  return result;
}

template <typename F>
auto measure_duration(F&& f) {
  auto start = std::chrono::high_resolution_clock::now();
  f();
  auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();  // in µs
}

template <uint8_t FilterSize, uint8_t K>
BenchmarkResult run_bloom_filter_evaluation(const std::vector<int32_t>& build_vec, const std::vector<int32_t>& probe_vec, const uint8_t hash_function, size_t vector_size, double distinctiveness) {
  std::cout << "Evaluation Bloom filter with size: " << std::to_string(FilterSize) << ", K: " << std::to_string(K)
            << ", and Hash: " << std::to_string(hash_function) << "\n";

  auto bloom_filter = BloomFilter<FilterSize, K>{};

  auto build_time = int64_t{0};
  if (hash_function == 0) {
    build_time = measure_duration([&]() {
      for (const auto& val : build_vec) {
        bloom_filter.insert(std::hash<int32_t>{}(val));
      }
    });
  } else if (hash_function == 1) {
    build_time = measure_duration([&]() {
      for (const auto& val : build_vec) {
        bloom_filter.insert(boost::hash<int32_t>{}(val));
      }
    });
  } else {
    Fail("Invalid hash function specified");
  }

  size_t hits = 0;
  auto probe_time = int64_t{0};
  if (hash_function == 0) {
    probe_time = measure_duration([&]() {
      for (const auto& val : probe_vec) {
        if (bloom_filter.probe(std::hash<int32_t>{}(val))) ++hits;
      }
    });
  } else if (hash_function == 1) {
    probe_time = measure_duration([&]() {
      for (const auto& val : probe_vec) {
        if (bloom_filter.probe(boost::hash<int32_t>{}(val))) ++hits;
      }
    });
  } else {
    Fail("Invalid hash function specified");
  } 

  return BenchmarkResult{
    vector_size,
    hits,
    distinctiveness,
    FilterSize,
    K,
    hash_function,
    build_time,
    probe_time
  };
}

void write_csv(const std::vector<BenchmarkResult>& results, const std::string& filename) {
  std::ofstream out(filename);
  out << "vector_size,hitrate,distinctiveness,filter_size,k,hash_function,build_time_us,probe_time_us\n";
  for (const auto& r : results) {
    out << r.vector_size << "," << r.hitrate << "," << r.distinctiveness << ","
        << static_cast<int>(r.filter_size) << "," << static_cast<int>(r.k) << ","
        << static_cast<int>(r.hash_function) << "," << r.build_time_us << "," << r.probe_time_us << "\n";
  }
}

std::vector<int32_t> vector_sizes = {10'000, 1'000'000, 100'000'000};
std::vector<double> distinctivenesses = {0.01, 0.1, 1};
uint8_t hash_functions = 2;

int main() {
  std::vector<BenchmarkResult> all_results;

  for (const auto vector_size : vector_sizes) {
    for (const auto distinctiveness : distinctivenesses) {
      const auto build_vec = generate_data(vector_size, distinctiveness);
      const auto probe_vec = generate_data(vector_size, distinctiveness);

      for (uint8_t hash_function = 0; hash_function < hash_functions; ++hash_function) {
        all_results.emplace_back(run_bloom_filter_evaluation<16, 1>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<17, 1>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<18, 1>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<19, 1>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<20, 1>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<21, 1>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<16, 2>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<17, 2>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<18, 2>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<19, 2>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<20, 2>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<21, 2>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<16, 3>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<17, 3>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<18, 3>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<19, 3>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<20, 3>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
        all_results.emplace_back(run_bloom_filter_evaluation<21, 3>(build_vec, probe_vec, hash_function, vector_size, distinctiveness));
      }
    }
  }

  write_csv(all_results, "bloom_filter_results.csv");
  return 0;
}
