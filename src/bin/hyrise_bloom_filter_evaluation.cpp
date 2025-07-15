#include <cassert>
#include <chrono>
#include <cstdint>
#include <exception>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <boost/functional/hash.hpp>
#include <filesystem>

#include "utils/assert.hpp"
#include "utils/bloom_filter.hpp"
#include "xxhash.h"

using namespace hyrise;  // NOLINT(build/namespaces)

std::vector<int32_t> vector_sizes = {10'000, 100'000, 1'000'000, 10'000'000, 100'000'000};
std::vector<double> distinctivenesses = {0.01, 0.1, 0.5, 1.0, 2.0, 3.0};
std::vector<double> overlaps = {0.0, 0.25, 0.5, 0.75, 1.0};
uint8_t hash_functions = 3;
uint16_t min_runs = 10;
int64_t min_time_ns = 30'000'000'000;

struct BenchmarkResult {
  int32_t vector_size;
  double distinctiveness;
  double overlap;
  uint8_t filter_size;
  uint8_t k;
  uint8_t hash_function;
  uint16_t run;
  int64_t build_time_ns;
  int64_t probe_time_ns;
  int32_t hits;
  double saturation;
};

std::pair<std::vector<int32_t>, std::vector<int32_t>> generate_data(const int32_t vector_size,
                                                                    const double distinctiveness,
                                                                    const double overlap) {
  Assert(vector_size > 0, "vector_size must be greater than 0");
  Assert(distinctiveness > 0, "distinctiveness must be greater than 0");
  Assert(overlap >= 0 && overlap <= 1, "overlap must be between 0 and 1");

  const auto range_min_0 = double{0};
  const auto range_max_0 = static_cast<double>(vector_size) * distinctiveness;

  const auto range_min_1 = range_max_0 * (1 - overlap);
  const auto range_max_1 = range_max_0 + range_min_1;
  Assert(range_max_1 <= static_cast<double>(INT32_MAX), "range_max_1 exceeds INT32_MAX");

  std::cout << "Generating vectors of size: " << vector_size << ", distinctiveness: " << distinctiveness
            << ", and overlap: " << overlap << "\n";
  std::cout << "Range 0: [" << range_min_0 << ", " << static_cast<int32_t>(range_max_0) << "]\n";
  std::cout << "Range 1: [" << range_min_1 << ", " << static_cast<int32_t>(range_max_1) << "]\n";

  std::mt19937 gen0(4615968);
  std::mt19937 gen1(4615968);
  std::uniform_int_distribution<int32_t> dis0(static_cast<int32_t>(range_min_0), static_cast<int32_t>(range_max_0));
  std::uniform_int_distribution<int32_t> dis1(static_cast<int32_t>(range_min_1), static_cast<int32_t>(range_max_1));

  std::vector<int32_t> vec0;
  std::vector<int32_t> vec1;
  vec0.reserve(vector_size);
  vec1.reserve(vector_size);

  for (int32_t i = 0; i < vector_size; ++i) {
    vec0.emplace_back(dis0(gen0));
    vec1.emplace_back(dis1(gen1));
  }

  return {vec0, vec1};
}

template <typename F>
auto measure_duration(F&& f) {
  auto start = std::chrono::high_resolution_clock::now();
  f();
  auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();  // in ns
}

template <uint8_t FilterSize, uint8_t K>
void run_bloom_filter_evaluation(const std::vector<int32_t>& build_vec,
                                 const std::vector<int32_t>& probe_vec,
                                 const uint8_t hash_function, size_t vector_size,
                                 double distinctiveness, double overlap,
                                 const std::string& csv_filename) {
  std::cout << "Evaluation Bloom filter with size: " << std::to_string(FilterSize) << ", K: " << std::to_string(K)
            << ", and Hash: " << std::to_string(hash_function) << "\n";

  auto run = uint16_t{0};
  auto total_time = int64_t{0};

  std::ofstream out(csv_filename, std::ios::app);  // Open file in append mode
  if (!out.is_open()) {
    throw std::runtime_error("Failed to open CSV file for appending.");
  }

  while (run < min_runs || total_time < min_time_ns) {
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
          size_t seed = 0;
          boost::hash_combine(seed, val);
          bloom_filter.insert(static_cast<uint64_t>(seed));  // Use the generated random number as the hash
        }
      });
    } else if (hash_function == 2) {
      build_time = measure_duration([&]() {
        for (const auto& val : build_vec) {
          uint64_t hash = XXH3_64bits(&val, sizeof(val));
          bloom_filter.insert(hash);
        }
      });
    } else {
      Fail("Invalid hash function specified");
    }

    auto hits = int32_t{0};
    auto probe_time = int64_t{0};
    if (hash_function == 0) {
      probe_time = measure_duration([&]() {
        for (const auto& val : probe_vec) {
          if (bloom_filter.probe(std::hash<int32_t>{}(val)))
            ++hits;
        }
      });
    } else if (hash_function == 1) {
      probe_time = measure_duration([&]() {
        for (const auto& val : probe_vec) {
          size_t seed = 0;
          boost::hash_combine(seed, val);
          if (bloom_filter.probe(static_cast<uint64_t>(seed)))  // Use the generated random number as the hash
            ++hits;
        }
      });
    } else if (hash_function == 2) {
      probe_time = measure_duration([&]() {
        for (const auto& val : probe_vec) {
          uint64_t hash = XXH3_64bits(&val, sizeof(val));
          if (bloom_filter.probe(hash))
            ++hits;
        }
      });
    } else {
      Fail("Invalid hash function specified");
    }

    total_time += build_time + probe_time;
    BenchmarkResult result{static_cast<int32_t>(vector_size), distinctiveness, overlap, FilterSize, K,
                           hash_function, run, build_time, probe_time, hits, bloom_filter.saturation()};

    // Append result to CSV file
    out << result.vector_size << "," << result.distinctiveness << "," << result.overlap << ","
        << static_cast<int>(result.filter_size) << "," << static_cast<int>(result.k) << ","
        << static_cast<int>(result.hash_function) << "," << result.run << "," << result.build_time_ns << ","
        << result.probe_time_ns << "," << result.hits << "," << result.saturation << "\n";

    ++run;
  }

  std::cout << "Total runs: " << run << ", Total time: " << total_time << " ns\n";
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <output_csv_file>\n";
    return 1;
  }

  const std::string csv_filename = argv[1];
  if (std::filesystem::exists(csv_filename)) {
    std::cerr << "Error: File " << csv_filename << " already exists.\n";
    return 1;
  }

  // Write CSV header
  std::ofstream out(csv_filename);
  if (!out.is_open()) {
    std::cerr << "Error: Failed to create CSV file.\n";
    return 1;
  }
  out << "vector_size,distinctiveness,overlap,filter_size,k,hash_function,run,build_time_ns,probe_time_ns,hits,"
         "saturation\n";
  out.close();

#define RUN_EVALUATION(filter_size, k)                                                                           \
  {                                                                                                              \
    run_bloom_filter_evaluation<filter_size, k>(build_vec, probe_vec, hash_function, vector_size,                \
                                                distinctiveness, overlap, csv_filename);                        \
  }

  for (const auto vector_size : vector_sizes) {
    for (const auto distinctiveness : distinctivenesses) {
      for (const auto overlap : overlaps) {
        const auto [build_vec, probe_vec] = generate_data(vector_size, distinctiveness, overlap);

        for (uint8_t hash_function = 0; hash_function < hash_functions; ++hash_function) {
          RUN_EVALUATION(16, 1)
          RUN_EVALUATION(17, 1)
          RUN_EVALUATION(18, 1)
          RUN_EVALUATION(19, 1)
          RUN_EVALUATION(20, 1)
          RUN_EVALUATION(21, 1)
          RUN_EVALUATION(16, 2)
          RUN_EVALUATION(17, 2)
          RUN_EVALUATION(18, 2)
          RUN_EVALUATION(19, 2)
          RUN_EVALUATION(20, 2)
          RUN_EVALUATION(21, 2)
          RUN_EVALUATION(16, 3)
          RUN_EVALUATION(17, 3)
          RUN_EVALUATION(18, 3)
          RUN_EVALUATION(19, 3)
          RUN_EVALUATION(20, 3)
          RUN_EVALUATION(21, 3)
        }
      }
    }
  }

  return 0;
}
