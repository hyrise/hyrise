#include <algorithm>
#include <iostream>
#include <random>

#include "types.hpp"
#include "utils/timer.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

constexpr auto MEASUREMENT_COUNT = size_t{10};
constexpr auto VECTOR_SIZE = size_t{5'000'000'000};
constexpr auto RANDOM_ACCESS_COUNT = size_t{10'000'000};

template <bool PREFETCH, int LOCALITY = 0>
std::pair<size_t, std::chrono::duration<double>> run_experiment(const std::vector<uint32_t>& vec, const std::vector<uint64_t>& positions, uint16_t prefetch_offset) {
  auto sum = size_t{0};
  const auto position_list_size = positions.size();

  auto timer = Timer{};
  const auto start = std::chrono::steady_clock::now();  
  for (auto pos_list_offset = size_t{0}; pos_list_offset < position_list_size; ++pos_list_offset) {
    if constexpr (PREFETCH) {
      __builtin_prefetch(&vec[positions[std::min(position_list_size - 1, pos_list_offset + prefetch_offset)]], 0, LOCALITY);
    }

    sum += vec[positions[pos_list_offset]] % 17;
  }
  const auto end = std::chrono::steady_clock::now();
  const auto duration = std::chrono::duration<double>(end - start);
  std::cout << timer.lap_formatted() << " (sum is " << sum << ")" << std::endl;

  return {sum, duration};
}

int main() {
  const auto offsets = std::vector<uint16_t>{1, 2, 4, 8, 16, 32, 64, 128, 256};

  auto random_engine = std::default_random_engine();
  auto distribution = std::uniform_int_distribution<uint64_t>(0, VECTOR_SIZE - 1);

  auto vec = std::vector<uint32_t>(VECTOR_SIZE, 17);

  auto row_index = size_t{0};
  for (auto& element : vec) {
    element += row_index % 16;
    ++row_index;
  }

  auto access_positions = std::vector<uint64_t>{};
  access_positions.reserve(RANDOM_ACCESS_COUNT);

  for (auto index = size_t{0}; index < RANDOM_ACCESS_COUNT; ++index) {
    access_positions.emplace_back(distribution(random_engine));
  }

  for (auto locality = int{0}; locality < 4; ++locality) {
    auto random_device = std::random_device{};
    auto generator = std::mt19937{random_device()};
    std::shuffle(access_positions.begin(), access_positions.end(), generator);

    std::cout << "Locality of " << locality << "\n\n";
    for (auto measurement_id = size_t{0}; measurement_id < MEASUREMENT_COUNT + 1; ++measurement_id) {
      const auto& [result, runtime] = run_experiment<false>(vec, access_positions, 0);
      for (const auto offset : offsets) {
        const auto& [prefetch_result, prefetch_runtime] = run_experiment<true>(vec, access_positions, offset);
        if (prefetch_result != result) {
          std::exit(1);
        }
      }
      std::cout << "\n";
    }
    std::cout << "\n\n";
  }

  return 0;
}
