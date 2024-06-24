#include <iostream>
#include <random>

#include "types.hpp"
#include "utils/timer.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

constexpr auto TEMPORAL_LOCALITY = 0; // 3 means going from 230ms to 240 ms.
constexpr auto MEASUREMENT_COUNT = size_t{10};

void prefetch_batch(const std::vector<uint32_t>& vec, const std::vector<uint64_t>& positions, size_t pos_list_offset, size_t offset, size_t count) {
  if (pos_list_offset % count == 0) {
    if (pos_list_offset == 0) {
      for (auto prefetch_offset = size_t{0}; prefetch_offset < count; ++prefetch_offset) {
        __builtin_prefetch(&vec[positions[prefetch_offset]], 0, TEMPORAL_LOCALITY);
      }
    }

    const auto position_list_size = positions.size();
    for (auto prefetch_offset = pos_list_offset + offset; prefetch_offset < pos_list_offset + offset + count && prefetch_offset < position_list_size; ++prefetch_offset) {
      __builtin_prefetch(&vec[positions[prefetch_offset]], 0, TEMPORAL_LOCALITY);
    }
  }
}

void prefetch_single(const std::vector<uint32_t>& vec, const std::vector<uint64_t>& positions, size_t pos_list_offset, size_t offset, size_t /* count */) {
  if (pos_list_offset == 0) {
    // We prefetch half of the offset.
    for (auto prefetch_offset = offset / 2; prefetch_offset < offset; ++prefetch_offset) {
      __builtin_prefetch(&vec[positions[prefetch_offset]], 0, TEMPORAL_LOCALITY);
    }
  }

  if (pos_list_offset + offset < positions.size()) {
    __builtin_prefetch(&vec[positions[pos_list_offset + offset]], 0, TEMPORAL_LOCALITY);
  }
}

void prefetch_no(const std::vector<uint32_t>& /* vec */, const std::vector<uint64_t>& /* positions */, size_t /* pos_list_offset */, size_t /* offset */, size_t /* count */) {
  return;
}

template <typename F>
void run_experiment(const std::vector<uint32_t>& vec, const std::vector<uint64_t>& positions, size_t offset, size_t count, F functor) {
  auto sum = size_t{0};
  auto timer = Timer{};
  const auto position_list_size = positions.size();
  for (auto pos_list_offset = size_t{0}; pos_list_offset < position_list_size; ++pos_list_offset) {
    functor(vec, positions, pos_list_offset, offset, count);
    sum += vec[positions[pos_list_offset]] % 17;
  }
  std::cout << timer.lap_formatted() << " (sum is " << sum << ")" << std::endl;
}

int main() {
  constexpr auto VECTOR_SIZE = size_t{5'000'000'000};
  constexpr auto RANDOM_ACCESS_COUNT = size_t{10'000'000};

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

  for (auto measurement_id = size_t{0}; measurement_id < MEASUREMENT_COUNT + 1; ++measurement_id) {
    run_experiment(vec, access_positions, 0, 0, prefetch_no);
    run_experiment(vec, access_positions, 4, 0, prefetch_single);
    run_experiment(vec, access_positions, 8, 0, prefetch_single);
    run_experiment(vec, access_positions, 16, 0, prefetch_single);
    run_experiment(vec, access_positions, 32, 0, prefetch_single);
    run_experiment(vec, access_positions, 48, 0, prefetch_single);
    run_experiment(vec, access_positions, 64, 0, prefetch_single);
    run_experiment(vec, access_positions, 4, 8, prefetch_batch);
    run_experiment(vec, access_positions, 8, 16, prefetch_batch);
    run_experiment(vec, access_positions, 16, 32, prefetch_batch);
    std::cout << "\n";
  }

  return 0;
}