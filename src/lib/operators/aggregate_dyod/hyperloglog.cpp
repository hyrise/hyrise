#include "operators/aggregate_dyod/hyperloglog.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>

namespace hyrise {

namespace {

constexpr auto REGISTER_COUNT = size_t{1} << HLL_PRECISION;
constexpr auto REMAINING_HASH_BITS = 64 - HLL_PRECISION;

size_t next_power_of_two(const size_t value) {
  if (value <= 1) {
    return 1;
  }

  constexpr auto max_power_of_two = size_t{1} << (std::numeric_limits<size_t>::digits - 1);
  if (value > max_power_of_two) {
    return max_power_of_two;
  }

  return size_t{1} << (std::numeric_limits<size_t>::digits - std::countl_zero(value - 1));
}

size_t ceil_divide(const size_t dividend, const size_t divisor) {
  return (dividend / divisor) + (dividend % divisor == 0 ? 0 : 1);
}

}  // namespace

HllSketch::HllSketch() : _registers(REGISTER_COUNT, uint8_t{0}) {}

void HllSketch::add(const uint64_t key_hash) {
  const auto register_index = static_cast<size_t>(key_hash >> REMAINING_HASH_BITS);
  const auto remaining_hash_bits = key_hash << HLL_PRECISION;

  const auto rank = remaining_hash_bits == 0
                        ? static_cast<uint8_t>(REMAINING_HASH_BITS + 1)
                        : static_cast<uint8_t>(std::countl_zero(remaining_hash_bits) + 1);
  _registers[register_index] = std::max(_registers[register_index], rank);
}

void HllSketch::merge(const HllSketch& other) {
  for (auto register_index = size_t{0}; register_index < REGISTER_COUNT; ++register_index) {
    _registers[register_index] = std::max(_registers[register_index], other._registers[register_index]);
  }
}

size_t HllSketch::estimate() const {
  auto inverse_sum = 0.0;
  auto zero_register_count = size_t{0};

  for (const auto rank : _registers) {
    inverse_sum += std::ldexp(1.0, -static_cast<int>(rank));
    if (rank == 0) {
      ++zero_register_count;
    }
  }

  constexpr auto register_count = static_cast<double>(REGISTER_COUNT);
  constexpr auto alpha = 0.7213 / (1.0 + 1.079 / register_count);
  const auto raw_estimate = alpha * register_count * register_count / inverse_sum;

  if (raw_estimate <= 2.5 * register_count && zero_register_count > 0) {
    return static_cast<size_t>(
        std::round(register_count * std::log(register_count / static_cast<double>(zero_register_count))));
  }

  if (raw_estimate >= static_cast<double>(std::numeric_limits<size_t>::max())) {
    return std::numeric_limits<size_t>::max();
  }

  return static_cast<size_t>(std::round(raw_estimate));
}

PartitionCount choose_partition_count(const size_t cardinality_estimate, const size_t worker_count) {
  const auto maximum_partition_count = static_cast<size_t>(MAX_PARTITION_COUNT);
  const auto minimum_partition_count = std::min(next_power_of_two(std::max(size_t{1}, worker_count)),
                                                maximum_partition_count);
  const auto target_partition_count = next_power_of_two(std::max(size_t{1}, ceil_divide(cardinality_estimate,
                                                                                        KEYS_BUDGET)));
  const auto partition_count = std::clamp(target_partition_count, minimum_partition_count, maximum_partition_count);

  return static_cast<PartitionCount>(partition_count);
}

}  // namespace hyrise
