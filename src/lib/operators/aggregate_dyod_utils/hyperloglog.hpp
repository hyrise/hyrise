#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>

namespace hyrise {

// A HyperLogLog sketch over precomputed 64-bit key hashes. `Precision` bits of each hash select one of the
// `2^Precision` one-byte registers, the rest supply the leading-zero run that the register keeps the maximum of. The
// relative standard error is 1.04 / sqrt(2^Precision), so precision 14 costs 16 KiB and estimates within ~0.8 %.
//
// The hashes fed in must be well avalanched in both their high bits (which pick the register) and their low bits (which
// carry the run length). `compute_hash` ends in `fmix64`, so its output can be used as is; raw `std::hash` results
// (identity for integers in libc++ and libstdc++) must be passed through `fmix64` first.
template <uint8_t Precision = 14>
class HyperLogLog {
 public:
  static constexpr auto REGISTER_COUNT = size_t{1} << Precision;
  static_assert(Precision >= 7 && Precision <= 32, "The bias correction below is only valid for 128+ registers.");

  void add(const uint64_t hash) {
    const auto index = static_cast<size_t>(hash >> (64 - Precision));
    // The remaining bits carry the leading-zero run. The sentinel bit below the bits shifted in caps the run at
    // `64 - Precision + 1` and keeps an all-zero hash from reporting an impossible one.
    const auto remainder = (hash << Precision) | (uint64_t{1} << (Precision - 1));
    const auto run_length = static_cast<uint8_t>(std::countl_zero(remainder) + 1);
    _registers[index] = std::max(_registers[index], run_length);
  }

  // Registers combine by element-wise maximum, so per-thread sketches merge into one without synchronization and the
  // merged sketch is exactly the sketch of the union of their inputs.
  void merge(const HyperLogLog& other) {
    for (auto index = size_t{0}; index < REGISTER_COUNT; ++index) {
      _registers[index] = std::max(_registers[index], other._registers[index]);
    }
  }

  size_t estimate() const {
    constexpr auto REGISTERS = static_cast<double>(REGISTER_COUNT);
    constexpr auto ALPHA = 0.7213 / (1.0 + 1.079 / REGISTERS);

    auto inverse_sum = 0.0;
    auto empty_registers = size_t{0};
    for (const auto run_length : _registers) {
      inverse_sum += std::ldexp(1.0, -run_length);  // 2^-run_length
      empty_registers += run_length == 0 ? 1 : 0;
    }

    const auto raw_estimate = ALPHA * REGISTERS * REGISTERS / inverse_sum;

    // Small-cardinality correction: while registers are still empty, linear counting is the more accurate estimator.
    // The classic large-range correction is not needed here - it only exists to undo 32-bit hash collisions.
    if (raw_estimate <= 2.5 * REGISTERS && empty_registers > 0) {
      return static_cast<size_t>(REGISTERS * std::log(REGISTERS / static_cast<double>(empty_registers)));
    }
    return static_cast<size_t>(raw_estimate);
  }

  // The estimate inflated by `sigmas` standard errors, i.e. an upper bound on the true count of what was fed in that
  // holds with high probability. It says nothing about data that was never added to the sketch.
  size_t estimate_upper_bound(const double sigmas = 3.0) const {
    const auto relative_error = 1.04 / std::sqrt(static_cast<double>(REGISTER_COUNT));
    return static_cast<size_t>(static_cast<double>(estimate()) * (1.0 + sigmas * relative_error)) + 1;
  }

 private:
  std::array<uint8_t, REGISTER_COUNT> _registers{};
};

}  // namespace hyrise
