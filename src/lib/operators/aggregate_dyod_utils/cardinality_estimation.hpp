#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

#include "operators/aggregate_dyod_utils/chunk_job_runner.hpp"
#include "operators/aggregate_dyod_utils/ticketing.hpp"
#include "operators/operator_state.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Cardinality estimation for sizing the fixed-capacity `ConcurrentTicketMap`.
// We use a precision of 10 bits, so the number of registers for HyperLogLog `m` is 1024.
// This gives a relative standard error of 1.04 / sqrt(m) = 0.0325, or 3.25 %.
// We use a precision of 10 giving 1 KiB of registers per sketch.
// The entire implementation here is stolen from "HyperLogLog: the analysis of a near-optimal cardinality estimation
// algorithm" by Philippe Flajolet, Éric Fusy, Olivier Gandouet, and Frédéric Meunier (2007).
template <uint8_t precision = 10>
class HyperLogLog : public Noncopyable {
 public:
  static constexpr auto PRECISION_BITS = uint64_t{precision};
  static constexpr auto HASH_BIT_COUNT = uint64_t{64};
  static constexpr auto REGISTER_COUNT = size_t{1} << PRECISION_BITS;

  void add(const uint64_t hash) {
    const auto register_index = static_cast<size_t>(hash >> (HASH_BIT_COUNT - PRECISION_BITS));
    const auto remaining_bits = (hash << PRECISION_BITS) | (uint64_t{1} << (PRECISION_BITS - 1));
    const auto rho_w = static_cast<uint8_t>(std::countl_zero(remaining_bits) + 1);
    _registers[register_index] = std::max(_registers[register_index], rho_w);
  }

  void merge(const HyperLogLog& other) {
    for (auto index = size_t{0}; index < REGISTER_COUNT; ++index) {
      _registers[index] = std::max(_registers[index], other._registers[index]);
    }
  }

  size_t estimate() const {
    constexpr auto REGISTERS = static_cast<double>(REGISTER_COUNT);
    constexpr auto ALPHA = 0.7213 / (1.0 + (1.079 / REGISTERS));

    auto inverse_sum = 0.0;
    auto empty_registers = size_t{0};
    for (const auto rho_w : _registers) {
      inverse_sum += std::ldexp(1.0, -rho_w);  // 2^-rho_w
      empty_registers += rho_w == 0 ? 1 : 0;
    }

    const auto raw_estimate = ALPHA * REGISTERS * REGISTERS / inverse_sum;

    // Small range correction.
    if (raw_estimate <= 2.5 * REGISTERS && empty_registers > 0) {
      return static_cast<size_t>(REGISTERS * std::log(REGISTERS / static_cast<double>(empty_registers)));
    }

    return static_cast<size_t>(raw_estimate);
  }

  // We need an upper bound. Therefore we use the standard error with default 3 sigmas (99.7% confidence interval).
  size_t estimate_upper_bound(const double sigmas = 3.0) const {
    return static_cast<size_t>(static_cast<double>(estimate()) * (1.0 + (sigmas * _standard_error))) + 1;
  }

 private:
  const double _standard_error = 1.04 / std::sqrt(static_cast<double>(REGISTER_COUNT));
  std::array<uint8_t, REGISTER_COUNT> _registers{};
};

// Per-worker state of the multi-column estimation.
struct MultiColumnEstimationState : public Noncopyable {
  void merge(const MultiColumnEstimationState& other) {
    sketch.merge(other.sketch);
  }

  HyperLogLog<> sketch;
  MaterializedRows materialized;
};

inline size_t estimate_group_count_multi_column(const RowFormat& format,
                                                const std::vector<ColumnID>& groupby_column_ids,
                                                const std::shared_ptr<const Table>& input_table,
                                                const size_t max_chunk_size) {
  const auto row_count = input_table->row_count();
  if (row_count == 0) {
    return 1;
  }

  auto operator_state = OperatorSharedState<MultiColumnEstimationState>{};
  const auto chunk_count = input_table->chunk_count();

  const auto process_chunk = [&](const ChunkID chunk_id) {
    auto& worker_state = operator_state.current_worker_state();
    auto& materialized = worker_state.materialized;

    // The first time this worker is called it needs to allocate the 'materialized' buffer.
    if (!materialized.rows) {
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays)
      materialized.rows = std::make_unique<uint8_t[]>(max_chunk_size * format.row_size);
    }

    const auto& chunk = input_table->get_chunk(chunk_id);
    DebugAssert(chunk, "Expected an existing input chunk.");
    materialize_rows(format, chunk, groupby_column_ids, materialized);

    auto* row_ptr = materialized.rows.get();
    for (auto chunk_offset = uint64_t{0}; chunk_offset < materialized.row_count; ++chunk_offset) {
      const auto row_view = RowView{.base = row_ptr, .format = format};
      // NOTE: We only compute the hash of the key bytes here. For strings, this can amount to only hashing the
      // inline prefix!
      worker_state.sketch.add(fmix64(compute_hash(row_view.key_bytes(), format.key_length)));
      row_ptr += format.row_size;
    }
  };

  run_jobs_over_chunks(chunk_count, [&](const size_t, auto&& next_chunk) {
    while (const auto chunk_id = next_chunk()) {
      process_chunk(*chunk_id);
    }
  });
  return operator_state.merge_worker_states().sketch.estimate_upper_bound();
}

template <typename ColumnDataType>
size_t estimate_group_count_single_column(const ColumnID groupby_column_id,
                                          const std::shared_ptr<const Table>& input_table) {
  const auto row_count = input_table->row_count();
  if (row_count == 0) {
    return 1;
  }

  auto operator_state = OperatorSharedState<HyperLogLog<>>{};
  const auto chunk_count = input_table->chunk_count();

  const auto process_chunk = [&](const ChunkID chunk_id) {
    auto& worker_state = operator_state.current_worker_state();
    const auto& chunk = input_table->get_chunk(chunk_id);
    DebugAssert(chunk, "Expected an existing input chunk.");
    segment_iterate<ColumnDataType>(*chunk->get_segment(groupby_column_id), [&](const auto& position) {
      if (!position.is_null()) {
        // NOTE: Preserve floating-point representations instead of truncating them to uint64_t.
        if constexpr (std::is_floating_point_v<ColumnDataType>) {
          using Bits = std::conditional_t<sizeof(ColumnDataType) == sizeof(float), uint32_t, uint64_t>;
          worker_state.add(fmix64(static_cast<uint64_t>(std::bit_cast<Bits>(position.value()))));
        } else {
          worker_state.add(fmix64(static_cast<uint64_t>(position.value())));
        }
      }
    });
  };

  run_jobs_over_chunks(chunk_count, [&](const size_t, auto&& next_chunk) {
    while (const auto chunk_id = next_chunk()) {
      process_chunk(*chunk_id);
    }
  });
  return operator_state.merge_worker_states().estimate_upper_bound();
}

}  // namespace hyrise
