#include "cardinality_estimation.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace hyrise {

std::vector<ChunkID> sample_chunk_ids(const std::shared_ptr<const Table>& input_table) {
  const auto chunk_count = size_t{input_table->chunk_count()};
  if (chunk_count == 0) {
    return {};
  }

  const auto sample_count =
      std::clamp(chunk_count / SAMPLE_CHUNK_DIVISOR, size_t{1}, std::min(MAX_SAMPLE_CHUNKS, chunk_count));
  const auto stride = chunk_count / sample_count;

  // Spread the sample over the whole table rather than taking a prefix: a table clustered or sorted on a group-by
  // column shows only a fraction of its groups in its first chunks.
  auto chunk_ids = std::vector<ChunkID>{};
  chunk_ids.reserve(sample_count);
  for (auto sample_index = size_t{0}; sample_index < sample_count; ++sample_index) {
    chunk_ids.emplace_back(static_cast<ChunkID::base_type>(sample_index * stride));
  }

  return chunk_ids;
}

size_t extrapolate_group_count(const size_t sampled_groups, const size_t sampled_rows, const size_t row_count) {
  if (sampled_rows == 0 || sampled_rows >= row_count) {
    return std::min(sampled_groups, row_count);
  }

  // Scale the sample's groups-per-row up to the whole table. Computed in floating point because the intermediate
  // product overflows `size_t` for large tables.
  const auto extrapolated =
      static_cast<double>(sampled_groups) * static_cast<double>(row_count) / static_cast<double>(sampled_rows);
  return static_cast<size_t>(std::min(extrapolated, static_cast<double>(row_count)));
}

size_t estimate_group_count_multi_column(const RowFormat& format, const std::vector<ColumnID>& groupby_column_ids,
                                         const std::shared_ptr<const Table>& input_table, const size_t max_chunk_size) {
  const auto row_count = input_table->row_count();
  if (row_count == 0 || input_table->chunk_count() == 0) {
    return 1;
  }

  const auto sampled_chunk_ids = sample_chunk_ids(input_table);
  const auto sample_count = sampled_chunk_ids.size();
  const auto thread_count = std::max<size_t>(Hyrise::get().topology.num_cpus() - 1, 1);
  const auto job_count = std::min(thread_count, sample_count);

  // One sketch per job, merged below. Sketches are fixed-size (16 KiB) and merge by element-wise maximum, so the jobs
  // share nothing.
  auto sketches = std::vector<CardinalitySketch>(job_count);
  auto sampled_rows_per_job = std::vector<size_t>(job_count, 0);
  auto next_sample_index = std::atomic<size_t>{0};

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(job_count);
  for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, job_id] {
      auto materialized = MaterializedRows{};
      materialized.rows = std::make_unique<uint8_t[]>(max_chunk_size * format.row_size);
      auto& sketch = sketches[job_id];

      // `compute_hash` over the key bytes covers the null bitmap, the fixed-width columns and the inline string
      // prefixes, but not the tail of a long string. Group-bys on long strings that share a prefix (`Customer#0000...`)
      // would otherwise all collapse into one sketch entry, so the tails are mixed in here. This costs nothing in the
      // grouping pass itself, which distinguishes such keys by full comparison rather than by hash.
      const auto row_key_hash = [&](const RowView& row) {
        auto hash = compute_hash(row.key_bytes(), format.key_length);
        const auto string_col_count = row.string_col_count();
        for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
          const auto* const string_ptr = row.string_ptr(string_col_index);
          if (string_ptr != nullptr) {
            hash = compute_hash(string_ptr, std::strlen(string_ptr), hash);
          }
        }
        return hash;
      };

      while (true) {
        const auto sample_index = next_sample_index.fetch_add(1);
        if (sample_index >= sample_count) {
          break;
        }

        const auto& chunk = input_table->get_chunk(sampled_chunk_ids[sample_index]);
        _materialize_rows(format, chunk, groupby_column_ids, materialized);
        sampled_rows_per_job[job_id] += materialized.row_count;

        // `compute_hash` ends in `fmix64`, so the row hashes the grouping pass computes anyway are already avalanched
        // well enough to feed the sketch directly.
        auto* row_ptr = materialized.rows.get();
        for (auto chunk_offset = size_t{0}; chunk_offset < materialized.row_count; ++chunk_offset) {
          sketch.add(row_key_hash(RowView{row_ptr, format}));
          row_ptr += format.row_size;
        }
      }
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  auto sampled_rows = sampled_rows_per_job[0];
  for (auto job_id = size_t{1}; job_id < job_count; ++job_id) {
    sketches[0].merge(sketches[job_id]);
    sampled_rows += sampled_rows_per_job[job_id];
  }

  return std::clamp(extrapolate_group_count(sketches[0].estimate_upper_bound(), sampled_rows, row_count), size_t{1},
                    size_t{row_count});
}

}  // namespace hyrise
