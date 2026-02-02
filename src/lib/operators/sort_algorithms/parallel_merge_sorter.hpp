#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "scheduler/abstract_task.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"

#include <cstddef>
#include <functional>
#include <utility>  // for std::move  // NOLINT(misc-include-cleaner)

#include "boost/sort/pdqsort/pdqsort.hpp"
#include "hyrise.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "types.hpp"

namespace hyrise {

struct AggEntry {
  RowID row_id;               // used for key comparison
  std::vector<double> sums;   // aggregate values (can support multiple non-key columns)
};

template <typename Compare>
class AbstractRowIDSorter {
 public:
  virtual ~AbstractRowIDSorter() = default;
  virtual void sort(RowIDPosList& rows, Compare comp) = 0;
};

template <typename Compare>
class AbstractAggSorter {
 public:
  virtual ~AbstractAggSorter() = default;
  virtual void sort_with_agg(std::vector<AggEntry>& entries, Compare comp_agg) = 0;
};

template <typename Compare>
class ParallelMergeSorter : public AbstractRowIDSorter<Compare> {
 public:
  void sort(RowIDPosList& rows, Compare comp) override;

 private:
  // Heuristic: arbitrary divider between small and large workloads.
  static constexpr size_t SMALL_ARRAY_THRESHOLD = 10'000;
  // Heuristic: arbitrary divider between parallel and sequential merging.
  static constexpr size_t SMALL_MERGE_THRESHOLD = 1'000;

  // Merge Path cut descriptor used to partition work between left/right sequences.
  struct Cut {
    size_t a;
    size_t b;
  };

  Cut find_cut_point(const RowID* left, size_t len_left, const RowID* right, size_t len_right, size_t diag,
                     Compare comp);
  void merge_path_parallel(RowIDPosList& rows, size_t start, size_t mid, size_t end, Compare comp, size_t max_workers);
};

template <typename Compare>
class ParallelAggregateSorter : public AbstractAggSorter<Compare> {
 public:
  void sort_with_agg(std::vector<AggEntry>& entries, Compare comp_agg) override;

 private:
  // Heuristic: arbitrary divider between small and large workloads.
  static constexpr size_t SMALL_ARRAY_THRESHOLD = 10'000;
  // Heuristic: arbitrary divider between parallel and sequential merging.
  static constexpr size_t SMALL_MERGE_THRESHOLD = 1'000;

  // Merge Path cut descriptor used to partition work between left/right sequences.
  struct AggCut {
    size_t a;
    size_t b;
  };

  AggCut find_cut_point_agg(const AggEntry* left, size_t len_left,
                          const AggEntry* right, size_t len_right,
                          size_t diag, Compare agg_comp);
  void merge_path_parallel_with_agg(std::vector<AggEntry>& entries, size_t start, size_t mid, size_t end, Compare comp_agg, size_t max_workers);
};



template <typename Compare>
inline void ParallelAggregateSorter<Compare>::sort_with_agg(std::vector<AggEntry>& entries, Compare comp_agg) {
  const auto row_count = entries.size();
  const auto is_multithreaded = Hyrise::get().is_multi_threaded();


  if (!HYRISE_DEBUG && (!is_multithreaded || row_count < SMALL_ARRAY_THRESHOLD)) {
    boost::sort::pdqsort(entries.begin(), entries.end(), [&](const AggEntry& a, const AggEntry& b) {
        return comp_agg(a, b);
    });
    return;
}

  // 1) Get number of workers and block size.
  // Default to single-threaded execution.
  auto num_workers = size_t{1};

  const auto nq_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
  if (nq_scheduler) {
    num_workers = static_cast<size_t>(nq_scheduler->active_worker_count().load());
  }

  const auto block = (row_count + num_workers - 1) / num_workers;

  // 2) Sort each block in parallel.
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(num_workers);
  std::vector<size_t> block_sizes(num_workers, 0);
  for (auto thread_idx = size_t{0}; thread_idx < num_workers; ++thread_idx) {
    const auto start = thread_idx * block;
    const auto end = std::min(start + block, row_count);
    if (start < end) {
        jobs.emplace_back(std::make_shared<JobTask>(
  [start, end, &entries, &comp_agg, &block_sizes, thread_idx]() {

    // 1) Sort block
    boost::sort::pdqsort(entries.begin() + start,
                          entries.begin() + end,
                          [&](const AggEntry& a, const AggEntry& b) {
                            return comp_agg(a, b);
                          });

    // 2) Aggregate + compact
    auto write = entries.begin() + start;
    auto read  = entries.begin() + start;

    while (read != entries.begin() + end) {
      auto current = *read;
      auto next = read + 1;

      while (next != entries.begin() + end &&
             !comp_agg(current, *next) &&
             !comp_agg(*next, current)) {
        for (size_t i = 0; i < current.sums.size(); ++i) {
          current.sums[i] += next->sums[i];
        }
        ++next;
      }

      *write++ = std::move(current);
      read = next;
    }

    block_sizes[thread_idx] = static_cast<size_t>(write - (entries.begin() + start));
}));
    }
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  size_t write_pos = 0;


for (size_t i = 0; i < num_workers; ++i) {
  const auto start = i * block;
  const auto count = block_sizes[i];

  std::move(entries.begin() + start,
            entries.begin() + start + count,
            entries.begin() + write_pos);

  write_pos += count;
}

entries.resize(write_pos);
const auto compacted_count = entries.size();

  // 3) Bottom-up merge sorted runs, doubling the run size each pass:
  auto run = block;
  while (run < compacted_count) {
    for (auto left = size_t{0}; left + run < compacted_count; left += 2 * run) {
      auto mid = left + run;
      auto right = std::min(left + (2 * run), compacted_count);

      merge_path_parallel_with_agg(entries, left, mid, right, comp_agg, num_workers);
    }
    run *= 2;
  }
}


template <typename Compare>
void ParallelMergeSorter<Compare>::sort(RowIDPosList& rows, Compare comp) {
  const auto row_count = rows.size();
  const auto is_multithreaded = Hyrise::get().is_multi_threaded();

  if (!HYRISE_DEBUG && (!is_multithreaded || row_count < SMALL_ARRAY_THRESHOLD)) {
    boost::sort::pdqsort(rows.begin(), rows.end(), comp);
    return;
  }

  // 1) Get number of workers and block size.
  // Default to single-threaded execution.
  auto num_workers = size_t{1};

  const auto nq_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
  if (nq_scheduler) {
    num_workers = static_cast<size_t>(nq_scheduler->active_worker_count().load());
  }

  const auto block = (row_count + num_workers - 1) / num_workers;

  // 2) Sort each block in parallel.
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(num_workers);
  for (auto thread_idx = size_t{0}; thread_idx < num_workers; ++thread_idx) {
    const auto start = thread_idx * block;
    const auto end = std::min(start + block, row_count);
    if (start < end) {
      jobs.emplace_back(std::make_shared<JobTask>([start, end, &rows, &comp]() {
        const auto start_offset = static_cast<std::ptrdiff_t>(start);
        const auto end_offset = static_cast<std::ptrdiff_t>(end);
        boost::sort::pdqsort(rows.begin() + start_offset, rows.begin() + end_offset, comp);
      }));
    }
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // 3) Bottom-up merge sorted runs, doubling the run size each pass:
  auto run = block;
  while (run < row_count) {
    for (auto left = size_t{0}; left + run < row_count; left += 2 * run) {
      auto mid = left + run;
      auto right = std::min(left + (2 * run), row_count);

      // For small runs, parallel merging is not worth the overhead.
      if (!HYRISE_DEBUG && run < SMALL_MERGE_THRESHOLD) {
        const auto start_offset = static_cast<std::ptrdiff_t>(left);
        const auto mid_offset = static_cast<std::ptrdiff_t>(mid);
        const auto end_offset = static_cast<std::ptrdiff_t>(right);
        std::inplace_merge(rows.begin() + start_offset, rows.begin() + mid_offset, rows.begin() + end_offset, comp);
      } else {
        merge_path_parallel(rows, left, mid, right, comp, num_workers);
      }
    }
    run *= 2;
  }
}

}  // namespace hyrise