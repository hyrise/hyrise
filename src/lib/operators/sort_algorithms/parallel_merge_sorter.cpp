#include "parallel_merge_sorter.hpp"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "boost/sort/pdqsort/pdqsort.hpp"
#include "hyrise.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"

namespace hyrise {

// Finds the cut point for the merge path diagonal inspired by https://arxiv.org/pdf/1406.2628.
// We use Merge Path to parallelize the binary merge across threads.
// The diagonal partitioning finds balanced cut points via binary
// searches so each worker merges disjoint, contiguous ranges.
// This minimizes synchronization, preserves sequential access patterns, and improves cache locality.
// It adapts to data skew, yielding near-equal work per worker and good load balancing.
// Compared to naive chunking, Merge Path scales better with core count for large runs.
template <typename Compare>
typename ParallelMergeSorter<Compare>::Cut ParallelMergeSorter<Compare>::find_cut_point(
    const RowID* left, size_t len_left, const RowID* right, size_t len_right, size_t diag, Compare comp) {
  auto low = diag > len_right ? diag - len_right : 0;
  auto high = std::min(diag, len_left);

  while (low < high) {
    const auto cut_left = (low + high) / 2;
    const auto cut_right = diag - cut_left;

    const bool left_smaller = (cut_left < len_left) && (cut_right == 0 || comp(left[cut_left], right[cut_right - 1]));

    if (left_smaller) {
      low = cut_left + 1;
    } else {
      high = cut_left;
    }
  }
  return Cut{low, diag - low};
}

// Parallel merge of two consecutive runs using Merge Path.
template <typename Compare>
void ParallelMergeSorter<Compare>::merge_path_parallel(RowIDPosList& rows, const size_t start, const size_t mid,
                                                       const size_t end, Compare comp, const size_t max_workers) {
  const auto len_left = mid - start;
  const auto len_right = end - mid;
  const auto total_len = len_left + len_right;

  const auto workers = static_cast<size_t>(std::min(max_workers, total_len));

  auto dest = RowIDPosList(total_len);

  // Compute cut points.
  auto cuts = std::vector<Cut>(workers + 1);
  cuts.front() = {0, 0};
  cuts.back() = {len_left, len_right};

  const auto* left = rows.data() + start;
  const auto* right = rows.data() + mid;

  for (auto index = size_t{1}; index < workers; ++index) {
    const auto diag = index * total_len / workers;
    const auto [a, b] = find_cut_point(left, len_left, right, len_right, diag, comp);
    cuts[index] = {a, b};
  }

  // Launch worker tasks.
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(workers);

  for (auto task_idx = size_t{0}; task_idx < workers; ++task_idx) {
    jobs.emplace_back(std::make_shared<JobTask>([&, task_idx] {
      const auto cut_l = cuts[task_idx];
      const auto cut_r = cuts[task_idx + 1];

      const auto* l_begin = left + cut_l.a;
      const auto* l_end = left + cut_r.a;
      const auto* r_begin = right + cut_l.b;
      const auto* r_end = right + cut_r.b;

      auto* out = dest.data() + (cut_l.a + cut_l.b);
      std::merge(l_begin, l_end, r_begin, r_end, out, comp);
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  const auto start_offset = static_cast<std::ptrdiff_t>(start);
  std::move(dest.begin(), dest.end(), rows.begin() + start_offset);
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

template class ParallelMergeSorter<std::function<bool(const RowID&, const RowID&)>>;
}  // namespace hyrise
