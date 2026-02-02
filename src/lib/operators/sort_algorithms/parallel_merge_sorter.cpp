#include "parallel_merge_sorter.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <utility>  // for std::move  // NOLINT(misc-include-cleaner)
#include <vector>

#include "boost/sort/pdqsort/pdqsort.hpp"
#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "types.hpp"

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

template <typename Compare>
typename ParallelAggregateSorter<Compare>::AggCut
ParallelAggregateSorter<Compare>::find_cut_point_agg(
    const AggEntry* left, size_t len_left,
    const AggEntry* right, size_t len_right,
    size_t diag, Compare comp) {

  size_t low  = diag > len_right ? diag - len_right : 0;
  size_t high = std::min(diag, len_left);

  while (low < high) {
    size_t cut_left  = (low + high) / 2;
    size_t cut_right = diag - cut_left;

    bool left_smaller = (cut_left < len_left) &&
                        (cut_right == 0 || comp(left[cut_left], right[cut_right - 1]));

    if (left_smaller) {
      low = cut_left + 1;
    } else {
      high = cut_left;
    }
  }

  return AggCut{low, diag - low};
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
void ParallelAggregateSorter<Compare>::merge_path_parallel_with_agg(std::vector<AggEntry>& entries, const size_t start, const size_t mid,
                                                       const size_t end, Compare comp_agg, const size_t max_workers) {
    const auto len_left = mid - start;
  const auto len_right = end - mid;
  const auto total_len = len_left + len_right;

  assert(start <= mid && mid <= end);

  if (total_len == 0) {
    return;
  }

  const auto workers = static_cast<size_t>(std::min(max_workers, total_len));

  //Hier

  // Destination buffer
  std::vector<AggEntry> dest(total_len);

  // Compute cut points for merge path
  std::vector<AggCut> cuts(workers + 1);
  cuts.front() = {0, 0};
  cuts.back() = {len_left, len_right};

  const auto* left = entries.data() + start;
  const auto* right = entries.data() + mid;

  // Compute cuts using your find_cut_point_agg function
  for (size_t i = 1; i < workers; ++i) {
    const size_t diag = i * total_len / workers;
    
    // Validate diag is within bounds
    assert(diag <= total_len);
    
    auto cut = find_cut_point_agg(left, len_left, right, len_right, diag, comp_agg);
    
    // Clamp to valid ranges (should already be valid from find_cut_point_agg)
    cut.a = std::min(cut.a, len_left);
    cut.b = std::min(cut.b, len_right);
    
    cuts[i] = cut;
    
    // Validate monotonicity
    if (i > 0) {
      assert(cut.a >= cuts[i-1].a);
      assert(cut.b >= cuts[i-1].b);
      assert((cut.a + cut.b) >= (cuts[i-1].a + cuts[i-1].b));
    }
  }

  // Final validation of all cuts
  for (size_t i = 0; i <= workers; ++i) {
    assert(cuts[i].a <= len_left);
    assert(cuts[i].b <= len_right);
    if (i > 0) {
      assert(cuts[i].a >= cuts[i-1].a);
      assert(cuts[i].b >= cuts[i-1].b);
      assert((cuts[i].a + cuts[i].b) >= (cuts[i-1].a + cuts[i-1].b));
    }
  }

  // Launch parallel merge tasks
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(workers);

  for (size_t task_idx = 0; task_idx < workers; ++task_idx) {
    jobs.emplace_back(std::make_shared<JobTask>([=, &dest, &cuts] {
      const auto& cut_l = cuts[task_idx];
      const auto& cut_r = cuts[task_idx + 1];

      const size_t task_len_left = cut_r.a - cut_l.a;
      const size_t task_len_right = cut_r.b - cut_l.b;
      
      // Early return if no work
      if (task_len_left == 0 && task_len_right == 0) return;

      // Validate bounds
      assert(cut_l.a <= len_left);
      assert(cut_l.b <= len_right);
      assert(cut_r.a <= len_left);
      assert(cut_r.b <= len_right);
      assert(cut_l.a <= cut_r.a);
      assert(cut_l.b <= cut_r.b);
      
      const auto* l_begin = left + cut_l.a;
      const auto* l_end = left + cut_r.a;
      const auto* r_begin = right + cut_l.b;
      const auto* r_end = right + cut_r.b;

      auto* out = dest.data() + (cut_l.a + cut_l.b);

      // Validate output position
      assert(out >= dest.data());
      assert(out + task_len_left + task_len_right <= dest.data() + total_len);

      std::merge(l_begin, l_end, r_begin, r_end, out, comp_agg);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // Validate before copy
  assert(entries.size() >= start + total_len);
  
  // Copy back to original vector
  std::move(dest.begin(), dest.end(), entries.begin() + start);
}

template class ParallelMergeSorter<std::function<bool(const RowID&, const RowID&)>>;

template class ParallelAggregateSorter<std::function<bool(const AggEntry&, const AggEntry&)>>;
}  // namespace hyrise