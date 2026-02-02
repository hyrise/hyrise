#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "scheduler/abstract_task.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"

namespace hyrise {

template <typename Compare>
class AbstractRowIDSorter {
 public:
  virtual ~AbstractRowIDSorter() = default;
  virtual void sort(RowIDPosList& rows, Compare comp) = 0;
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

}  // namespace hyrise
