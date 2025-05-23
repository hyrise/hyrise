#include "union_positions.hpp"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/sort/pdqsort/pdqsort.hpp>

#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/abstract_pos_list.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

/**
 * ### UnionPositions implementation
 * The UnionPositions Operator turns each input table into a ReferenceMatrix.
 * The rows in the ReferenceMatrices (see below) need to be sorted in order for them to be merged, that is,
 * performing the UnionPositions operation.
 * Since sorting a multi-column matrix by rows would require a lot of value-copying, for each ReferenceMatrix, a
 * VirtualPosList is created.
 * Each element of this VirtualPosList references a row in a ReferenceMatrix by index. This way, if two values need to
 * be swapped while sorting, only two indices need to swapped instead of a RowID for each column in the
 * ReferenceMatrices.
 * Using a implementation derived from std::set_union, the two virtual pos lists are merged into the result table.
 *
 *
 * ### About ReferenceMatrices
 * The ReferenceMatrix consists of N rows and X columns of RowIDs.
 * N is the same number as the number of rows in the input table.
 * Each of the C column can represent 1..X columns in the input table and is called a ColumnCluster, see below.
 *
 *
 * ### About ColumnCluster
 * For each ColumnID in which one (or both) of the InputTables has a different PosList than in the Column left of it,
 * an entry in _column_cluster_offsets is created.
 *
 * The ReferenceMatrix of a StoredTable will only contain one column, the ReferenceMatrix of the result of a 3 way Join
 * will contain 3 columns.
 *
 * Example:
 *      TableA                                         TableA
 *      a        | b        | c        | d             a        | b        | c        | d
 *      PosList0 | PosList0 | PosList0 | PosList1      PosList2 | PosList2 | PosList3 | PosList4
 *
 *      _column_cluster_offsets = {0, 2, 3}
 *
 *
 * ### TODO(anybody) for potential performance improvements
 * Instead of using a ReferenceMatrix, consider using a linked list of RowIDs for each row. Since most of the sorting
 *      will depend on the leftmost column, this way most of the time no remote memory would need to be accessed
 *
 * The sorting, which is the most expensive part of this operator, could probably be parallelized.
 */
namespace hyrise {

UnionPositions::UnionPositions(const std::shared_ptr<const AbstractOperator>& left,
                               const std::shared_ptr<const AbstractOperator>& right)
    : AbstractReadOnlyOperator(OperatorType::UnionPositions, left, right) {}

std::shared_ptr<AbstractOperator> UnionPositions::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<UnionPositions>(copied_left_input, copied_right_input);
}

void UnionPositions::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void UnionPositions::_on_cleanup() {
  _referenced_tables.clear();
}

const std::string& UnionPositions::name() const {
  static const auto name = std::string{"UnionPositions"};
  return name;
}

std::shared_ptr<const Table> UnionPositions::_on_execute() {
  auto early_result = _prepare_operator();
  if (early_result) {
    return early_result;
  }

  const auto& left_in_table = *left_input_table();

  /**
   * For each input, create a ReferenceMatrix
   */
  auto reference_matrix_left = _build_reference_matrix(left_input_table());
  auto reference_matrix_right = _build_reference_matrix(right_input_table());

  /**
   * Init the virtual pos lists
   */
  VirtualPosList virtual_pos_list_left(left_in_table.row_count(), 0u);
  std::iota(virtual_pos_list_left.begin(), virtual_pos_list_left.end(), 0u);
  VirtualPosList virtual_pos_list_right(right_input_table()->row_count(), 0u);
  std::iota(virtual_pos_list_right.begin(), virtual_pos_list_right.end(), 0u);

  /**
   * Sort the virtual pos lists so that they bring the rows in their respective ReferenceMatrix into order.
   * This is necessary for merging them.
   * Performance note: These sorts take the vast majority of time spent in this operator. Using boost's pdqsort helps
   * a lot over std::sort, but there is probably still room for improvement (see comment above about other data
   * structures). The reason why pdqsort can be much faster than std::sort is that is more efficient for already sorted
   * data, which happens when no "shuffling" operators (e.g., inner joins) occur before the UnionPosition so the
   * position lists are already sorted. For cases where the input is not sorted, pdqsort is usually still more than 20%
   * faster than std::sort.
   */
  boost::sort::pdqsort(virtual_pos_list_left.begin(), virtual_pos_list_left.end(),
                       VirtualPosListCmpContext{reference_matrix_left});
  boost::sort::pdqsort(virtual_pos_list_right.begin(), virtual_pos_list_right.end(),
                       VirtualPosListCmpContext{reference_matrix_right});

  /**
   * Build result table
   */
  auto left_idx = size_t{0};
  auto right_idx = size_t{0};
  const auto num_rows_left = virtual_pos_list_left.size();
  const auto num_rows_right = virtual_pos_list_right.size();

  auto out_table = std::make_shared<Table>(left_in_table.column_definitions(), TableType::References);

  std::vector<std::shared_ptr<RowIDPosList>> pos_lists(reference_matrix_left.size());
  std::generate(pos_lists.begin(), pos_lists.end(), [&] {
    return std::make_shared<RowIDPosList>();
  });

  // Adds the row `row_idx` from `reference_matrix` to the pos_lists we're currently building
  const auto emit_row = [&](const ReferenceMatrix& reference_matrix, size_t row_idx) {
    for (size_t pos_list_idx = 0; pos_list_idx < pos_lists.size(); ++pos_list_idx) {
      pos_lists[pos_list_idx]->emplace_back(reference_matrix[pos_list_idx][row_idx]);
    }
  };

  // Turn 'pos_lists' into a new chunk and append it to the table
  const auto emit_chunk = [&]() {
    Segments output_segments;

    for (size_t pos_lists_idx = 0; pos_lists_idx < pos_lists.size(); ++pos_lists_idx) {
      const auto cluster_column_id_begin = _column_cluster_offsets[pos_lists_idx];
      const auto cluster_column_id_end = pos_lists_idx >= _column_cluster_offsets.size() - 1
                                             ? left_in_table.column_count()
                                             : _column_cluster_offsets[pos_lists_idx + 1];
      for (auto column_id = cluster_column_id_begin; column_id < cluster_column_id_end; ++column_id) {
        auto ref_segment = std::make_shared<ReferenceSegment>(
            _referenced_tables[pos_lists_idx], _referenced_column_ids[column_id], pos_lists[pos_lists_idx]);
        output_segments.push_back(ref_segment);
      }
    }

    out_table->append_chunk(output_segments);
  };

  /**
   * This loop merges reference_matrix_left and reference_matrix_right into the result table. The implementation is
   * derived from std::set_union() and only differs from it insofar as that it builds the output table at the same
   * time as merging the two ReferenceMatrices
   */

  const auto out_chunk_size = Chunk::DEFAULT_SIZE;

  size_t chunk_row_idx = 0;
  for (; left_idx < num_rows_left || right_idx < num_rows_right;) {
    /**
     * Begin derived from std::union()
     */
    if (left_idx == num_rows_left) {  // NOLINT(bugprone-branch-clone)
      emit_row(reference_matrix_right, virtual_pos_list_right[right_idx]);
      ++right_idx;
    } else if (right_idx == num_rows_right) {
      emit_row(reference_matrix_left, virtual_pos_list_left[left_idx]);
      ++left_idx;
    } else if (_compare_reference_matrix_rows(reference_matrix_right, virtual_pos_list_right[right_idx],
                                              reference_matrix_left, virtual_pos_list_left[left_idx])) {
      emit_row(reference_matrix_right, virtual_pos_list_right[right_idx]);
      ++right_idx;
    } else {
      emit_row(reference_matrix_left, virtual_pos_list_left[left_idx]);

      if (!_compare_reference_matrix_rows(reference_matrix_left, virtual_pos_list_left[left_idx],
                                          reference_matrix_right, virtual_pos_list_right[right_idx])) {
        ++right_idx;
      }
      ++left_idx;
    }
    ++chunk_row_idx;
    /**
     * End derived from std::union()
     */

    /**
     * Emit a completed chunk
     */
    if (chunk_row_idx == out_chunk_size && out_chunk_size != 0) {
      emit_chunk();

      chunk_row_idx = 0;
      std::generate(pos_lists.begin(), pos_lists.end(), [&] {
        return std::make_shared<RowIDPosList>();
      });
    }
  }

  if (chunk_row_idx != 0) {
    emit_chunk();
  }

  return out_table;
}

std::shared_ptr<const Table> UnionPositions::_prepare_operator() {
  Assert(left_input_table()->column_definitions() == right_input_table()->column_definitions(),
         "Input tables do not have the same layout.");

  // Later code relies on input tables containing columns. This is guaranteed by the AbstractOperator.

  /**
   * Later code relies on both tables having > 0 rows. If one doesn't, we can just return the other as the result of
   * the operator
   */
  if (left_input_table()->row_count() == 0) {
    return right_input_table();
  }

  if (right_input_table()->row_count() == 0) {
    return left_input_table();
  }

  /**
   * Both tables must contain only ReferenceSegments
   */
  Assert(left_input_table()->type() == TableType::References && right_input_table()->type() == TableType::References,
         "UnionPositions does not support non-reference tables yet.");

  /**
   * Identify the ColumnClusters (verification that this is the same for all chunks happens in the #if HYRISE_DEBUG block
   * below)
   */
  const auto add = [&](const auto& table) {
    auto current_pos_list = std::shared_ptr<const AbstractPosList>();
    const auto first_chunk = table->get_chunk(ChunkID{0});
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      const auto segment = first_chunk->get_segment(column_id);
      const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(segment);
      auto pos_list = ref_segment->pos_list();

      if (current_pos_list != pos_list) {
        current_pos_list = pos_list;
        _column_cluster_offsets.emplace_back(column_id);
      }
    }
  };
  add(left_input_table());
  add(right_input_table());

  boost::sort::pdqsort(_column_cluster_offsets.begin(), _column_cluster_offsets.end());
  const auto unique_end_iter = std::unique(_column_cluster_offsets.begin(), _column_cluster_offsets.end());
  _column_cluster_offsets.resize(std::distance(_column_cluster_offsets.begin(), unique_end_iter));

  /**
   * Identify the tables referenced in each ColumnCluster (verification that this is the same for all chunks happens
   * in the #if HYRISE_DEBUG block below)
   */
  const auto first_chunk_left = left_input_table()->get_chunk(ChunkID{0});
  for (const auto& cluster_begin : _column_cluster_offsets) {
    const auto segment = first_chunk_left->get_segment(cluster_begin);
    const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(segment);
    _referenced_tables.emplace_back(ref_segment->referenced_table());
  }

  /**
   * Identify the column_ids referenced by each column (verification that this is the same for all chunks happens
   * in the #if HYRISE_DEBUG block below)
   */
  for (auto column_id = ColumnID{0}; column_id < left_input_table()->column_count(); ++column_id) {
    const auto segment = first_chunk_left->get_segment(column_id);
    const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(segment);
    _referenced_column_ids.emplace_back(ref_segment->referenced_column_id());
  }

  if constexpr (HYRISE_DEBUG) {
    /**
     * Make sure all chunks have the same ColumnClusters and actually reference the tables and column_ids that the
     * segments in the first chunk of the left input table reference
     */
    const auto verify_column_clusters_in_all_chunks = [&](const auto& table) {
      for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
        auto current_pos_list = std::shared_ptr<const AbstractPosList>();
        size_t next_cluster_id = 0;
        const auto chunk = table->get_chunk(chunk_id);
        for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
          if (next_cluster_id < _column_cluster_offsets.size() &&
              column_id == _column_cluster_offsets[next_cluster_id]) {
            next_cluster_id++;
            current_pos_list = nullptr;
          }

          const auto segment = chunk->get_segment(column_id);
          const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(segment);
          auto pos_list = ref_segment->pos_list();

          if (!current_pos_list) {
            current_pos_list = pos_list;
          }

          Assert(ref_segment->referenced_table() == _referenced_tables[next_cluster_id - 1],
                 "ReferenceSegment (Chunk: " + std::to_string(chunk_id) + ", Column: " + std::to_string(column_id) +
                     ") "
                     "does not reference the same table as the segment at the same index in the first chunk "
                     "of the left input table does.");
          Assert(ref_segment->referenced_column_id() == _referenced_column_ids[column_id],
                 "ReferenceSegment (Chunk: " + std::to_string(chunk_id) + ", Column: " + std::to_string(column_id) +
                     ")"
                     " does not reference the same column as the segment at the same index in the first chunk "
                     "of the left input table does.");
          Assert(current_pos_list == pos_list, "Different PosLists in ColumnCluster.");
        }
      }
    };
    verify_column_clusters_in_all_chunks(left_input_table());
    verify_column_clusters_in_all_chunks(right_input_table());
  }

  return nullptr;
}

UnionPositions::ReferenceMatrix UnionPositions::_build_reference_matrix(
    const std::shared_ptr<const Table>& input_table) const {
  ReferenceMatrix reference_matrix;
  reference_matrix.resize(_column_cluster_offsets.size());
  for (auto& pos_list : reference_matrix) {
    pos_list.reserve(input_table->row_count());
  }

  for (auto chunk_id = ChunkID{0}; chunk_id < input_table->chunk_count(); ++chunk_id) {
    const auto chunk = input_table->get_chunk(ChunkID{chunk_id});

    for (size_t cluster_id = 0; cluster_id < _column_cluster_offsets.size(); ++cluster_id) {
      const auto column_id = _column_cluster_offsets[cluster_id];
      const auto segment = chunk->get_segment(column_id);
      const auto ref_segment = std::static_pointer_cast<const ReferenceSegment>(segment);

      auto& out_pos_list = reference_matrix[cluster_id];
      auto in_pos_list = ref_segment->pos_list();
      std::copy(in_pos_list->begin(), in_pos_list->end(), std::back_inserter(out_pos_list));
    }
  }
  return reference_matrix;
}

bool UnionPositions::_compare_reference_matrix_rows(const ReferenceMatrix& left_matrix, size_t left_row_idx,
                                                    const ReferenceMatrix& right_matrix, size_t right_row_idx) {
  for (size_t column_idx = 0; column_idx < left_matrix.size(); ++column_idx) {
    if (left_matrix[column_idx][left_row_idx] < right_matrix[column_idx][right_row_idx]) {
      return true;
    }

    if (right_matrix[column_idx][right_row_idx] < left_matrix[column_idx][left_row_idx]) {
      return false;
    }
  }
  return false;
}

bool UnionPositions::VirtualPosListCmpContext::operator()(size_t left, size_t right) const {
  for (const auto& reference_matrix_column : reference_matrix) {
    const auto left_row_id = reference_matrix_column[left];
    const auto right_row_id = reference_matrix_column[right];

    if (left_row_id < right_row_id) {
      return true;
    }

    if (right_row_id < left_row_id) {
      return false;
    }
  }
  return false;
}

}  // namespace hyrise
