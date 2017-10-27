#pragma once

#include <memory>
#include <string>
#include <vector>

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

/**
 * ## Purpose
 *  The backend for realising OR statements such as `SELECT a FROM t WHERE a < 10 OR a > 20;`
 *
 * ## Input / Output
 *  Takes two reference tables and computes the Set Union of their pos lists. The output contains all rows of both input
 *  tables exactly once.
 *  The input tables `left` and `right` must
 *      - have the same number of columns with the same names and types
 *      - each column must reference the same table and same column_id for all chunks.
 *          - this means: if the first column of `left` references table "a" and ColumnID 3, the first column of `right`
 *                        has to reference "a".3 as well.
 *
 *
 * ## Example
 *  Table T0
 *    == Columns ==
 *    a   | b
 *    int | int
 *    == Chunk 0 ==
 *    1   | 2
 *    3   | 4
 *    == Chunk 1 ==
 *    1   | 2
 *    4   | 6
 *
 *  Table T1
 *    == Columns ==
 *    c
 *    ref T0.a
 *    == Chunk 0 ==
 *    RowID{0, 0}
 *    RowID{0, 0}
 *    RowID{1, 0}
 *    RowID{0, 1}
 *
 *  Table T2
 *    == Columns ==
 *    c
 *    ref T0.a
 *    == Chunk 0 ==
 *    RowID{1, 0}
 *    RowID{1, 0}
 *    RowID{0, 1}
 *    == Chunk 1 ==
 *    RowID{1, 1}
 *    RowID{0, 1}
 *
 *  Table UnionUnique(T1, T2)
 *    == Columns ==
 *    c
 *    ref T0.a
 *    == Chunk 0 ==
 *    RowID{0, 0}
 *    RowID{0, 1}
 *    RowID{1, 0}
 *
 */
class UnionPositions : public AbstractReadOnlyOperator {
 public:
  UnionPositions(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right);

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;
  const std::string name() const override;

 private:
  // See docs at the top of the cpp
  using ReferenceMatrix = std::vector<opossum::PosList>;
  using VirtualPosList = std::vector<size_t>;

  /**
   * Comparator for performing the std::sort() of a virtual pos list.
   * Needs to know about the ReferenceMatrix that the VirtualPosList references and is thus dubbed a "Context".
   */
  struct VirtualPosListCmpContext {
    ReferenceMatrix& reference_matrix;
    bool operator()(size_t lhs, size_t rhs) const;
  };

  std::shared_ptr<const Table> _on_execute() override;

  /**
   * Makes sure the input data is valid for this operator (see implementation for details) and initializes
   * _column_segment_begins, _referenced_tables and _referenced_column_ids
   *
   * @returns the result table of the operator if one or both of the inputs was empty and we don't actually need to
   *    execute the operatopr. nullptr otherwise.
   */
  std::shared_ptr<const Table> _prepare_operator();

  UnionPositions::ReferenceMatrix _build_reference_matrix(const std::shared_ptr<const Table>& input_table) const;
  bool _cmp_reference_matrix_rows(const ReferenceMatrix& matrix_a, size_t row_idx_a,
                                  const ReferenceMatrix& matrix_b, size_t row_idx_b) const;

  // In the input tables, the column indices in which a different pos_list is used than in the column left of it
  std::vector<ColumnID> _column_segment_offsets;

  // For each column segment, the table its pos_list references
  std::vector<std::shared_ptr<const Table>> _referenced_tables;

  // For each column_idx in the input tables, specifies the referenced column in the referenced table
  std::vector<ColumnID> _referenced_column_ids;
};
}  // namespace opossum
