#pragma once

#include "operators/abstract_read_only_operator.hpp"

namespace opossum {

/**
 * ## Purpose
 *  The backend for realising OR statements such as `SELECT a FROM t WHERE a < 10 OR a > 20;`
 *
 * ## Input / Output
 *  Takes two reference tables and computes the Set Union of their pos lists. The output contains all rows of both input
 *  tables exactly once
 *
 * ## Notes
 *  - If an input table contains duplicate row_ids, they will be eliminated as well
 *
 * ## Limitations
 *  Currently this only works if both input tables
 *      - reference only one table
 *      - both reference the same table
 *      - all columns in a chunk use the same pos_list
 *      - have the same column layout
 *  This means simple OR statements like the one above can be performed, but the Operator will fail gracefully
 *  when working on input tables that are the result of a JOIN.
 *
 * ## Example
 *  Table T0
 *  == Columns ==
 *  a   | b
 *  int | int
 *  == Chunk 0 ==
 *  1   | 2
 *  3   | 4
 *  == Chunk 1 ==
 *  1   | 2
 *  4   | 6
 *
 *  Table T1
 *  == Columns ==
 *  c
 *  ref T0.a
 *  == Chunk 0 ==
 *  RowID{0, 0}
 *  RowID{0, 0}
 *  RowID{1, 0}
 *  RowID{0, 1}
 *
 *  Table T2
 *  == Columns ==
 *  c
 *  ref T0.a
 *  == Chunk 0 ==
 *  RowID{1, 0}
 *  RowID{1, 0}
 *  RowID{0, 1}
 *  == Chunk 1 ==
 *  RowID{1, 1}
 *  RowID{0, 1}
 *
 *
 *  Table UnionUnique(T1, T2)
 *  == Columns ==
 *  c
 *  ref T0.a
 *  == Chunk 0 ==
 *  RowID{0, 0}
 *  RowID{0, 1}
 *  RowID{1, 0}
 *
 */
class UnionUnique : public AbstractReadOnlyOperator {
 public:
  UnionUnique(const std::shared_ptr<const AbstractOperator>& left,
              const std::shared_ptr<const AbstractOperator>& right);

  uint8_t num_in_tables() const override;
  uint8_t num_out_tables() const override;
  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;
  const std::string name() const override;
  const std::string description() const override;

 private:
  std::shared_ptr<const Table> _on_execute() override;

  // Returning the referenced table from a "verify" function may seem odd, but eliminates the need to compute it twice
  std::shared_ptr<const Table> _verify_input_and_get_referenced_table() const;
};
}