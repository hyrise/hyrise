#pragma once

#include <cstdint>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "storage/chunk.hpp"
#include "storage/deprecated_dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "types.hpp"

namespace opossum {

class PQPExpression;

/**
 * Operator to select a subset of the set of all columns found in the table
 *
 * Note: Projection does not support null values at the moment
 */
class Projection : public AbstractReadOnlyOperator {
 public:
  using ColumnExpressions = std::vector<std::shared_ptr<PQPExpression>>;

  Projection(const std::shared_ptr<const AbstractOperator> in, const ColumnExpressions& column_expressions);

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  const ColumnExpressions& column_expressions() const;

  std::shared_ptr<AbstractOperator> recreate(const std::vector<AllParameterVariant>& args) const override;

  /**
   * The dummy table is used for literal projections that have no input table.
   * This was introduce to allow queries like INSERT INTO tbl VALUES (1, 2, 3);
   * Because each INSERT uses a projection as input, the above case needs to project the three
   * literals (1, 2, 3) without any specific input table. Therefore, this dummy table is used instead.
   *
   * The dummy table contains one (value) column with one row. This way, the above projection
   * contains exactly one row with the given literals.
   */
  class DummyTable : public Table {
   public:
    DummyTable() : Table(Chunk::MAX_SIZE) {
      add_column("dummy", DataType::Int);
      append(std::vector<AllTypeVariant>{0});
    }
  };

  static std::shared_ptr<Table> dummy_table();

 protected:
  ColumnExpressions _column_expressions;

  template <typename T>
  static void _create_column(boost::hana::basic_type<T> type, const std::shared_ptr<Chunk>& chunk,
                             const ChunkID chunk_id, const std::shared_ptr<PQPExpression>& expression,
                             std::shared_ptr<const Table> input_table_left, bool reuse_column_from_input);

  static DataType _get_type_of_expression(const std::shared_ptr<PQPExpression>& expression,
                                          const std::shared_ptr<const Table>& table);

  /**
   * This function evaluates the given expression on a single chunk.
   * It returns a vector containing the materialized values resulting from the expression.
   */
  template <typename T>
  static const pmr_concurrent_vector<std::pair<bool, T>> _evaluate_expression(
      const std::shared_ptr<PQPExpression>& expression, const std::shared_ptr<const Table> table,
      const ChunkID chunk_id);

  std::shared_ptr<const Table> _on_execute() override;
};

}  // namespace opossum
