#pragma once

#include <memory>

#include "abstract_read_only_operator.hpp"

#include "all_type_variant.hpp"
#include "storage/index/column_index_type.hpp"
#include "types.hpp"

namespace opossum {

class Table;
class JobTask;

/**
 * Operator that performs a predicate search using indices
 *
 * Note: Scans only the set of chunks passed to the constructor
 */
class IndexScan : public AbstractReadOnlyOperator {
  friend class LQPTranslatorTest;

 public:
  IndexScan(const AbstractOperatorCSPtr in, const ColumnIndexType index_type,
            const std::vector<ColumnID> left_column_ids, const PredicateCondition predicate_condition,
            const std::vector<AllTypeVariant> right_values, const std::vector<AllTypeVariant> right_values2 = {});

  const std::string name() const final;

  /**
   * @brief If set, only the specified chunks will be scanned.
   *
   * @see TableScan::set_excluded_chunk_ids for usage
   */
  void set_included_chunk_ids(const std::vector<ChunkID>& chunk_ids);

 protected:
  TableCSPtr _on_execute() final;

  AbstractOperatorSPtr _on_recreate(
      const std::vector<AllParameterVariant>& args, const AbstractOperatorSPtr& recreated_input_left,
      const AbstractOperatorSPtr& recreated_input_right) const override;

  void _validate_input();
  JobTaskSPtr _create_job_and_schedule(const ChunkID chunk_id, std::mutex& output_mutex);
  PosList _scan_chunk(const ChunkID chunk_id);

 private:
  const ColumnIndexType _index_type;
  const std::vector<ColumnID> _left_column_ids;
  const PredicateCondition _predicate_condition;
  const std::vector<AllTypeVariant> _right_values;
  const std::vector<AllTypeVariant> _right_values2;

  std::vector<ChunkID> _included_chunk_ids;

  TableCSPtr _in_table;
  TableSPtr _out_table;
};

}  // namespace opossum
