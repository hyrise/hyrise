#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/statistics_objects/dips_min_max_filter.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TransactionContext;

/**
 * Operator that inserts a number of rows from one table into another.
 * Expects the table name of the table to insert into as a string and
 * the values to insert in a separate table using the same column layout.
 *
 * Assumption: The input has been validated before.
 */
class Insert : public AbstractReadWriteOperator {
 public:
  explicit Insert(const std::string& target_table_name,
                  const std::shared_ptr<const AbstractOperator>& values_to_insert);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_commit_records(const CommitID cid) override;
  void _on_rollback_records() override;

  template <typename T>
  std::shared_ptr<AttributeStatistics<T>> _update_segment_statistic(
      std::shared_ptr<AbstractSegment> source_segment, ChunkOffset source_chunk_offset,
      std::shared_ptr<BaseAttributeStatistics> segment_statistic, ChunkOffset num_rows_current_iteration,
      std::shared_ptr<TransactionContext> context) {
    auto attribute_statistic = std::dynamic_pointer_cast<AttributeStatistics<T>>(segment_statistic);

    std::shared_ptr<AttributeStatistics<T>> result = std::make_shared<AttributeStatistics<T>>();

    segment_with_iterators<T>(*source_segment, [&](const auto source_begin, const auto source_end) {
      auto source_iter = source_begin + source_chunk_offset;

      T new_min;
      T new_max;

      if (attribute_statistic->min_max_filter) {
        new_min = attribute_statistic->min_max_filter->min;
        new_max = attribute_statistic->min_max_filter->max;
      } else {
        new_min = source_iter->value();
        new_max = source_iter->value();
      }

      // Copy values and null values
      for (auto index = ChunkOffset(source_chunk_offset); index < num_rows_current_iteration; index++) {
        if (!source_iter->is_null()) {
          // ValueSegments not being NULLable will be handled over there
          auto value = source_iter->value();
          if (value > new_max) {
            new_max = value;
          }
          if (value < new_min) {
            new_min = value;
          }
        }
        ++source_iter;
      }

      auto new_min_max_filter = std::make_shared<DipsMinMaxFilter<T>>(new_min, new_max, context->transaction_id());
      result->set_statistics_object(new_min_max_filter);
    });

    return result;
  }

 private:
  const std::string _target_table_name;

  // Ranges of rows to which the inserted values are written
  struct ChunkRange {
    ChunkID chunk_id{};
    ChunkOffset begin_chunk_offset{};
    ChunkOffset end_chunk_offset{};
  };
  std::vector<ChunkRange> _target_chunk_ranges;

  std::shared_ptr<Table> _target_table;
};

}  // namespace opossum
