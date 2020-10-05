#pragma once

#include <memory>
#include <string>
#include <vector>

#include "../operators/property.hpp"
#include "../visualization/serializer/json_serializer.hpp"
#include "abstract_read_only_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Validates visibility of records of a table
 * within the context of a given transaction
 *
 * Assumption: Validate happens before joins.
 */
class Validate : public AbstractReadOnlyOperator {
  friend class OperatorsValidateTest;

 public:
  explicit Validate(const std::shared_ptr<AbstractOperator>& in);

  const std::string& name() const override;

  // MVCC evaluation logic is exposed so that it can be used in asserts and tests
  static bool is_row_visible(TransactionID our_tid, CommitID snapshot_commit_id, const TransactionID row_tid,
                             const CommitID begin_cid, const CommitID end_cid);

 private:
  void _validate_chunks(const std::shared_ptr<const Table>& in_table, const ChunkID chunk_id_start,
                        const ChunkID chunk_id_end, const TransactionID our_tid, const TransactionID snapshot_commit_id,
                        std::vector<std::shared_ptr<Chunk>>& output_chunks, std::mutex& output_mutex) const;

  // This is a performance optimization that can only be used if a couple of conditions are met, i.e., if
  // _can_use_chunk_shortcut is true. Consult _on_execute() for more details on the conditions.
  bool _is_entire_chunk_visible(const std::shared_ptr<const Chunk>& chunk, const CommitID snapshot_commit_id) const;

  bool _can_use_chunk_shortcut = true;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> transaction_context) override;
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  friend class JsonSerializer;

 public:
  // TODO(CAJan93): Support all relevant members, including parent members.
  inline constexpr static auto properties = std::make_tuple(
      property(&Validate::_can_use_chunk_shortcut, "_can_use_chunk_shortcut"),
      // from AbstractOperator via AbstractReadOnlyOperator
      /*property(&Validate::lqp_node, "lqp_node"), property(&Validate::performance_data, "performance_data"),*/
      property(&Validate::_type, "_type"), property(&Validate::_left_input, "_left_input"),
      property(&Validate::_right_input, "_right_input")
      /*, property(&Validate::_transaction_context, "_transaction_context")*/
      /*property(&Validate::_output, "_output"), // should not be important*/
  );
};
}  // namespace opossum
