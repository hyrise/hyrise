#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TransactionContext;
class Insert;

/**
 * Operator that partitions a table.
 * Expects the table name of the table to partition as a string and
 * the target partitioning schema.
 */
class Partitioning : public AbstractReadWriteOperator {
 public:
  explicit Partitioning(const std::string& table_name,
                        std::shared_ptr<AbstractPartitionSchema> target_partition_schema);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  void _on_commit_records(const CommitID cid) override{};
  void _on_rollback_records() override{};

  std::shared_ptr<Table> _get_table_to_be_partitioned();
  std::unique_lock<std::mutex> _lock_table(std::shared_ptr<Table> table_to_lock);
  std::shared_ptr<Table> _create_partitioned_table_copy(std::shared_ptr<Table> table_to_be_partitioned);
  void _copy_table_content(std::shared_ptr<Table> source, std::shared_ptr<Table> target);
  void _replace_table(std::shared_ptr<Table> partitioned_table);

  std::unordered_map<PartitionID, uint32_t> _count_rows_for_partitions(std::map<RowID, PartitionID> target_partition_mapping);

 private:
  const std::string _table_name;
  std::shared_ptr<AbstractPartitionSchema> _target_partition_schema;
  std::shared_ptr<Insert> _insert;
};

}  // namespace opossum
