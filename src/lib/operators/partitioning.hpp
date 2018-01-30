#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TransactionContext;
class Insert;

class Partitioning : public AbstractReadWriteOperator {
 public:
  explicit Partitioning(const std::string& table_to_partition_name, std::shared_ptr<AbstractPartitionSchema> target_partition_schema);

  ~Partitioning();

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  void _on_commit_records(const CommitID cid) override;
  void _on_rollback_records() override;

  std::shared_ptr<const Table> _get_and_drop_table_to_be_partitioned();
  std::shared_ptr<Table> _create_partitioned_table_copy(std::shared_ptr<const Table> table_to_be_partitioned);
  void _copy_table_content(const std::shared_ptr<const Table> source, std::shared_ptr<TransactionContext> context);

 private:
  const std::string _table_to_partition_name;
  std::shared_ptr<AbstractPartitionSchema> _target_partition_schema;
  std::shared_ptr<Insert> _insert;
};

}  // namespace opossum
