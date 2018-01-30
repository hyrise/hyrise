#include "partitioning.hpp"

#include "insert.hpp"
#include "storage/storage_manager.hpp"
#include "table_wrapper.hpp"

namespace opossum {

Partitioning::Partitioning(const std::string& table_to_partition_name, std::shared_ptr<AbstractPartitionSchema> target_partition_schema)
    : AbstractReadWriteOperator(), _table_to_partition_name{table_to_partition_name}, _target_partition_schema{target_partition_schema} {}

Partitioning::~Partitioning() = default;

const std::string Partitioning::name() const { return "Partitioning"; }

std::shared_ptr<const Table> Partitioning::_on_execute(std::shared_ptr<TransactionContext> context) {

  const auto table_to_partition = _get_and_drop_table_to_be_partitioned();
  auto partitioned_table = _create_partitioned_table_copy(table_to_partition);
  _copy_table_content(table_to_partition, context);

  return nullptr;
}

std::shared_ptr<const Table> Partitioning::_get_and_drop_table_to_be_partitioned() {
  const auto table_to_be_partitioned = StorageManager::get().get_table(_table_to_partition_name);

  // drop table but keep it in memory because of the shared_ptr for now
  // otherwise, the "new" partitioned table cannot be added to the StorageManager
  StorageManager::get().drop_table(_table_to_partition_name);
  return table_to_be_partitioned;
}

std::shared_ptr<Table> Partitioning::_create_partitioned_table_copy(std::shared_ptr<const Table> table_to_be_partitioned) {
  auto partitioned_table = Table::create_with_layout_from(table_to_be_partitioned, table_to_be_partitioned->max_chunk_size());
  StorageManager::get().add_table(_table_to_partition_name, partitioned_table);
  partitioned_table->apply_partitioning(_target_partition_schema);
  return partitioned_table;
}

void Partitioning::_copy_table_content(const std::shared_ptr<const Table> source, std::shared_ptr<TransactionContext> context) {
  auto helper_operator = std::make_shared<TableWrapper>(source);
  helper_operator->execute();

  _insert = std::make_shared<Insert>(_table_to_partition_name, helper_operator);
  _insert->set_transaction_context(context);

  _insert->execute();
}

}  // namespace opossum
