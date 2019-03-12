#include "get_table.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

GetTable::GetTable(const std::string& name) : AbstractReadOnlyOperator(OperatorType::GetTable), _name(name) {}

const std::string GetTable::name() const { return "GetTable"; }

const std::string GetTable::description(DescriptionMode description_mode) const {
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  std::stringstream stream;
  stream << name() << separator << "(" << table_name() << ")";
  if (!_excluded_chunk_ids.empty()) {
    stream << separator << "(" << _excluded_chunk_ids.size() << " Chunks pruned)";
  }
  return stream.str();
}

const std::string& GetTable::table_name() const { return _name; }

void GetTable::set_excluded_chunk_ids(const std::vector<ChunkID>& excluded_chunk_ids) {
  _excluded_chunk_ids = excluded_chunk_ids;
}

std::shared_ptr<AbstractOperator> GetTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  auto copy = std::make_shared<GetTable>(_name);
  copy->set_excluded_chunk_ids(_excluded_chunk_ids);
  return copy;
}

void GetTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> GetTable::_on_execute() {
  DebugAssert(!transaction_context_is_set() || transaction_context()->phase() == TransactionPhase::Active,
              "Transaction is not active anymore.");

  auto original_table = StorageManager::get().get_table(_name);
  auto temp_excluded_chunk_ids = std::vector<ChunkID>(_excluded_chunk_ids);

  if (HYRISE_DEBUG && !transaction_context_is_set()) {
    for (ChunkID chunk_id{0}; chunk_id < original_table->chunk_count(); ++chunk_id) {
      DebugAssert(original_table->get_chunk(chunk_id) && !original_table->get_chunk(chunk_id)->get_cleanup_commit_id(),
                  "For tables with physically deleted chunks, the transaction context must be set.");
    }
  }

  // Add logically & physically deleted chunks to list of excluded chunks
  if (transaction_context_is_set()) {
    for (ChunkID chunk_id{0}; chunk_id < original_table->chunk_count(); ++chunk_id) {
      const auto chunk = original_table->get_chunk(chunk_id);

      if (!chunk || (chunk->get_cleanup_commit_id() &&
                     *chunk->get_cleanup_commit_id() <= transaction_context()->snapshot_commit_id())) {
        temp_excluded_chunk_ids.emplace_back(chunk_id);
      }
    }
  }

  if (temp_excluded_chunk_ids.empty()) {
    return original_table;
  }

  // We create a copy of the original table, but omit excluded chunks
  const auto pruned_table = std::make_shared<Table>(original_table->column_definitions(), TableType::Data,
                                                    original_table->max_chunk_size(), original_table->has_mvcc());

  std::sort(temp_excluded_chunk_ids.begin(), temp_excluded_chunk_ids.end());
  temp_excluded_chunk_ids.erase(std::unique(temp_excluded_chunk_ids.begin(), temp_excluded_chunk_ids.end()),
                                temp_excluded_chunk_ids.end());

  for (ChunkID chunk_id{0}; chunk_id < original_table->chunk_count(); ++chunk_id) {
    const auto chunk = original_table->get_chunk(chunk_id);
    if (chunk && !std::binary_search(temp_excluded_chunk_ids.cbegin(), temp_excluded_chunk_ids.cend(), chunk_id)) {
      pruned_table->append_chunk(chunk);
    }
  }

  return pruned_table;
}

}  // namespace opossum
