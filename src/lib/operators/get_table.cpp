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

GetTable::GetTable(const std::string& name) : GetTable(name, {}, {}) {}

GetTable::GetTable(const std::string& name, const std::vector<ChunkID>& pruned_chunk_ids,
                   const std::vector<ColumnID>& pruned_column_ids)
    : AbstractReadOnlyOperator(OperatorType::GetTable),
      _name(name),
      _pruned_chunk_ids(pruned_chunk_ids),
      _pruned_column_ids(pruned_column_ids) {
  DebugAssert(std::is_sorted(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()), "Expected sorted vector of ChunkIDs");
  DebugAssert(std::is_sorted(_pruned_column_ids.begin(), _pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");
}

const std::string GetTable::name() const { return "GetTable"; }

const std::string GetTable::description(DescriptionMode description_mode) const {
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";

  std::stringstream stream;
  stream << name() << separator << "(" << table_name() << ")";
  if (!_pruned_chunk_ids.empty()) {
    stream << separator << "pruned chunks: " << _pruned_chunk_ids.size();
  }
  if (!_pruned_column_ids.empty()) {
    stream << separator << "pruned columns: " << _pruned_column_ids.size();
  }

  return stream.str();
}

const std::string& GetTable::table_name() const { return _name; }

const std::vector<ChunkID>& GetTable::pruned_chunk_ids() const { return _pruned_chunk_ids; }

const std::vector<ColumnID>& GetTable::pruned_column_ids() const { return _pruned_column_ids; }

std::shared_ptr<AbstractOperator> GetTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<GetTable>(_name, _pruned_chunk_ids, _pruned_column_ids);
}

void GetTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> GetTable::_on_execute() {
  auto stored_table = StorageManager::get().get_table(_name);

  /**
   * Build a sorted vector physically or logically deleted ChunkIDs
   */
  DebugAssert(!transaction_context_is_set() || transaction_context()->phase() == TransactionPhase::Active,
              "Transaction is not active anymore.");
  if (HYRISE_DEBUG && !transaction_context_is_set()) {
    for (ChunkID chunk_id{0}; chunk_id < stored_table->chunk_count(); ++chunk_id) {
      DebugAssert(stored_table->get_chunk(chunk_id) && !stored_table->get_chunk(chunk_id)->get_cleanup_commit_id(),
                  "For tables with physically deleted chunks, the transaction context must be set.");
    }
  }

  auto deleted_chunk_ids = std::vector<ChunkID>(_pruned_chunk_ids);
  if (transaction_context_is_set()) {
    for (ChunkID chunk_id{0}; chunk_id < stored_table->chunk_count(); ++chunk_id) {
      const auto chunk = stored_table->get_chunk(chunk_id);

      if (!chunk || (chunk->get_cleanup_commit_id() &&
                     *chunk->get_cleanup_commit_id() <= transaction_context()->snapshot_commit_id())) {
        deleted_chunk_ids.emplace_back(chunk_id);
      }
    }
  }

  /**
   * Early out if no pruning of Chunks or Columns is necessary
   */
  if (deleted_chunk_ids.empty() && _pruned_chunk_ids.empty() && _pruned_column_ids.empty()) {
    return stored_table;
  }

  // We cannot create a Table without columns - since Chunks rely on their first column to determine their row count
  Assert(_pruned_column_ids.size() < stored_table->column_count(), "Cannot prune all columns from Table");
  DebugAssert(std::all_of(_pruned_column_ids.begin(), _pruned_column_ids.end(),
                          [&](const auto column_id) { return column_id < stored_table->column_count(); }),
              "ColumnID out of range");

  /**
   * Build pruned TableColumnDefinitions of the output Table
   */
  auto pruned_column_definitions = TableColumnDefinitions{};
  if (_pruned_column_ids.empty()) {
    pruned_column_definitions = stored_table->column_definitions();
  } else {
    pruned_column_definitions =
        TableColumnDefinitions{stored_table->column_definitions().size() - _pruned_column_ids.size()};

    auto pruned_column_ids_iter = _pruned_column_ids.begin();
    for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0};
         stored_column_id < stored_table->column_count(); ++stored_column_id) {
      if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
        ++pruned_column_ids_iter;
        continue;
      }

      pruned_column_definitions[output_column_id] = stored_table->column_definitions()[stored_column_id];
      ++output_column_id;
    }
  }

  /**
   * Build the output Table, omitting pruned Chunks and Columns as well as deleted Chunks
   */
  const auto output_table = std::make_shared<Table>(pruned_column_definitions, TableType::Data,
                                                    stored_table->max_chunk_size(), stored_table->has_mvcc());

  auto pruned_chunk_ids_iter = _pruned_chunk_ids.begin();
  auto deleted_chunk_ids_iter = deleted_chunk_ids.begin();

  for (ChunkID chunk_id{0}; chunk_id < stored_table->chunk_count(); ++chunk_id) {
    // Exclude the Chunk if is either pruned or deleted.
    // A Chunk can be both, so we have to make sure both iterators are incremented in that case
    const auto chunk_id_is_pruned =
        pruned_chunk_ids_iter != _pruned_chunk_ids.end() && *pruned_chunk_ids_iter == chunk_id;
    const auto chunk_id_is_deleted =
        deleted_chunk_ids_iter != deleted_chunk_ids.end() && *deleted_chunk_ids_iter == chunk_id;

    if (chunk_id_is_pruned) ++pruned_chunk_ids_iter;
    if (chunk_id_is_deleted) ++deleted_chunk_ids_iter;

    if (chunk_id_is_pruned || chunk_id_is_deleted) {
      continue;
    }

    // The Chunk is to be included in the output Table, now we progress to excluding Columns
    const auto stored_chunk = stored_table->get_chunk(chunk_id);

    if (_pruned_column_ids.empty()) {
      output_table->append_chunk(stored_chunk);
    } else {
      auto output_segments = Segments{stored_table->column_count() - _pruned_column_ids.size()};

      auto pruned_column_ids_iter = _pruned_column_ids.begin();
      for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0};
           stored_column_id < stored_table->column_count(); ++stored_column_id) {
        if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
          ++pruned_column_ids_iter;
          continue;
        }

        output_segments[output_column_id] = stored_chunk->get_segment(stored_column_id);
        ++output_column_id;
      }

      output_table->append_chunk(output_segments, stored_chunk->mvcc_data(), stored_chunk->get_allocator());
    }
  }

  return output_table;
}

}  // namespace opossum
