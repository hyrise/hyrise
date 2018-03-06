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

GetTable::GetTable(const std::string& name) : _name(name) {}

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

const std::vector<ChunkID>& GetTable::excluded_chunk_ids() const { return _excluded_chunk_ids; }

std::shared_ptr<AbstractOperator> GetTable::recreate(const std::vector<AllParameterVariant>& args) const {
  auto copy = std::make_shared<GetTable>(_name);
  copy->set_excluded_chunk_ids(_excluded_chunk_ids);
  return copy;
}

std::shared_ptr<const Table> GetTable::_on_execute() {
  auto original_table = StorageManager::get().get_table(_name);
  if (_excluded_chunk_ids.empty()) {
    return original_table;
  }

  // we create a copy of the original table and don't include the excluded chunks
  auto pruned_table = Table::create_with_layout_from(original_table, original_table->max_chunk_size());
  auto excluded_chunks_set = std::unordered_set<ChunkID>(_excluded_chunk_ids.cbegin(), _excluded_chunk_ids.cend());
  for (ChunkID chunk_id{0}; chunk_id < original_table->chunk_count(); ++chunk_id) {
    if (excluded_chunks_set.count(chunk_id)) {
      continue;
    }
    pruned_table->emplace_chunk(original_table->get_chunk(chunk_id));
  }
  return pruned_table;
}

void GetTable::set_excluded_chunk_ids(const std::vector<ChunkID>& excluded_chunk_ids) {
  _excluded_chunk_ids = excluded_chunk_ids;
}
}  // namespace opossum
