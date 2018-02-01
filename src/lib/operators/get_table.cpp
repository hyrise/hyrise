#include "get_table.hpp"

#include <experimental/optional>
#include <memory>
#include <string>
#include <vector>

#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

GetTable::GetTable(const std::string& name) : _name(name) {}

const std::string GetTable::name() const { return "GetTable"; }

const std::string GetTable::description(DescriptionMode description_mode) const {
  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";
  auto prune_description =
      _excluded_chunks ? std::string() + separator + "(" + std::to_string(_excluded_chunks->size()) + " Chunks pruned)"
                       : "";
  return name() + separator + "(" + table_name() + ")" + prune_description;
}

const std::string& GetTable::table_name() const { return _name; }

std::shared_ptr<AbstractOperator> GetTable::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<GetTable>(_name);
}

std::shared_ptr<const Table> GetTable::_on_execute() {
  auto original_table = StorageManager::get().get_table(_name);
  if (!_excluded_chunks || _excluded_chunks->empty()) {
    return original_table;
  }
  auto pruned_table = Table::create_with_layout_from(original_table, original_table->max_chunk_size());
  auto excluded_chunks_it = _excluded_chunks->begin();
  for (ChunkID i = ChunkID{0}; i < original_table->chunk_count(); ++i) {
    if (excluded_chunks_it != _excluded_chunks->end()) {
      DebugAssert(i <= (*excluded_chunks_it), "Excluded Chunks vector must be sorted");
      if (*excluded_chunks_it == i) {
        ++excluded_chunks_it;
        continue;
      }
    }
    pruned_table->emplace_chunk(original_table->get_chunk(i));
  }
  return pruned_table;
}

void GetTable::set_excluded_chunks(const std::vector<ChunkID>& excluded_chunks) {
  _excluded_chunks.emplace(excluded_chunks);
}
}  // namespace opossum
