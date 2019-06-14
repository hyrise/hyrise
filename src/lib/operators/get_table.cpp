#include "get_table.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <boost/algorithm/string.hpp>
#include <array>

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
  // Check pruned_chunk_ids
  DebugAssert(std::is_sorted(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()), "Expected sorted vector of ChunkIDs");
  DebugAssert(std::adjacent_find(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()) == _pruned_chunk_ids.end(),
              "Expected vector of unique ChunkIDs");

  // Check pruned_column_ids
  DebugAssert(std::is_sorted(_pruned_column_ids.begin(), _pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");
  DebugAssert(std::adjacent_find(_pruned_column_ids.begin(), _pruned_column_ids.end()) == _pruned_column_ids.end(),
              "Expected vector of unique ColumnIDs");
}

const std::string GetTable::name() const { return "GetTable"; }

const std::string GetTable::description(DescriptionMode description_mode) const {
  const auto stored_table = StorageManager::get().get_table(_name);

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";

  std::stringstream stream;

  stream << name() << separator << "(" << table_name() << ")";

  stream << separator << "pruned:" << separator;
  stream << _pruned_chunk_ids.size() << "/" << stored_table->chunk_count() << " chunk(s)";
  if (description_mode == DescriptionMode::SingleLine) stream << ",";
  stream << separator << _pruned_column_ids.size() << "/" << stored_table->column_count() << " column(s)";

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

//https://stackoverflow.com/questions/478898/how-do-i-execute-a-command-and-get-output-of-command-within-c-using-posix
std::string exec(const std::string cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

std::shared_ptr<const Table> GetTable::_on_execute() {
  const auto stored_table = StorageManager::get().get_table(_name);

  /**
   * Build a sorted vector (`excluded_chunk_ids`) of physically/logically deleted and pruned ChunkIDs
   */
//   if (_name == "system")   {
//     TableColumnDefinitions column_definitions;

//     column_definitions.emplace_back("CPU", DataType::String);
//     column_definitions.emplace_back("MEM", DataType::String);

//     auto t = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
// #ifdef __APPLE__
//     auto top = exec(std::string("top -l 1 | grep ") + std::to_string(getpid()) + " | tail -n 1");
//     const auto cpu_idx = 2, mem_idx = 7;
//     const auto mem_type = "";
// #else
//     auto top = exec(std::string("top -b -n 1 | grep ") + std::to_string(getpid()) + " | tail -n 1");
//     const auto cpu_idx = 9, mem_idx = 10;
//     const auto mem_type = "%";
// #endif
//     std::vector<std::string> strs;
//     boost::split(strs, top, boost::is_any_of(" \t"), boost::token_compress_on);

//     t->append({pmr_string{strs.at(cpu_idx)} + "%", pmr_string{strs.at(mem_idx)} + mem_type});

//     return t;
//   }

  DebugAssert(!transaction_context_is_set() || transaction_context()->phase() == TransactionPhase::Active,
              "Transaction is not active anymore.");
  if (HYRISE_DEBUG && !transaction_context_is_set()) {
    for (ChunkID chunk_id{0}; chunk_id < stored_table->chunk_count(); ++chunk_id) {
      DebugAssert(stored_table->get_chunk(chunk_id) && !stored_table->get_chunk(chunk_id)->get_cleanup_commit_id(),
                  "For tables with physically deleted chunks, the transaction context must be set.");
    }
  }

  auto excluded_chunk_ids = std::vector<ChunkID>{};
  auto pruned_chunk_ids_iter = _pruned_chunk_ids.begin();
  for (ChunkID stored_chunk_id{0}; stored_chunk_id < stored_table->chunk_count(); ++stored_chunk_id) {
    // Check whether the Chunk is pruned
    if (pruned_chunk_ids_iter != _pruned_chunk_ids.end() && *pruned_chunk_ids_iter == stored_chunk_id) {
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      ++pruned_chunk_ids_iter;
      continue;
    }

    // Check whether the Chunk is deleted
    if (transaction_context_is_set()) {
      const auto chunk = stored_table->get_chunk(stored_chunk_id);

      if (!chunk || (chunk->get_cleanup_commit_id() &&
                     *chunk->get_cleanup_commit_id() <= transaction_context()->snapshot_commit_id())) {
        excluded_chunk_ids.emplace_back(stored_chunk_id);
      }
    }
  }

  /**
   * Early out if no exclusion of Chunks or Columns is necessary
   */
  if (excluded_chunk_ids.empty() && _pruned_column_ids.empty()) {
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
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{stored_table->chunk_count() - excluded_chunk_ids.size()};
  auto output_chunks_iter = output_chunks.begin();

  auto excluded_chunk_ids_iter = excluded_chunk_ids.begin();

  for (ChunkID stored_chunk_id{0}; stored_chunk_id < stored_table->chunk_count(); ++stored_chunk_id) {
    // Skip `stored_chunk_id` if it is in the sorted vector `excluded_chunk_ids`
    if (excluded_chunk_ids_iter != excluded_chunk_ids.end() && *excluded_chunk_ids_iter == stored_chunk_id) {
      ++excluded_chunk_ids_iter;
      continue;
    }

    // The Chunk is to be included in the output Table, now we progress to excluding Columns
    const auto stored_chunk = stored_table->get_chunk(stored_chunk_id);

    if (_pruned_column_ids.empty()) {
      *output_chunks_iter = stored_chunk;
    } else {
      auto output_segments = Segments{stored_table->column_count() - _pruned_column_ids.size()};
      auto output_segments_iter = output_segments.begin();

      auto pruned_column_ids_iter = _pruned_column_ids.begin();
      for (auto stored_column_id = ColumnID{0}; stored_column_id < stored_table->column_count(); ++stored_column_id) {
        // Skip `stored_column_id` if it is in the sorted vector `_pruned_column_ids`
        if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
          ++pruned_column_ids_iter;
          continue;
        }

        *output_segments_iter = stored_chunk->get_segment(stored_column_id);
        ++output_segments_iter;
      }

      *output_chunks_iter =
          std::make_shared<Chunk>(std::move(output_segments), stored_chunk->mvcc_data(), stored_chunk->get_allocator());
    }

    ++output_chunks_iter;
  }

  return std::make_shared<Table>(pruned_column_definitions, TableType::Data, std::move(output_chunks),
                                 stored_table->has_mvcc());
}

}  // namespace opossum
