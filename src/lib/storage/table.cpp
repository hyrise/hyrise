#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "dictionary_column.hpp"

namespace opossum {

Table::Table(const size_t chunk_size, const bool auto_compress)
    : append_mtx(std::make_unique<std::mutex>()), _chunk_size(chunk_size), _auto_compress(auto_compress) {
  _chunks.push_back(Chunk{true});
}

void Table::add_column(const std::string &name, const std::string &type, bool create_value_column) {
  _column_names.push_back(name);
  _column_types.push_back(type);
  if (create_value_column) {
    for (auto &chunk : _chunks) {
      chunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
    }
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  // TODO(Anyone): Chunks should be preallocated for chunk size
  if (_chunk_size > 0 && _chunks.back().size() == _chunk_size) {
    if (_auto_compress) compress_chunk(chunk_count() - 1);

    // creates chunk with mvcc columns
    Chunk newChunk{true};
    for (auto &&type : _column_types) {
      newChunk.add_column(make_shared_by_column_type<BaseColumn, ValueColumn>(type));
    }
    _chunks.push_back(std::move(newChunk));
  }

  _chunks.back().append(values);
}

size_t Table::col_count() const { return _column_types.size(); }

size_t Table::row_count() const {
  size_t ret = 0;
  for (auto &&chunk : _chunks) {
    ret += chunk.size();
  }
  return ret;
}

size_t Table::chunk_count() const { return _chunks.size(); }

size_t Table::column_id_by_name(const std::string &column_name) const {
  for (size_t column_id = 0; column_id < col_count(); ++column_id) {
    // TODO(Anyone): make more efficient
    if (_column_names[column_id] == column_name) {
      return column_id;
    }
  }
  throw std::runtime_error("column " + column_name + " not found");
}

void Table::compress_chunk(ChunkID chunk_id) {
  Chunk newChunk;
  for (size_t column_id = 0; column_id < col_count(); ++column_id) {
    auto dict_col = make_shared_by_column_type<BaseColumn, DictionaryColumn>(
        column_type(column_id), _chunks.at(chunk_id).get_column(column_id));
    newChunk.add_column(std::move(dict_col));
  }

  _chunks[chunk_id] = std::move(newChunk);
}

size_t Table::chunk_size() const { return _chunk_size; }

const std::string &Table::column_name(size_t column_id) const { return _column_names[column_id]; }

const std::string &Table::column_type(size_t column_id) const { return _column_types[column_id]; }

Chunk &Table::get_chunk(ChunkID chunk_id) { return _chunks[chunk_id]; }
const Chunk &Table::get_chunk(ChunkID chunk_id) const { return _chunks[chunk_id]; }

void Table::add_chunk(Chunk chunk) {
  if (_chunks.size() == 1 && _chunks.back().col_count() == 0) {
    // the initial chunk was not used yet
    _chunks.clear();
  }
  if (IS_DEBUG && _chunks.size() > 0 && chunk.col_count() != col_count()) {
    throw std::runtime_error(std::string("adding chunk with ") + std::to_string(chunk.col_count()) +
                             " columns to table with " + std::to_string(col_count()) + " columns");
  }
  _chunks.emplace_back(std::move(chunk));
}

}  // namespace opossum
