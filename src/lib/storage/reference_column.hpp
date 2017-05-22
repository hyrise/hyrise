#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "dictionary_column.hpp"
#include "table.hpp"
#include "value_column.hpp"

#include "types.hpp"

namespace opossum {

// ReferenceColumn is a specific column type that stores all its values as position list of a referenced column
class ReferenceColumn : public BaseColumn {
 protected:
  // After an operator finishes, its shared_ptr reference to the table gets deleted. Thus, the ReferenceColumns need
  // their own shared_ptrs
  const std::shared_ptr<const Table> _referenced_table;

  const size_t _referenced_column_id;

  // The position list can be shared amongst multiple columns
  const std::shared_ptr<const PosList> _pos_list;

 public:
  // creates a reference column
  // the parameters specify the positions and the referenced column
  ReferenceColumn(const std::shared_ptr<const Table> referenced_table, const size_t referenced_column_id,
                  const std::shared_ptr<const PosList> pos);

  const AllTypeVariant operator[](const size_t i) const override;

  void append(const AllTypeVariant &) override;

  // return generated vector of all values
  template <typename T>
  const tbb::concurrent_vector<T> materialize_values() const {
    tbb::concurrent_vector<T> values;
    values.reserve(_pos_list->size());

    std::map<ChunkID, std::shared_ptr<ValueColumn<T>>> value_columns;
    std::map<ChunkID, std::shared_ptr<DictionaryColumn<T>>> dict_columns;

    for (const RowID &row : *_pos_list) {
      auto search = value_columns.find(row.chunk_id);
      if (search != value_columns.end()) {
        values.push_back(search->second->get(row.chunk_offset));
        continue;
      }
      auto search_dict = dict_columns.find(row.chunk_id);
      if (search_dict != dict_columns.end()) {
        values.push_back(search_dict->second->get(row.chunk_offset));
        continue;
      }

      auto &chunk = _referenced_table->get_chunk(row.chunk_id);
      std::shared_ptr<BaseColumn> column = chunk.get_column(_referenced_column_id);

      if (auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(column)) {
        value_columns[row.chunk_id] = value_column;
        values.push_back(value_column->get(row.chunk_offset));
        continue;
      }

      if (auto dict_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(column)) {
        dict_columns[row.chunk_id] = dict_column;
        values.push_back(dict_column->get(row.chunk_offset));
        continue;
      }
      throw std::logic_error("column is no dictonary or value column");
    }

    return values;
  }

  size_t size() const override;

  const std::shared_ptr<const PosList> pos_list() const;
  const std::shared_ptr<const Table> referenced_table() const;

  size_t referenced_column_id() const;

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable &visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) override;

  // writes the length and value at the chunk_offset to the end off row_string
  void write_string_representation(std::string &row_string, const ChunkOffset chunk_offset) const override;

  template <typename ContextClass>
  void visit_dereferenced(ColumnVisitable &visitable, std::shared_ptr<ColumnVisitableContext> ctx) {
    /*
    The pos_list might be unsorted. In that case, we would have to jump around from chunk to chunk.
    One-chunk-at-a-time processing should be faster. For this, we place a pair {chunk_offset, original_position}
    into a vector for each chunk. A potential optimization would be to only do this if the pos_list is really
    unsorted.
    */
    std::vector<std::shared_ptr<std::vector<ChunkOffset>>> all_chunk_offsets(_referenced_table->chunk_count());

    for (ChunkID chunk_id = 0; chunk_id < _referenced_table->chunk_count(); ++chunk_id) {
      all_chunk_offsets[chunk_id] = std::make_shared<std::vector<ChunkOffset>>();
    }

    for (auto pos : *(_pos_list)) {
      auto chunk_info = _referenced_table->locate_row(pos);
      all_chunk_offsets[chunk_info.first]->emplace_back(chunk_info.second);
    }

    for (ChunkID chunk_id = 0; chunk_id < _referenced_table->chunk_count(); ++chunk_id) {
      if (all_chunk_offsets[chunk_id]->empty()) {
        continue;
      }
      auto &chunk = _referenced_table->get_chunk(chunk_id);
      auto referenced_column = chunk.get_column(_referenced_column_id);

      auto c = std::make_shared<ContextClass>(referenced_column, _referenced_table, ctx, chunk_id,
                                              all_chunk_offsets[chunk_id]);
      referenced_column->visit(visitable, c);
    }
  }

  // copies one of its own values to a different ValueColumn - mainly used for materialization
  // we cannot always use the materialize method below because sort results might come from different BaseColumns
  void copy_value_to_value_column(BaseColumn &, ChunkOffset) const override;
};

}  // namespace opossum
