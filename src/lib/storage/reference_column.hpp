#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "base_column.hpp"
#include "table.hpp"

namespace opossum {

// ReferencColumn is a specific column type that stores all its values as position list of a referenced column
class ReferenceColumn : public BaseColumn {
  // TODO(Anyone): move implementation to CPP

 protected:
  const std::shared_ptr<Table> _referenced_table;
  const size_t _referenced_column_id;

  // nullptr in _pos_list means all positions of the referenced column
  const std::shared_ptr<PosList> _pos_list;

 public:
  // creates a reference column
  // the parameters specify the positions and the referenced column
  // _pos_list == nullptr means all positions
  ReferenceColumn(const std::shared_ptr<Table> referenced_table, const size_t referenced_column_id,
                  const std::shared_ptr<PosList> pos)
      : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos) {
    if (IS_DEBUG) {
      auto referenced_column = _referenced_table->get_chunk(0).get_column(referenced_column_id);
      auto reference_col = std::dynamic_pointer_cast<ReferenceColumn>(referenced_column);
      if (reference_col != NULL) {
        // cast was successful, but was expected to fail
        throw std::logic_error("referenced_column must not be a ReferenceColumn");
      }
    }
  }

  virtual const AllTypeVariant operator[](const size_t i) const DEV_ONLY {
    if (_pos_list) {
      auto chunk_info = _referenced_table->locate_row((*_pos_list)[i]);
      auto &chunk = _referenced_table->get_chunk(chunk_info.first);

      return (*chunk.get_column(_referenced_column_id))[chunk_info.second];
    } else {
      // handle the special case that all positions are referenced, i.e., _pos_list == nullptr)
      auto chunk_size = _referenced_table->chunk_size();
      auto &chunk = _referenced_table->get_chunk(i / chunk_size);
      return (*chunk.get_column(_referenced_column_id))[i % chunk_size];
    }
  }

  virtual void append(const AllTypeVariant &) { throw std::logic_error("ReferenceColumn is immutable"); }

  virtual size_t size() const {
    if (_pos_list) {
      return _pos_list->size();
    } else {
      // handle the special case that all positions are referenced, i.e., _pos_list == nullptr)
      return _referenced_table->row_count();
    }
  }

  const std::shared_ptr<PosList> pos_list() const { return _pos_list; }
  const std::shared_ptr<Table> referenced_table() const { return _referenced_table; }
};

// TODO(Anyone): Dokumentieren, dass nicht alle Chunks einer Tabelle gleich groß sind.
// Wenn man einen Union aus zwei Tabellen mit jeweils einem Chunk macht, entstehenden zwei ReferenceColumns, die auf
// verschiedene Tabellen
// verweisen und unterschiedlich lang sind
}  // namespace opossum
