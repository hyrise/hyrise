#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "base_column.hpp"
#include "table.hpp"

namespace opossum {

class ReferenceColumn : public BaseColumn {
  // TODO(Anyone): move implementation to CPP

 protected:
  const std::shared_ptr<Table> _referenced_table;
  const size_t _referenced_column_id;
  const std::shared_ptr<PosList> _pos_list;

 public:
  ReferenceColumn(const std::shared_ptr<Table> referenced_table, const size_t referenced_column_id,
                  const std::shared_ptr<PosList> pos)
      : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos) {
    if (DEBUG) {
      auto referenced_column = _referenced_table->get_chunk(0).get_column(referenced_column_id);
      auto reference_col = std::dynamic_pointer_cast<ReferenceColumn>(referenced_column);
      if (reference_col != NULL) {
        // cast was successful, but was expected to fail
        throw std::logic_error("referenced_column must not be a ReferenceColumn");
      }
    }
  }

  virtual AllTypeVariant operator[](const size_t i) const DEV_ONLY {
    auto &chunk = _referenced_table->get_chunk(get_chunk_id_from_row_id(i));
    return (*chunk.get_column(_referenced_column_id))[get_chunk_offset_from_row_id(i)];
  }

  virtual void append(const AllTypeVariant &) { throw std::logic_error("ReferenceColumn is immutable"); }

  virtual size_t size() const { return _pos_list->size(); }

  const std::shared_ptr<PosList> get_pos_list() const { return _pos_list; }
};

// TODO(Anyone): Dokumentieren, dass nicht alle Chunks einer Tabelle gleich groß sind.
// Wenn man einen Union aus zwei Tabellen mit jeweils einem Chunk macht, entstehenden zwei ReferenceColumns, die auf
// verschiedene Tabellen
// verweisen und unterschiedlich lang sind
}  // namespace opossum
