#pragma once

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "table.hpp"

namespace opossum {

// ReferenceColumn is a specific column type that stores all its values as position list of a referenced column
class ReferenceColumn : public BaseColumn {
  // TODO(Anyone): move implementation to CPP

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
                  const std::shared_ptr<const PosList> pos)
      : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos) {
    if (IS_DEBUG) {
      auto referenced_column = _referenced_table->get_chunk(0).get_column(referenced_column_id);
      auto reference_col = std::dynamic_pointer_cast<ReferenceColumn>(referenced_column);
      if (reference_col != nullptr) {
        // cast was successful, but was expected to fail
        throw std::logic_error("referenced_column must not be a ReferenceColumn");
      }
    }
  }

  virtual const AllTypeVariant operator[](const size_t i) const DEV_ONLY {
    auto chunk_info = _referenced_table->locate_row((*_pos_list).at(i));
    auto &chunk = _referenced_table->get_chunk(chunk_info.first);

    return (*chunk.get_column(_referenced_column_id))[chunk_info.second];
  }

  virtual void append(const AllTypeVariant &) { throw std::logic_error("ReferenceColumn is immutable"); }

  virtual size_t size() const { return _pos_list->size(); }

  const std::shared_ptr<const PosList> pos_list() const { return _pos_list; }
  const std::shared_ptr<const Table> referenced_table() const { return _referenced_table; }
  size_t referenced_column_id() const { return _referenced_column_id; }

  // visitor pattern, see base_column.hpp
  virtual void visit(ColumnVisitable &visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) {
    visitable.handle_reference_column(*this, std::move(context));
  }
};

// TODO(Anyone): Dokumentieren, dass nicht alle Chunks einer Tabelle gleich groß sind.
// Wenn man einen Union aus zwei Tabellen mit jeweils einem Chunk macht, entstehenden zwei ReferenceColumns, die auf
// verschiedene Tabellen
// verweisen und unterschiedlich lang sind
}  // namespace opossum
