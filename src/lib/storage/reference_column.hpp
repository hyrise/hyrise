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

  size_t size() const override { return _pos_list->size(); }

  const std::shared_ptr<const PosList> pos_list() const;
  const std::shared_ptr<const Table> referenced_table() const;
  size_t referenced_column_id() const;

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable &visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) override;
};

}  // namespace opossum
