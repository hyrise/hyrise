#include "base_encoded_column.hpp"

#include "storage/column_visitable.hpp"

namespace opossum {

// Encoded columns are immutable
void BaseEncodedColumn::append(const AllTypeVariant&) { Fail("Encoded column is immutable."); }

// Visitor pattern, see base_column.hpp
void BaseEncodedColumn::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) const {
  visitable.handle_encoded_column(*this, std::move(context));
}

}  // namespace opossum
