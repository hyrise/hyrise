#include "base_encoded_column.hpp"

#include "storage/column_visitable.hpp"
#include "utils/assert.hpp"

namespace opossum {

void BaseEncodedColumn::append(const AllTypeVariant&) { Fail("Encoded column is immutable."); }

void BaseEncodedColumn::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) const {
  visitable.handle_column(*this, std::move(context));
}

}  // namespace opossum
