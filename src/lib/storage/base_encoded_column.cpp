#include "base_encoded_column.hpp"

#include "storage/column_visitable.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

void BaseEncodedColumn::append(const AllTypeVariant&) { Fail("Encoded column is immutable."); }

void BaseEncodedColumn::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) const {
  visitable.handle_column(*this, std::move(context));
}

CompressedVectorType BaseEncodedColumn::compressed_vector_type() const { return CompressedVectorType::Invalid; }

}  // namespace opossum
