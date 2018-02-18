#include "base_dictionary_column.hpp"

#include "storage/column_visitable.hpp"

namespace opossum {

EncodingType BaseDictionaryColumn::encoding_type() const { return EncodingType::Dictionary; }

void BaseDictionaryColumn::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) const {
  visitable.handle_column(*this, std::move(context));
}

}  // namespace opossum
