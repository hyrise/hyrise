#include "column_iterable.hpp"

namespace opossum {

ColumnIterable::ColumnIterable(const std::shared_ptr<const Table>& table, const ColumnID column_id)
    : table(table), column_id(column_id) {}

}  // namespace opossum
