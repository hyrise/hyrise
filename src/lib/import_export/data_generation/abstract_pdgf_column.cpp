#include "abstract_pdgf_column.hpp"

namespace hyrise {
DataType hyrise_type_for_column_type(ColumnType column_type) {
  switch (column_type) {
    case STRING: return DataType::String;
    case BOOL:
    case INTEGER:
      return DataType::Int;
    case LONG: return DataType::Long;
    case DOUBLE: return DataType::Double;
    default: throw std::runtime_error("Unrecognized column type");
  }
}

AbstractPDGFColumn::AbstractPDGFColumn(int64_t num_rows, ChunkOffset chunk_size) : _num_rows(num_rows), _chunk_size(chunk_size) {}
} // namespace hyrise
