#include "reference_column.hpp"

#include <memory>
#include <string>
#include <utility>

#include "column_visitable.hpp"

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

ReferenceColumn::ReferenceColumn(const std::shared_ptr<const Table> referenced_table,
                                 const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos) {
  if (IS_DEBUG) {
    auto referenced_column = _referenced_table->get_chunk(ChunkID{0}).get_column(referenced_column_id);
    auto reference_col = std::dynamic_pointer_cast<ReferenceColumn>(referenced_column);

    DebugAssert(!(reference_col), "referenced_column must not be a ReferenceColumn");
  }
}

const AllTypeVariant ReferenceColumn::operator[](const size_t i) const {
  PerformanceWarning("operator[] used");

  auto chunk_info = _pos_list->at(i);

  if (chunk_info == NULL_ROW_ID) return NULL_VALUE;

  auto &chunk = _referenced_table->get_chunk(chunk_info.chunk_id);

  return (*chunk.get_column(_referenced_column_id))[chunk_info.chunk_offset];
}

void ReferenceColumn::append(const AllTypeVariant &) { Fail("ReferenceColumn is immutable"); }

const std::shared_ptr<const PosList> ReferenceColumn::pos_list() const { return _pos_list; }
const std::shared_ptr<const Table> ReferenceColumn::referenced_table() const { return _referenced_table; }
ColumnID ReferenceColumn::referenced_column_id() const { return _referenced_column_id; }

size_t ReferenceColumn::size() const { return _pos_list->size(); }

void ReferenceColumn::visit(ColumnVisitable &visitable, std::shared_ptr<ColumnVisitableContext> context) {
  visitable.handle_reference_column(*this, std::move(context));
}

// writes the length and value at the chunk_offset to the end off row_string
void ReferenceColumn::write_string_representation(std::string &row_string, const ChunkOffset chunk_offset) const {
  // retrieving the chunk_id for the given chunk_offset
  auto chunk_info = _referenced_table->locate_row((*_pos_list).at(chunk_offset));
  // call the equivalent function of the referenced value column
  _referenced_table->get_chunk(chunk_info.first)
      .get_column(_referenced_column_id)
      ->write_string_representation(row_string, chunk_offset);
}

// copies one of its own values to a different ValueColumn - mainly used for materialization
// we cannot always use the materialize method below because sort results might come from different BaseColumns
void ReferenceColumn::copy_value_to_value_column(BaseColumn &, ChunkOffset) const {
  Fail("It is not allowed to copy directly from a reference column");
}

}  // namespace opossum
