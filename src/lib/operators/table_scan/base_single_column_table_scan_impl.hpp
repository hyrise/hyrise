#pragma once

#include <memory>
#include <unordered_map>

#include "base_table_scan_impl.hpp"

#include "storage/column_visitable.hpp"
#include "storage/iterables/base_iterators.hpp"

#include "types.hpp"

namespace opossum {

class Table;
class ReferenceColumn;

/**
 * @brief Resolves reference columns
 *
 * The position list of reference columns is split by the referenced columns and
 * then each one is visited separately.
 */
class BaseSingleColumnTableScanImpl : public BaseTableScanImpl, public ColumnVisitable {
 public:
  BaseSingleColumnTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id,
                                const ScanType scan_type, const bool skip_null_row_ids = true);

  PosList scan_chunk(const ChunkID &chunk_id) override;

  void handle_reference_column(ReferenceColumn &left_column,
                               std::shared_ptr<ColumnVisitableContext> base_context) override;

 protected:
  /**
   * @brief the context used for the columns’ visitor pattern
   */
  struct Context : public ColumnVisitableContext {
    Context(const ChunkID chunk_id, PosList &matches_out) : _chunk_id{chunk_id}, _matches_out{matches_out} {}

    Context(const ChunkID chunk_id, PosList &matches_out,
            std::unique_ptr<std::vector<std::pair<ChunkOffset, ChunkOffset>>> mapped_chunk_offsets)
        : _chunk_id{chunk_id}, _matches_out{matches_out}, _mapped_chunk_offsets{std::move(mapped_chunk_offsets)} {}

    const ChunkID _chunk_id;
    PosList &_matches_out;

    std::unique_ptr<std::vector<std::pair<ChunkOffset, ChunkOffset>>> _mapped_chunk_offsets;
  };

 private:
  /**
   * @defgroup Methods used for handling reference columns
   * @{
   */

  using ChunkOffsetsByChunkID = std::unordered_map<ChunkID, ChunkOffsetsList>;

  // If _skip_null_row_ids == true, the result won’t include row IDs that are equal to NULL_ROW_ID
  ChunkOffsetsByChunkID _split_pos_list_by_chunk_id(const PosList &pos_list);

  /**@}*/

 private:
  const bool _skip_null_row_ids;
};

}  // namespace opossum
