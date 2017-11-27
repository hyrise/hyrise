#pragma once

#include <memory>

#include "abstract_read_only_operator.hpp"

#include "all_type_variant.hpp"
#include "storage/index/column_index_type.hpp"
#include "types.hpp"

namespace opossum {

class Table;

/**
 * Operator that performs a predicate search using indices
 *
 * Note: Scans only the set of chunks passed to the constructor
 */
class IndexScan : public AbstractReadOnlyOperator {
 public:
  IndexScan(std::shared_ptr<AbstractOperator> in, std::vector<ChunkID> chunk_ids, const ColumnIndexType index_type,
            std::vector<ColumnID> left_column_ids, const ScanType scan_type, std::vector<AllTypeVariant> right_values,
            std::vector<AllTypeVariant> right_values2 = {});

  const std::string name() const final;

 protected:
  std::shared_ptr<const Table> _on_execute() final;
  void _on_cleanup() final;

  void _validate_input();
  PosList _scan_chunk(const ChunkID chunk_id);

 private:
  const std::vector<ChunkID> _chunk_ids;
  const ColumnIndexType _index_type;
  const std::vector<ColumnID> _left_column_ids;
  const ScanType _scan_type;
  const std::vector<AllTypeVariant> _right_values;
  const std::vector<AllTypeVariant> _right_values2;

  std::shared_ptr<const Table> _in_table;
  std::shared_ptr<Table> _out_table;
};

}  // namespace opossum
