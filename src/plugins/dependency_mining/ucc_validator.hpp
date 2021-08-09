#pragma once

#include "operators/aggregate_hash.hpp"

namespace opossum {

class UCCValidator {
 public:
  UCCValidator(const std::shared_ptr<const Table>& table);

  bool is_unique();

 protected:
  template <typename AggregateKey>
  KeysPerChunk<AggregateKey> _partition_by_groupby_keys();

  template <typename AggregateKey>
  bool _aggregate();

  const std::shared_ptr<const Table> _table;
  std::vector<ColumnID> _groupby_column_ids;

  std::atomic_size_t _expected_result_size{};
  bool _use_immediate_key_shortcut{};
};

}  // namespace opossum
