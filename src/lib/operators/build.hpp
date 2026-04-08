#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "operators/join_hash/join_hash_steps.hpp"
#include "operators/join_hash/join_hash_traits.hpp"
#include "types.hpp"

namespace hyrise {

class BaseBuildStatistics : public Noncopyable {};

template <typename T, typename HashedType>
class BuildStatistics : public BaseBuildStatistics {
 public:
  std::vector<std::optional<PosHashTable<HashedType>>> hash_tables;
  T min;
  T max;
  bool is_continuous;
  uint64_t row_count;
};

extern template class BuildStatistics<int32_t, int32_t>;
extern template class BuildStatistics<int32_t, int64_t>;
extern template class BuildStatistics<int64_t, int64_t>;

class Build : public AbstractReadOnlyOperator {
 public:
  Build(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID column_id,
        const DataType build_type, const DataType probe_type);

  const std::string& name() const override;

  ColumnID column_id() const;
  const BloomFilter& bloom_filter() const;
  size_t radix_bits() const;
  DataType build_data_type() const;
  DataType probe_data_type() const;

  const BaseBuildStatistics& build_statistics() const;
  void clear_statistics() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  ColumnID _column_id;
  DataType _build_type;
  DataType _probe_type;

  BloomFilter _bloom_filter;
  std::optional<size_t> _radix_bits{};
  mutable std::unique_ptr<BaseBuildStatistics> _build_statistics;
};

}  // namespace hyrise
