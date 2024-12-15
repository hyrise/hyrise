#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/progressive/chunk_sink.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "types.hpp"

namespace hyrise {

class Shuffle : public AbstractReadOnlyOperator {
 public:
  Shuffle(const std::shared_ptr<const AbstractOperator>& input_operator, std::vector<ColumnID>&& column_ids,
          std::vector<uint8_t>&& partition_counts);
  const std::string& name() const override;
  const std::vector<ColumnID>& column_ids() const;
  const std::vector<uint8_t>& partition_counts() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  std::vector<ColumnID> _column_ids{};
  std::vector<uint8_t> _partition_counts{};
};

}  // namespace hyrise
