#pragma once

#include <vector>
#include "operators/abstract_read_only_operator.hpp"
#include "operators/sort.hpp"

namespace hyrise {

class RowSort : public AbstractReadOnlyOperator {
public:
  RowSort(const std::shared_ptr<const AbstractOperator>& input,
          const std::vector<SortColumnDefinition>& sort_definitions,
          const ChunkOffset output_chunk_size = Chunk::DEFAULT_SIZE,
          const Sort::ForceMaterialization force_materialization = Sort::ForceMaterialization::No);

  const std::string& name() const override;

protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

private:

  const std::vector<SortColumnDefinition> _sort_definitions;
  const ChunkOffset _output_chunk_size;
  const Sort::ForceMaterialization _force_materialization;
};

}  // namespace hyrise