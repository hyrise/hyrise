#pragma once

#include <vector>

#include "operators/abstract_read_only_operator.hpp"
#include "operators/sort.hpp"

namespace hyrise {

class RowSort : public AbstractReadOnlyOperator {
 public:
  enum class ForceMaterialization : bool { Yes = true, No = false };

  enum class OperatorSteps : uint8_t { MaterializeSortColumns, Sort, TemporaryResultWriting, WriteOutput };

  RowSort(const std::shared_ptr<const AbstractOperator>& input_operator,
          const std::vector<SortColumnDefinition>& sort_definitions,
          const ChunkOffset output_chunk_size = Chunk::DEFAULT_SIZE,
          const Sort::ForceMaterialization force_materialization = Sort::ForceMaterialization::No);

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  const std::vector<SortColumnDefinition>& sort_definitions() const;

  const std::string& name() const override;

 protected:
  struct SortEntry {
    std::vector<unsigned char> sort_key;
    RowID original_row_id;
  };

  std::shared_ptr<const Table> _on_execute() override;

  const std::vector<SortColumnDefinition> _sort_definitions;
  const ChunkOffset _output_chunk_size;
  const Sort::ForceMaterialization _force_materialization;
};

}  // namespace hyrise