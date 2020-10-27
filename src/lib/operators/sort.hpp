#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Operator to sort a table by one or multiple columns. This implements a stable sort, i.e., rows that share the same
 * value will maintain their relative order.
 * By passing multiple sort column definitions it is possible to sort multiple columns with one operator run.
 */
class Sort : public AbstractReadOnlyOperator {
 public:
  enum class ForceMaterialization : bool { Yes = true, No = false };

  enum class OperatorSteps : uint8_t { Sort, WriteOutput };

  Sort(const std::shared_ptr<const AbstractOperator>& in, const std::vector<SortColumnDefinition>& sort_definitions,
       const ChunkOffset output_chunk_size = Chunk::DEFAULT_SIZE,
       const ForceMaterialization force_materialization = ForceMaterialization::No);

  const std::vector<SortColumnDefinition>& sort_definitions() const;

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  template <typename SortColumnType>
  class SortImpl;

  template <typename SortColumnType>
  class SortImplMaterializeOutput;

  const std::vector<SortColumnDefinition> _sort_definitions;
  const ChunkOffset _output_chunk_size;
  const ForceMaterialization _force_materialization;
};

}  // namespace opossum
