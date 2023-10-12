#include "od_validation_rule.hpp"

#include <numeric>
#include <random>

#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/sort.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

OdValidationRule::OdValidationRule() : AbstractDependencyValidationRule{DependencyType::Order} {}

ValidationResult OdValidationRule::_on_validate(const AbstractDependencyCandidate& candidate) const {
  const auto& od_candidate = static_cast<const OdCandidate&>(candidate);

  const auto& table = Hyrise::get().storage_manager.get_table(od_candidate.table_name);
  const auto ordering_column_id = od_candidate.ordering_column_id;
  const auto ordered_column_id = od_candidate.ordered_column_id;

  auto status = ValidationStatus::Uncertain;

  auto pruned_column_ids = std::vector<ColumnID>{};
  const auto column_count = table->column_count();
  pruned_column_ids.reserve(std::max(column_count - 2, 0));
  for (auto pruned_column_id = ColumnID{0}; pruned_column_id < column_count; ++pruned_column_id) {
    if (pruned_column_id != ordering_column_id && pruned_column_id != ordered_column_id) {
      pruned_column_ids.emplace_back(pruned_column_id);
    }
  }

  const auto ordered_column_last = ordered_column_id > ordering_column_id;
  const auto sort_column_id = ordered_column_last ? ColumnID{0} : ColumnID{1};
  const auto check_column_id = ordered_column_last ? ColumnID{1} : ColumnID{0};

  const auto get_table = std::make_shared<GetTable>(od_candidate.table_name, std::vector<ChunkID>{}, pruned_column_ids);
  const auto sort_definitions = std::vector<SortColumnDefinition>{SortColumnDefinition{sort_column_id}};
  const auto sort = std::make_shared<Sort>(get_table, sort_definitions);
  get_table->execute();
  sort->execute();
  const auto& result_table = sort->get_output();

  resolve_data_type(result_table->column_data_type(check_column_id), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    auto is_initialized = false;
    auto last_value = ColumnDataType{};

    const auto chunk_count = result_table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      if (status == ValidationStatus::Invalid) {
        return;
      }

      const auto& chunk = result_table->get_chunk(chunk_id);
      Assert(chunk, "Physically deleted chunks shouldn't reach this point.");

      segment_with_iterators<ColumnDataType>(*chunk->get_segment(check_column_id), [&](auto it, const auto end) {
        while (it != end) {
          if (it->is_null()) {
            status = ValidationStatus::Invalid;
            return;
          }

          const auto& value = it->value();
          if (is_initialized && value < last_value) {
            status = ValidationStatus::Invalid;
            return;
          }

          is_initialized = true;
          ++it;
          last_value = value;
        }
      });
    }

    status = ValidationStatus::Valid;
  });

  auto result = ValidationResult(status);
  if (status == ValidationStatus::Valid) {
    result.constraints[table] = _constraint_from_candidate(candidate);
  }

  return result;
}

}  // namespace hyrise
