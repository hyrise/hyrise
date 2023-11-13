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

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <typename T>
std::vector<size_t> sort_permutation(const std::vector<T>& values) {
  auto permutation = std::vector<size_t>(values.size());
  // Fill permutation with [0, 1, ..., n - 1] and order the permutation by sorting column_ids.
  std::iota(permutation.begin(), permutation.end(), 0);
  std::sort(permutation.begin(), permutation.end(),
            [&](const auto& lhs, const auto& rhs) { return values[lhs] < values[rhs]; });
  return permutation;
}

template <typename T>
std::vector<T> apply_permutation(std::vector<T>& values, const std::vector<size_t>& permutation) {
  auto sorted_values = std::vector<T>(values.size());

  std::transform(permutation.begin(), permutation.end(), sorted_values.begin(),
                 [&](const auto position) { return values[position]; });
  return sorted_values;
}

template <typename T>
bool fill_sample_consecutive(const std::shared_ptr<const Table>& table, const ColumnID column_id,
                             const uint64_t sample_size, std::vector<T>& values) {
  const auto chunk_count = table->chunk_count();
  auto row_count = uint64_t{1};
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    const auto& segment = chunk->get_segment(column_id);

    auto contains_nulls = false;
    segment_with_iterators<T>(*segment, [&](auto it, const auto end) {
      while (it != end) {
        if (it->is_null()) {
          contains_nulls = true;
          return;
        }

        values.emplace_back(it->value());
        if (row_count == sample_size) {
          return;
        }
        ++row_count;
        ++it;
      }
    });

    if (contains_nulls) {
      return false;
    }

    if (row_count == sample_size) {
      break;
    }
  }

  return true;
}

template <typename OrderingType, typename OrderedType>
bool sample_ordered(const std::shared_ptr<const Table>& table, const ColumnID ordering_column_id,
                    const ColumnID ordered_column_id, const uint64_t row_count) {
  const auto sample_size = std::min(row_count, OdValidationRule::SAMPLE_SIZE);

  auto ordering_sample_values = std::vector<OrderingType>{};
  auto ordered_sample_values = std::vector<OrderedType>{};
  ordering_sample_values.reserve(sample_size);
  ordered_sample_values.reserve(sample_size);

  // Just take first SAMPLE_SIZE rows if table is small (and drawing random sample is likely slower).
  if (row_count < OdValidationRule::MIN_SIZE_FOR_RANDOM_SAMPLE) {
    if (!fill_sample_consecutive(table, ordering_column_id, sample_size, ordering_sample_values) ||
        !fill_sample_consecutive(table, ordered_column_id, sample_size, ordered_sample_values)) {
      return false;
    }
  } else {
    auto random_rows = std::unordered_set<size_t>{sample_size};
    auto generator = std::mt19937{1337};
    auto distribution = std::uniform_int_distribution<size_t>{0, row_count - 1};
    while (random_rows.size() < sample_size) {
      random_rows.emplace(distribution(generator));
    }

    auto performance_warning_disabler = PerformanceWarningDisabler{};
    for (const auto random_row : random_rows) {
      const auto& random_ordering_value = table->get_value<OrderingType>(ordering_column_id, random_row);
      const auto& random_ordered_value = table->get_value<OrderedType>(ordered_column_id, random_row);
      if (!random_ordering_value || !random_ordered_value) {
        return false;
      }
      ordering_sample_values.emplace_back(*random_ordering_value);
      ordered_sample_values.emplace_back(*random_ordered_value);
    }
  }

  const auto& permutation = sort_permutation<OrderingType>(ordering_sample_values);
  const auto& ordered_values = apply_permutation<OrderedType>(ordered_sample_values, permutation);
  bool is_initialized = false;
  auto last_value = OrderedType{};
  for (const auto& current_value : ordered_values) {
    if (is_initialized && last_value > current_value) {
      return false;
    }
    is_initialized = true;
    last_value = current_value;
  }
  return true;
}

}  // namespace

namespace hyrise {

OdValidationRule::OdValidationRule() : AbstractDependencyValidationRule{DependencyType::Order} {}

ValidationResult OdValidationRule::_on_validate(const AbstractDependencyCandidate& candidate) const {
  const auto& od_candidate = static_cast<const OdCandidate&>(candidate);

  const auto& table = Hyrise::get().storage_manager.get_table(od_candidate.table_name);
  const auto ordering_column_id = od_candidate.ordering_column_id;
  const auto ordered_column_id = od_candidate.ordered_column_id;

  auto status = ValidationStatus::Uncertain;
  const auto row_count = table->row_count();
  resolve_data_type(table->column_data_type(ordering_column_id), [&](const auto ordering_data_type_t) {
    using OrderingColumnDataType = typename decltype(ordering_data_type_t)::type;
    resolve_data_type(table->column_data_type(ordered_column_id), [&](const auto ordered_data_type_t) {
      using OrderedColumnDataType = typename decltype(ordered_data_type_t)::type;
      const auto ordered = sample_ordered<OrderingColumnDataType, OrderedColumnDataType>(table, ordering_column_id,
                                                                                         ordered_column_id, row_count);
      if (!ordered) {
        status = ValidationStatus::Invalid;
        return;
      }

      if (row_count <= SAMPLE_SIZE) {
        status = ValidationStatus::Valid;
      }
    });
  });

  if (status != ValidationStatus::Uncertain) {
    auto result = ValidationResult{status};
    if (status == ValidationStatus::Valid) {
      result.constraints[table] = _constraint_from_candidate(candidate);
    }
    return result;
  }

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

  status = ValidationStatus::Valid;
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

          auto value = it->value();
          if (is_initialized && value < last_value) {
            status = ValidationStatus::Invalid;
            return;
          }

          is_initialized = true;
          ++it;
          last_value = std::move(value);
        }
      });
    }
  });

  auto result = ValidationResult(status);
  if (status == ValidationStatus::Valid) {
    result.constraints[table] = _constraint_from_candidate(candidate);
  }

  return result;
}

}  // namespace hyrise
