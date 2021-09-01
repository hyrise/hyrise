#include "od_validation_rule.hpp"

#include <numeric>
#include <random>

#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/sort.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"

namespace {

template <typename T, typename Compare>
std::vector<std::size_t> sort_permutation(const std::vector<T>& vec, Compare& compare) {
  std::vector<std::size_t> p(vec.size());
  std::iota(p.begin(), p.end(), 0);
  std::sort(p.begin(), p.end(), [&](std::size_t i, std::size_t j) { return compare(vec[i], vec[j]); });
  return p;
}

template <typename T>
void apply_permutation_in_place(std::vector<T>& vec, const std::vector<std::size_t>& p) {
  std::vector<bool> done(vec.size());
  for (std::size_t i = 0; i < vec.size(); ++i) {
    if (done[i]) {
      continue;
    }
    done[i] = true;
    std::size_t prev_j = i;
    std::size_t j = p[i];
    while (i != j) {
      std::swap(vec[prev_j], vec[j]);
      done[j] = true;
      prev_j = j;
      j = p[j];
    }
  }
}

}  // namespace

namespace opossum {

ODValidationRule::ODValidationRule() : AbstractDependencyValidationRule(DependencyType::Order) {}

std::shared_ptr<ValidationResult> ODValidationRule::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.dependents.size() == 1, "Currently, OD Validation supports only one dependent column.");

  const auto table_name = candidate.determinants[0].table_name;
  const auto table = Hyrise::get().storage_manager.get_table(table_name);

  // validate on sample to avoid full sorting
  if (candidate.determinants.size() == 1 && table->row_count() > SAMPLE_ROW_COUNT) {
    const auto determinant_column_id = candidate.determinants[0].column_id;
    const auto dependent_column_id = candidate.dependents[0].column_id;

    const uint64_t range_from{0};
    const uint64_t range_to{table->row_count() - 1};
    std::mt19937 generator(1337);
    std::uniform_int_distribution<uint64_t> distr(range_from, range_to);
    std::unordered_set<size_t> random_rows;
    while (random_rows.size() < SAMPLE_ROW_COUNT) {
      random_rows.emplace(size_t{distr(generator)});
    }
    bool invalid = false;
    resolve_data_type(table->column_definitions().at(determinant_column_id).data_type, [&](auto determinant_type) {
      using DeterminantDataType = typename decltype(determinant_type)::type;
      resolve_data_type(table->column_definitions().at(dependent_column_id).data_type, [&](auto dependent_type) {
        using DependentDataType = typename decltype(dependent_type)::type;
        std::vector<DependentDataType> random_dependents;
        std::vector<DeterminantDataType> random_determinants;
        random_determinants.reserve(SAMPLE_ROW_COUNT);
        random_dependents.reserve(SAMPLE_ROW_COUNT);
        for (const auto& random_row : random_rows) {
          random_determinants.emplace_back(*table->get_value<DeterminantDataType>(determinant_column_id, random_row));
          random_dependents.emplace_back(*table->get_value<DependentDataType>(dependent_column_id, random_row));
        }
        const auto compare = [](const DeterminantDataType& a, const DeterminantDataType& b) { return a < b; };
        auto p = sort_permutation(random_determinants, compare);
        apply_permutation_in_place(random_dependents, p);
        bool is_init = false;
        DependentDataType last_value{};
        for (const auto& current_value : random_dependents) {
          if (is_init) {
            invalid = last_value > current_value;
          } else {
            is_init = true;
          }
          if (invalid) {
            break;
          }
          last_value = current_value;
        }
      });
    });
    if (invalid) {
      return INVALID_VALIDATION_RESULT;
    }
  }

  const auto column_sorted = [](const auto& sorted_table, const auto column_id) {
    const auto column_type = sorted_table->column_definitions().at(column_id).data_type;
    bool is_valid = true;
    resolve_data_type(column_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto num_chunks = sorted_table->chunk_count();
      ColumnDataType last_value{};
      bool is_init = false;
      for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; ++chunk_id) {
        if (!is_valid) {
          return;
        }
        const auto chunk = sorted_table->get_chunk(chunk_id);
        if (!chunk) {
          continue;
        }
        const auto segment = chunk->get_segment(column_id);
        segment_iterate<ColumnDataType>(*segment, [&](const auto& pos) {
          const auto current_value = pos.value();
          if (!is_init) {
            is_init = true;
          } else {
            if (last_value > current_value) {
              is_valid = false;
              return;
            }
          }
          last_value = current_value;
        });
      }
    });
    return is_valid;
  };

  std::vector<ColumnID> pruned_column_ids;
  const auto det_column_id = candidate.determinants[0].column_id;
  const auto dep_column_id = candidate.dependents[0].column_id;
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    if (column_id == det_column_id || column_id == dep_column_id) continue;
    pruned_column_ids.emplace_back(column_id);
  }

  const auto sort_column_id = det_column_id < dep_column_id ? ColumnID{0} : ColumnID{1};
  const auto target_column_id = det_column_id < dep_column_id ? ColumnID{1} : ColumnID{0};
  std::vector<SortColumnDefinition> sort_columns;
  sort_columns.emplace_back(sort_column_id, SortMode::Ascending);
  const auto get_table = std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, pruned_column_ids);
  const auto sort_operator = std::make_shared<Sort>(get_table, sort_columns);
  get_table->execute();
  sort_operator->execute();

  if (column_sorted(sort_operator->get_output(), target_column_id)) {
    auto table_order_constraint =
        std::make_shared<TableOrderConstraint>(std::vector<ColumnID>{candidate.determinants[0].column_id},
                                               std::vector<ColumnID>{candidate.dependents[0].column_id});
    const auto result = std::make_shared<ValidationResult>(DependencyValidationStatus::Valid);
    result->constraints[table_name].emplace_back(table_order_constraint);
    return result;
  }

  return INVALID_VALIDATION_RESULT;
}

}  // namespace opossum
