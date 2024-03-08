#include "od_validation_rule.hpp"

#include <numeric>
#include <random>

#include "dependency_discovery/validation_strategy/validation_utils.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
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
  // if (row_count < OdValidationRule::MIN_SIZE_FOR_RANDOM_SAMPLE) {
    if (!fill_sample_consecutive(table, ordering_column_id, sample_size, ordering_sample_values) ||
        !fill_sample_consecutive(table, ordered_column_id, sample_size, ordered_sample_values)) {
      return false;
    }
  /*} else {
    auto random_rows = std::unordered_set<size_t>{sample_size};
    auto generator = std::mt19937{1337};
    auto distribution = std::uniform_int_distribution<size_t>{0, row_count - 1};
    while (random_rows.size() < sample_size) {
      random_rows.emplace(distribution(generator));
    }

    auto performance_warning_disabler = PerformanceWarningDisabler{};
    for (const auto random_row : random_rows) {
    // for (auto random_row = uint64_t{0}; random_row < OdValidationRule::MIN_SIZE_FOR_RANDOM_SAMPLE; ++random_row) {
      const auto& random_ordering_value = table->get_value<OrderingType>(ordering_column_id, random_row);
      const auto& random_ordered_value = table->get_value<OrderedType>(ordered_column_id, random_row);
      if (!random_ordering_value || !random_ordered_value) {
        return false;
      }
      ordering_sample_values.emplace_back(*random_ordering_value);
      ordered_sample_values.emplace_back(*random_ordered_value);
    }
  }*/

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

template <typename T>
bool check_column_sortedness(const std::shared_ptr<const Table> table, const ColumnID column_id,
                             const ChunkID chunk_count) {
  auto ordered = true;
  auto is_initialized = false;
  auto last_value = T{};

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    if (!ordered) {
      return false;
    }

    const auto& chunk = table->get_chunk(chunk_id);
    Assert(chunk, "Physically deleted chunks shouldn't reach this point.");

    segment_with_iterators<T>(*chunk->get_segment(column_id), [&](auto it, const auto end) {
      while (it != end) {
        if (it->is_null()) {
          ordered = false;
          return;
        }

        auto value = it->value();
        if (is_initialized && value < last_value) {
          ordered = false;
          return;
        }

        is_initialized = true;
        ++it;
        last_value = std::move(value);
      }
    });
  }

  return ordered;
}

template <typename LhsType, typename RhsType>
ValidationStatus check_two_column_sortedness(const AbstractSegment& lhs_segment, const AbstractSegment& rhs_segment) {
  auto status = ValidationStatus::Valid;
  auto is_initialized = false;
  auto lhs_last_value = LhsType{};
  auto rhs_last_value = RhsType{};

  segment_with_iterators<LhsType>(lhs_segment, [&](auto lhs_it, const auto& lhs_end) {
    segment_with_iterators<RhsType, EraseTypes::Always>(rhs_segment, [&](auto rhs_it, const auto& rhs_end) {
      while (lhs_it != lhs_end) {
        if (lhs_it->is_null() || rhs_it->is_null()) {
          status = ValidationStatus::Invalid;
          return;
        }

        // We have to sort if the LHS is not sorted ascending.
        auto lhs_value = lhs_it->value();
        if (is_initialized && lhs_value < lhs_last_value) {
          status = ValidationStatus::Uncertain;
          return;
        }

        // If LHS is ascending, but RHS is not, the OD is invalid.
        auto rhs_value = rhs_it->value();
        if (is_initialized && rhs_value < rhs_last_value) {
          status = ValidationStatus::Invalid;
          return;
        }

        is_initialized = true;
        ++lhs_it;
        ++rhs_it;
        lhs_last_value = std::move(lhs_value);
        rhs_last_value = std::move(rhs_value);
      }
      // We did not return and LHS segment is traversed. Ensure RHS segment is also traversed.
      Assert(rhs_it == rhs_end, "Segment sizes differ.");
    });
  });

  return status;
}

template <typename T>
bool add_to_index(const ChunkID chunk_id, const std::shared_ptr<const Chunk>& chunk, const ColumnID column_id,
                  std::map<T, SegmentDomainInfo>& index,
                  typename std::map<T, SegmentDomainInfo>::iterator& previous_it) {
  const auto segment_statistics = ValidationUtils<T>::gather_segment_statistics(chunk, column_id);
  const auto& min = *segment_statistics.min;
  const auto& max = *segment_statistics.max;

  const auto segment_is_constant = min == max;
  if (!segment_is_constant) {
    const auto min_it = index.emplace_hint(previous_it, min, std::make_pair(chunk_id, SegmentDomainBound::Min));
    const auto max_it = index.emplace_hint(min_it, max, std::make_pair(chunk_id, SegmentDomainBound::Max));
    previous_it = max_it;

    if (min_it != index.begin()) {
      const auto prev_it = std::prev(min_it);
      const auto next_it = std::next(max_it);
      // domain is completely inside domain of other segment
      if (next_it != index.end() && prev_it->second.first == next_it->second.first) {
        return false;
      }
    }

    const auto max_duplicate = max_it->second.first != chunk_id;
    const auto max_continues = max_duplicate && max_it->second.second == SegmentDomainBound::Min;

    // Make sure there is always an index entry for segment at max position.
    if (max_continues) {
      max_it->second = std::make_pair(chunk_id, SegmentDomainBound::Max);
    }

    // min/max are duplicates. domain overlaps with other segment if it does not perfectly continue its domain.
    if ((min_it->second.first != chunk_id && min_it->second.second != SegmentDomainBound::Max) ||
        (max_duplicate && !max_continues)) {
      return false;
    }

    // entries between min and max, domain overlaps with other segment
    if (max_it != std::next(min_it)) {
      return false;
    }

    return true;
  }

  return false;
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

      // Check ordering for sample.
      const auto ordered = sample_ordered<OrderingColumnDataType, OrderedColumnDataType>(table, ordering_column_id,
                                                                                         ordered_column_id, row_count);
      if (!ordered) {
        status = ValidationStatus::Invalid;
        return;
      }

      // If sample covers entire table, orderd sample means column ordered.
      if (row_count <= SAMPLE_SIZE) {
        status = ValidationStatus::Valid;
        return;
      }

      status = ValidationStatus::Valid;
      const auto chunk_count = table->chunk_count();

      if (chunk_count == 1) {
        const auto& chunk = table->get_chunk(ChunkID{0});
        Assert(chunk, "Did not expect empty table.");
        const auto& ordering_segment = chunk->get_segment(ordering_column_id);
        const auto& ordered_segment = chunk->get_segment(ordered_column_id);

        // We do not need to sort if the LHS segment is already sorted.
        const auto chunk_status = check_two_column_sortedness<OrderingColumnDataType, OrderedColumnDataType>(
            *ordering_segment, *ordered_segment);
        if (chunk_status == ValidationStatus::Invalid) {
          status = ValidationStatus::Invalid;
          return;
        } else if (chunk_status == ValidationStatus::Valid) {
          status = ValidationStatus::Valid;
          return;
        }
      } else {
        // For tables with more chunks, check if we can sort and check chunks individually for result:
        //   - For each column, domains of segments do not overlap.
        //   - Order of segments is the same for both columns.
        auto sort_column_min_max_ordered = std::map<OrderingColumnDataType, SegmentDomainInfo>{};
        auto check_column_min_max_ordered = std::map<OrderedColumnDataType, SegmentDomainInfo>{};

        // Add all segments of both columns to indexes, abort if there are overlaps.
        auto sort_column_it = sort_column_min_max_ordered.begin();
        auto check_column_it = check_column_min_max_ordered.begin();
        auto both_columns_disjoint = true;

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto& chunk = table->get_chunk(chunk_id);
          if (!chunk || chunk->size() == 0) {
            continue;
          }

          if (!(add_to_index(chunk_id, chunk, ordering_column_id, sort_column_min_max_ordered, sort_column_it) &&
                add_to_index(chunk_id, chunk, ordered_column_id, check_column_min_max_ordered, check_column_it))) {
            both_columns_disjoint = false;
            break;
          }
        }

        if (both_columns_disjoint) {
          // Check that order of segments is the same. If not, OD cannot hold.
          sort_column_it = sort_column_min_max_ordered.begin();
          check_column_it = check_column_min_max_ordered.begin();

          auto left_chunk_id = sort_column_it->second.first;
          auto right_chunk_id = check_column_it->second.first;
          auto abort = false;

          while (sort_column_it != sort_column_min_max_ordered.end() &&
                 check_column_it != check_column_min_max_ordered.end()) {
            if (left_chunk_id != right_chunk_id) {
              abort = true;
              break;
            }

            while (sort_column_it != sort_column_min_max_ordered.end() &&
                   sort_column_it->second.first == left_chunk_id) {
              ++sort_column_it;
            }

            if (sort_column_it != sort_column_min_max_ordered.end()) {
              left_chunk_id = sort_column_it->second.first;
            }
            while (check_column_it != check_column_min_max_ordered.end() &&
                   check_column_it->second.first == right_chunk_id) {
              ++check_column_it;
            }
            if (check_column_it != check_column_min_max_ordered.end()) {
              right_chunk_id = check_column_it->second.first;
            }

            // Break if sort column segment perfectly continues other segment, but check column segment does not. That
            // means there are different values in LHS for same in RHS, which breaks OD.
            if (sort_column_it->second.second == SegmentDomainBound::Max &&
                check_column_it->second.second != SegmentDomainBound::Max) {
              abort = true;
              break;
            }
          }

          if (!abort) {
            Assert(check_column_it == check_column_min_max_ordered.end() ||
                       std::next(check_column_it) == check_column_min_max_ordered.end(),
                   "Both indexes should have same size!");

            // Sort chunks individually, abort if any chunk not ordered.
            const auto& original_column_definitions = table->column_definitions();
            const auto column_definitions = std::vector{original_column_definitions[ordering_column_id],
                                                        original_column_definitions[ordered_column_id]};
            const auto sort_definitions = std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}}};

            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto& chunk = table->get_chunk(chunk_id);
              if (!chunk || chunk->size() == 0) {
                continue;
              }

              const auto& ordering_segment = chunk->get_segment(ordering_column_id);
              const auto& ordered_segment = chunk->get_segment(ordered_column_id);

              // We do not need to sort if the LHS segment is already sorted.
              const auto chunk_status = check_two_column_sortedness<OrderingColumnDataType, OrderedColumnDataType>(
                  *ordering_segment, *ordered_segment);
              if (chunk_status == ValidationStatus::Invalid) {
                status = ValidationStatus::Invalid;
                return;
              } else if (chunk_status == ValidationStatus::Valid) {
                continue;
              }

              auto segments_to_sort = Segments{ordering_segment, ordered_segment};
              auto chunk_to_sort = std::make_shared<Chunk>(std::move(segments_to_sort));
              auto chunks_to_sort = std::vector<std::shared_ptr<Chunk>>{std::move(chunk_to_sort)};

              const auto table_to_sort =
                  std::make_shared<Table>(column_definitions, TableType::Data, std::move(chunks_to_sort));
              const auto table_wrapper = std::make_shared<TableWrapper>(table_to_sort);
              const auto sort = std::make_shared<Sort>(table_wrapper, sort_definitions);
              table_wrapper->execute();
              sort->execute();
              const auto& result_table = sort->get_output();

              if (!check_column_sortedness<OrderedColumnDataType>(result_table, ColumnID{1}, ChunkID{1})) {
                status = ValidationStatus::Invalid;
                return;
              }
            }
            status = ValidationStatus::Valid;
            return;
          }
        }
      }

      // Fallback: Sort entire relation.
      auto pruned_column_ids = std::vector<ColumnID>{};
      const auto column_count = table->column_count();
      if (column_count > 2) {
        pruned_column_ids.reserve(column_count - 2);

        for (auto pruned_column_id = ColumnID{0}; pruned_column_id < column_count; ++pruned_column_id) {
          if (pruned_column_id != ordering_column_id && pruned_column_id != ordered_column_id) {
            pruned_column_ids.emplace_back(pruned_column_id);
          }
        }
      }

      const auto ordered_column_last = ordered_column_id > ordering_column_id;
      const auto sort_column_id = ordered_column_last ? ColumnID{0} : ColumnID{1};
      const auto check_column_id = ordered_column_last ? ColumnID{1} : ColumnID{0};
      const auto sort_definitions = std::vector<SortColumnDefinition>{SortColumnDefinition{sort_column_id}};
      const auto get_table =
          std::make_shared<GetTable>(od_candidate.table_name, std::vector<ChunkID>{}, pruned_column_ids);
      const auto sort = std::make_shared<Sort>(get_table, sort_definitions);
      get_table->execute();
      sort->execute();
      const auto& result_table = sort->get_output();

      if (!check_column_sortedness<OrderedColumnDataType>(result_table, check_column_id, result_table->chunk_count())) {
        status = ValidationStatus::Invalid;
      }
    });
  });

  auto result = ValidationResult(status);
  if (status == ValidationStatus::Valid) {
    result.constraints[table] = _constraint_from_candidate(candidate);
  }

  return result;
}

}  // namespace hyrise
