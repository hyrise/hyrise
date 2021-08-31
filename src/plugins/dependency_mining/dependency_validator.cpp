#include "dependency_validator.hpp"

#include <numeric>
#include <random>

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>

#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/segment_iterate.hpp"
#include "ucc_validator.hpp"
#include "utils/timer.hpp"

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

DependencyValidator::DependencyValidator(
    const std::shared_ptr<DependencyCandidateQueue>& queue,
    tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes, size_t id)
    : _queue(queue), _table_constraint_mutexes(table_constraint_mutexes), _id(id) {}

void DependencyValidator::start() {
  _running = true;
  std::cout << "Run DependencyValidator " + std::to_string(_id) + "\n";
  Timer timer;
  DependencyCandidate candidate;
  while (_queue->try_pop(candidate)) {
    Timer candidate_timer;
    std::stringstream my_out;
    my_out << "[" << _id << "] Check candidate: ";
    // std::cout << "Check candidate: ";
    candidate.output_to_stream(my_out, DescriptionMode::MultiLine);
    my_out << std::endl;
    switch (candidate.type) {
      case DependencyType::Order:
        _validate_od(candidate, my_out);
        break;
      case DependencyType::Functional:
        _validate_fd(candidate, my_out);
        break;
      case DependencyType::Unique:
        _validate_ucc(candidate, my_out);
        break;
      case DependencyType::Inclusion:
        _validate_ind(candidate, my_out);
        break;
    }
    my_out << "    " << candidate_timer.lap_formatted() << std::endl;
    std::cout << my_out.rdbuf();
  }
  std::cout << "DependencyValidator " + std::to_string(_id) + " finished in " + timer.lap_formatted() + "\n";
}

void DependencyValidator::stop() { _running = false; }

bool DependencyValidator::_validate_od(const DependencyCandidate& candidate, std::ostream& out) {
  Assert(candidate.type == DependencyType::Order, "Expected OD");
  Assert(!candidate.determinants.empty() && !candidate.dependents.empty(), "Did not expect useless OD");
  std::unordered_set<std::string> table_names;
  for (const auto& determinant : candidate.determinants) {
    table_names.emplace(determinant.table_name);
  }
  if (table_names.size() > 1) {
    out << "    SKIP: Cannot resolve OD between multiple tables" << std::endl;
    return false;
  }
  if (candidate.dependents.size() > 1) {
    out << "    SKIP: Cannot resolve OD with multiple dependents" << std::endl;
    return false;
  }
  if (candidate.determinants.size() > 1) {
    out << "    SKIP: Cannot resolve OD with multiple determinants" << std::endl;
    return false;
  }
  const auto table_name = *table_names.begin();
  const auto table = Hyrise::get().storage_manager.get_table(table_name);
  const auto num_random_rows = size_t{100};

  if (candidate.determinants.size() == 1 && candidate.dependents.size() == 1 &&
      size_t{table->row_count()} > num_random_rows) {
    const auto determinant_column_id = candidate.determinants[0].column_id;
    const auto dependent_column_id = candidate.dependents[0].column_id;

    const uint64_t range_from{0};
    const uint64_t range_to{table->row_count() - 1};
    std::mt19937 generator(1337);
    std::uniform_int_distribution<uint64_t> distr(range_from, range_to);
    std::unordered_set<size_t> random_rows;
    while (random_rows.size() < num_random_rows) {
      random_rows.emplace(size_t{distr(generator)});
    }
    bool invalid = false;
    resolve_data_type(table->column_definitions().at(determinant_column_id).data_type, [&](auto determinant_type) {
      using DeterminantDataType = typename decltype(determinant_type)::type;
      resolve_data_type(table->column_definitions().at(dependent_column_id).data_type, [&](auto dependent_type) {
        using DependentDataType = typename decltype(dependent_type)::type;
        std::vector<DependentDataType> random_dependents;
        std::vector<DeterminantDataType> random_determinants;
        random_determinants.reserve(num_random_rows);
        random_dependents.reserve(num_random_rows);
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
      out << "    INVALID (shortcut sample)" << std::endl;
      return false;
    }

    /*std::vector<ColumnID> determinant_pruned_columns;
    std::vector<ColumnID> dependent_pruned_columns;
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      if (column_id != determinant_column_id) {
        determinant_pruned_columns.emplace_back(column_id);
      }
      if (column_id != dependent_column_id) {
        dependent_pruned_columns.emplace_back(column_id);
      }
    }
    const auto get_table_determinants =
        std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, determinant_pruned_columns);
    get_table_determinants->never_clear_output();
    get_table_determinants->execute();
    const auto get_table_dependents =
        std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, dependent_pruned_columns);
    get_table_dependents->never_clear_output();
    get_table_dependents->execute();
    const auto determinants_aggregate =
        std::make_shared<AggregateHash>(get_table_determinants, std::vector<std::shared_ptr<AggregateExpression>>{},
                                        std::vector<ColumnID>{ColumnID{0}});
    determinants_aggregate->never_clear_output();
    const auto dependents_aggregate = std::make_shared<AggregateHash>(
        get_table_dependents, std::vector<std::shared_ptr<AggregateExpression>>{}, std::vector<ColumnID>{ColumnID{0}});
    dependents_aggregate->never_clear_output();
    determinants_aggregate->execute();
    dependents_aggregate->execute();
    if (determinants_aggregate->get_output()->row_count() < dependents_aggregate->get_output()->row_count()) {
      out << "    INVALID (shortcut column sizes)" << std::endl;
      return false;
    }*/
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
  /*std::vector<ChunkID> pruned_chunk_ids;
  pruned_chunk_ids.reserve(table->chunk_count() - 1);
  for (auto chunk_id = ChunkID{1}; chunk_id < table->chunk_count(); ++chunk_id) {
    pruned_chunk_ids.emplace_back(chunk_id);
  }
  auto get_table = std::make_shared<GetTable>(table_name, pruned_chunk_ids, pruned_column_ids);*/
  const auto sort_column_id = det_column_id < dep_column_id ? ColumnID{0} : ColumnID{1};
  const auto target_column_id = det_column_id < dep_column_id ? ColumnID{1} : ColumnID{0};
  std::vector<SortColumnDefinition> sort_columns;
  sort_columns.emplace_back(sort_column_id, SortMode::Ascending);
  /*auto sort_operator = std::make_shared<Sort>(get_table, sort_columns);
  get_table->execute();
  sort_operator->execute();

  if (!column_sorted(sort_operator->get_output(), target_column_id)) {
    out << "    INVALID (shortcut sample)" << std::endl;
      return false;
  }*/

  const auto get_table = std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, pruned_column_ids);
  const auto sort_operator = std::make_shared<Sort>(get_table, sort_columns);
  get_table->execute();
  sort_operator->execute();

  if (column_sorted(sort_operator->get_output(), target_column_id)) {
    out << "    VALID" << std::endl;
    const auto order_constraint =
        TableOrderConstraint{{candidate.determinants[0].column_id}, {candidate.dependents[0].column_id}};
    {
      auto mutex_iter = _table_constraint_mutexes.find(table_name);
      if (mutex_iter == _table_constraint_mutexes.end()) {
        const auto mutex = std::make_shared<std::mutex>();
        _table_constraint_mutexes[table_name] = std::move(mutex);
        mutex_iter = _table_constraint_mutexes.find(table_name);
      }
      std::lock_guard<std::mutex> lock(*mutex_iter->second);
      const auto& current_constraints = table->soft_order_constraints();
      bool is_new = true;
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint == order_constraint) {
          is_new = false;
          break;
        }
      }
      if (is_new) {
        table->add_soft_order_constraint(order_constraint);
      }
    }
    return true;
  } else {
    out << "    INVALID" << std::endl;
    return false;
  }
}
bool DependencyValidator::_validate_fd(const DependencyCandidate& candidate, std::ostream& out) {
  Assert(candidate.type == DependencyType::Functional, "Expected FD");
  Assert(!candidate.determinants.empty(), "Did not expect useless FD");
  Assert(candidate.dependents.empty(), "Invalid dependents for FD");
  bool has_ucc = false;
  for (const auto& determinant : candidate.determinants) {
    out << "  try UCC " << determinant.description() << std::endl;
    has_ucc |= _validate_ucc({TableColumnIDs{determinant}, {}, DependencyType::Unique, 0}, out);
  }
  if (has_ucc) return true;

  out << "  (need further validation - not implemented)" << std::endl;
  return false;
}
bool DependencyValidator::_validate_ucc(const DependencyCandidate& candidate, std::ostream& out) {
  Assert(candidate.type == DependencyType::Unique, "Expected UCC");
  Assert(!candidate.determinants.empty(), "Did not expect useless UCC");
  Assert(candidate.dependents.empty(), "Invalid dependents for UCC");
  std::unordered_set<std::string> table_names;

  for (const auto& determinant : candidate.determinants) {
    table_names.emplace(determinant.table_name);
  }
  if (table_names.size() > 1) {
    out << "    SKIP: Cannot resolve UCC between multiple tables" << std::endl;
    return false;
  }
  const auto table_name = *table_names.begin();
  const auto table = Hyrise::get().storage_manager.get_table(table_name);

  std::unordered_set<ColumnID> column_ids;
  for (const auto& determinant : candidate.determinants) {
    column_ids.emplace(determinant.column_id);
  }
  const auto unique_constraint = TableKeyConstraint{column_ids, KeyConstraintType::UNIQUE};
  {
    auto mutex_iter = _table_constraint_mutexes.find(table_name);
    if (mutex_iter == _table_constraint_mutexes.end()) {
      const auto mutex = std::make_shared<std::mutex>();
      _table_constraint_mutexes[table_name] = std::move(mutex);
      mutex_iter = _table_constraint_mutexes.find(table_name);
    }
    std::lock_guard<std::mutex> lock(*mutex_iter->second);
    const auto& current_constraints = table->soft_key_constraints();
    for (const auto& current_constraint : current_constraints) {
      if (current_constraint.columns() == unique_constraint.columns()) {
        out << "    VALID: already known" << std::endl;
        return true;
      }
    }
  }

  const auto add_ucc = [&]() {
    {
      auto mutex_iter = _table_constraint_mutexes.find(table_name);
      if (mutex_iter == _table_constraint_mutexes.end()) {
        const auto mutex = std::make_shared<std::mutex>();
        _table_constraint_mutexes[table_name] = std::move(mutex);
        mutex_iter = _table_constraint_mutexes.find(table_name);
      }
      std::lock_guard<std::mutex> lock(*mutex_iter->second);
      const auto& current_constraints = table->soft_key_constraints();
      bool is_new = true;
      for (const auto& current_constraint : current_constraints) {
        if (current_constraint.columns() == unique_constraint.columns()) {
          is_new = false;
          break;
        }
      }
      if (is_new) {
        table->add_soft_key_constraint(unique_constraint);
      }
    }
  };

  Timer timer;
  if (candidate.determinants.size() == 1) {
    Assert(table->type() == TableType::Data, "Expected Data table");
    const auto column_id = candidate.determinants[0].column_id;
    const auto num_chunks = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }
      const auto segment = chunk->get_segment(column_id);
      if (const auto dictionary_segment = std::dynamic_pointer_cast<BaseDictionarySegment>(segment)) {
        if (dictionary_segment->unique_values_count() != dictionary_segment->size()) {
          out << "   INVALID " << timer.lap_formatted() << std::endl;
          return false;
        }
      }
    }
  }

  std::unordered_set<ColumnID> candidate_columns;
  std::vector<ColumnID> pruned_columns;
  for (const auto& determinant : candidate.determinants) {
    candidate_columns.emplace(determinant.column_id);
  }
  for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
    if (!candidate_columns.count(column_id)) {
      pruned_columns.emplace_back(column_id);
    }
  }

  const auto get_table = std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, pruned_columns);
  get_table->never_clear_output();
  get_table->execute();
  auto ucc_validator = UCCValidator(get_table->get_output());
  if (ucc_validator.is_unique()) {
    out << "    VALID " << timer.lap_formatted() << std::endl;
    add_ucc();
    return true;
  } else {
    out << "    INVALID " << timer.lap_formatted() << std::endl;
    return false;
  }
}

// semantics: dependent INCLUDED IN determinant
bool DependencyValidator::_validate_ind(const DependencyCandidate& candidate, std::ostream& out) {
  Assert(candidate.type == DependencyType::Inclusion, "Expected IND");
  Assert(candidate.determinants.size() == 1, "Invalid determinats for IND");
  Assert(candidate.dependents.size() == 1, "Invalid dependents for IND");

  const auto determinant = candidate.determinants[0];
  const auto dependent = candidate.dependents[0];
  auto det_column_type =
      Hyrise::get().storage_manager.get_table(determinant.table_name)->column_data_type(determinant.column_id);
  if (det_column_type == DataType::Double) {
    det_column_type = DataType::Float;
  } else if (det_column_type == DataType::Long) {
    det_column_type = DataType::Int;
  }
  auto dep_column_type =
      Hyrise::get().storage_manager.get_table(dependent.table_name)->column_data_type(dependent.column_id);
  if (dep_column_type == DataType::Double) {
    dep_column_type = DataType::Float;
  } else if (dep_column_type == DataType::Long) {
    dep_column_type = DataType::Int;
  }

  if (dep_column_type != det_column_type) {
    out << "    INVALID" << std::endl;
    return false;
  }

  const auto [det_status, det_result] =
      SQLPipelineBuilder{"SELECT DISTINCT " + determinant.column_name() + " placeholder_name FROM " +
                         determinant.table_name + " ORDER BY " + determinant.column_name()}
          .create_pipeline()
          .get_result_table();
  if (det_status != SQLPipelineStatus::Success) {
    out << "    FAILED" << std::endl;
    return false;
  }

  const auto [dep_status, dep_result] =
      SQLPipelineBuilder{"SELECT DISTINCT " + dependent.column_name() + " placeholder_name FROM " +
                         dependent.table_name + " ORDER BY " + dependent.column_name()}
          .create_pipeline()
          .get_result_table();
  if (dep_status != SQLPipelineStatus::Success) {
    out << "    FAILED" << std::endl;
    return false;
  }

  if (dep_result->row_count() > det_result->row_count()) {
    out << "    INVALID" << std::endl;
    return false;
  }

  const auto dep_rows = dep_result->get_rows();
  const auto det_rows = det_result->get_rows();

  auto dep_iter = dep_rows.begin();
  auto det_iter = det_rows.begin();

  while (dep_iter != dep_rows.end()) {
    if (*dep_iter != *dep_iter) {
      out << "    INVALID" << std::endl;
      return false;
    }
    ++dep_iter;
    ++det_iter;
  }

  if (dep_rows.size() == det_rows.size()) {
    out << "    VALID (bidirectional)" << std::endl;
    return true;
  }

  out << "    VALID" << std::endl;
  return true;
}

}  // namespace opossum
