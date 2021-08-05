#include "dependency_validator.hpp"

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>

#include "hyrise.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/timer.hpp"

namespace opossum {

// void DependencyValidator::set_queue(const DependencyCandidateQueue& queue) { _queue = queue; };

DependencyValidator::DependencyValidator(const std::shared_ptr<DependencyCandidateQueue>& queue, tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes, size_t id) : _queue(queue), _table_constraint_mutexes(table_constraint_mutexes), _id(id) {}

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
  Assert(!candidate.determinants.empty() && !candidate.dependents.empty(), "Did not expect useless UCC");
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
  const auto table_name = *table_names.begin();
  const auto table = Hyrise::get().storage_manager.get_table(table_name);

  if (candidate.determinants.size() == 1 && candidate.dependents.size() == 1) {
    const auto determinant_column_name = table->column_name(candidate.determinants[0].column_id);
     const auto [det_status, det_result] = SQLPipelineBuilder{"SELECT DISTINCT " + determinant_column_name + " FROM " + table_name}
                                    .create_pipeline()
                                    .get_result_table();

     if (det_status == SQLPipelineStatus::Success) {
      const auto det_value_count = det_result->row_count();
      const auto dependent_column_name = table->column_name(candidate.dependents[0].column_id);
      const auto [dep_status, dep_result] = SQLPipelineBuilder{"SELECT DISTINCT " + dependent_column_name + " FROM " + table_name}
                                    .create_pipeline()
                                    .get_result_table();
      if (dep_status == SQLPipelineStatus::Success && det_value_count < dep_result->row_count()) {
        out << "    INVALID (shortcut)" << std::endl;
        return false;
      }
     }
  }

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  std::vector<SortColumnDefinition> sort_columns;
  for (const auto& determinant : candidate.determinants) {
    sort_columns.emplace_back(determinant.column_id, SortMode::Ascending);
  }

  const auto sort_operator = std::make_shared<Sort>(table_wrapper, sort_columns);
  table_wrapper->execute();
  sort_operator->execute();
  const auto result_table = sort_operator->get_output();

  const auto column_type = result_table->column_definitions().at(candidate.dependents[0].column_id).data_type;
  bool is_valid = true;
  const auto column_id = candidate.dependents[0].column_id;
  resolve_data_type(column_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    const auto num_chunks = result_table->chunk_count();
    ColumnDataType last_value{};
    bool is_init = false;
    for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; ++chunk_id) {
      if (!is_valid) {
        return;
      }
      const auto chunk = result_table->get_chunk(chunk_id);
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
  if (is_valid) {
    out << "    VALID" << std::endl;
    const auto order_constraint = TableOrderConstraint{{candidate.determinants[0].column_id}, {candidate.dependents[0].column_id}};
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
    out << "    SKIP: Cannot resolve UCC between multipe tables" << std::endl;
    return false;
  }
  const auto table_name = *table_names.begin();
  const auto table = Hyrise::get().storage_manager.get_table(table_name);

  const auto add_ucc = [&](){

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
    //out << " optimized path" << std::endl;
    Assert(table->type() == TableType::Data, "Expected Data table");
    const auto column_id = candidate.determinants[0].column_id;
    // 0 ... UCC found, 1 ... not found, -1 ... error
    int optim_status = -1;
    resolve_data_type(table->column_data_type(column_id), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      std::vector<std::shared_ptr<const pmr_vector<ColumnDataType>>> dictionaries;
      const auto num_chunks = table->chunk_count();
      for (auto chunk_id = ChunkID{0}; chunk_id < num_chunks; ++chunk_id) {
        const auto chunk = table->get_chunk(chunk_id);
        if (!chunk) {
          continue;
        }
        const auto segment = chunk->get_segment(column_id);
        if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment)) {
          const auto dictionary = dictionary_segment->dictionary();
          if (dictionary->size() != dictionary_segment->size()) {
            optim_status = 2;
            return;
          }
          dictionaries.emplace_back(dictionary);
        } else {
          optim_status = -1;
          return;
        }
      }
      std::vector<size_t> current_dictionary_positions;
      current_dictionary_positions.reserve(dictionaries.size());
      std::for_each(dictionaries.begin(), dictionaries.end(), [&](const auto _){current_dictionary_positions.emplace_back(0);});
      size_t finished_dictionaries = 0;
      ColumnDataType next_value = ColumnDataType{0};
      while(finished_dictionaries < dictionaries.size()) {
        ColumnDataType current_smallest_value = ColumnDataType{0};
        size_t current_smallest_dictionary = 0;
        bool is_init = false;
        for (size_t dictionary_id = 0; dictionary_id < dictionaries.size(); ++dictionary_id) {
          const auto& dictionary = dictionaries.at(dictionary_id);
          const auto& value_pointer = current_dictionary_positions.at(dictionary_id);
          if (value_pointer == dictionary->size()) {
            continue;
          }
          const ColumnDataType my_next_value = (*dictionary).at(value_pointer);
          if (!is_init) {
            current_smallest_value = my_next_value;
            current_smallest_dictionary = dictionary_id;
            is_init = true;
            continue;
          }
          if (my_next_value == current_smallest_value) {
            optim_status = 1;
            return;
          }
          if (my_next_value < current_smallest_value) {
            current_smallest_value = my_next_value;
            current_smallest_dictionary = dictionary_id;
          }
        }
        if (current_dictionary_positions[current_smallest_dictionary] != 0 && current_smallest_value == next_value) {
          optim_status = 1;
          return;
        }
        next_value = current_smallest_value;
        const auto dictionary = dictionaries[current_smallest_dictionary];
        if (dictionary->size() == current_dictionary_positions[current_smallest_dictionary] + 1) {
          ++finished_dictionaries;
        }
        ++current_dictionary_positions[current_smallest_dictionary];
      }
      optim_status = 0;
    });

    if (optim_status == 0) {
      out << "   VALID? " << timer.lap_formatted() << std::endl;
      add_ucc();
      //return true;
    }
    if (optim_status == 1) {
      out << "   INVALID? " << timer.lap_formatted() << std::endl;
      //return false;
    }
    if (optim_status == 2) {
      out << "   INVALID " << timer.lap_formatted() << std::endl;
      return false;
    }
  }

  const auto table_num_rows = table->row_count();

  std::vector<std::string> column_names;
  for (const auto& determinant : candidate.determinants) {
    column_names.emplace_back(table->column_name(determinant.column_id));
  }
  const auto columns_string = boost::algorithm::join(column_names, ", ");
  // do not use MVCC currently as nothing here is transaction-safe
  const auto [status, result] = SQLPipelineBuilder{"SELECT DISTINCT " + columns_string + " FROM " + table_name}
                                    .create_pipeline()
                                    .get_result_table();
  if (status != SQLPipelineStatus::Success) {
    out << "    FAILED" << std::endl;
    return false;
  }
  const auto unique_num_rows = result->row_count();
  if (table_num_rows == unique_num_rows) {
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
  auto det_column_type = Hyrise::get().storage_manager.get_table(determinant.table_name)->column_data_type(determinant.column_id);
  if (det_column_type == DataType::Double) {
        det_column_type = DataType::Float;
      } else if (det_column_type == DataType::Long) {
        det_column_type = DataType::Int;
    }
  auto dep_column_type = Hyrise::get().storage_manager.get_table(dependent.table_name)->column_data_type(dependent.column_id);
      if (dep_column_type == DataType::Double) {
        dep_column_type = DataType::Float;
      } else if (dep_column_type == DataType::Long) {
        dep_column_type = DataType::Int;
    }

    if (dep_column_type != det_column_type) {
    out << "    INVALID" << std::endl;
    return false;
  }


  const auto [det_status, det_result] = SQLPipelineBuilder{"SELECT DISTINCT " + determinant.column_name() + " placeholder_name FROM " + determinant.table_name + " ORDER BY " + determinant.column_name()}
                                    .create_pipeline()
                                    .get_result_table();
  if (det_status != SQLPipelineStatus::Success) {
    out << "    FAILED" << std::endl;
    return false;
  }

  const auto [dep_status, dep_result] = SQLPipelineBuilder{"SELECT DISTINCT " + dependent.column_name() + " placeholder_name FROM " + dependent.table_name + " ORDER BY " + dependent.column_name()}
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
