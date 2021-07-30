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

DependencyValidator::DependencyValidator(const std::shared_ptr<DependencyCandidateQueue>& queue) : _queue(queue) {}

void DependencyValidator::start() {
  _running = true;
  std::cout << "Run DependencyValidator" << std::endl;
  Timer timer;
  DependencyCandidate candidate;
  while (_queue->try_pop(candidate)) {
    std::cout << "Check candidate: ";
    candidate.output_to_stream(std::cout, DescriptionMode::MultiLine);
    switch (candidate.type) {
      case DependencyType::Order:
        _validate_od(candidate);
        break;
      case DependencyType::Functional:
        _validate_fd(candidate);
        break;
      case DependencyType::Unique:
        _validate_ucc(candidate);
        break;
      case DependencyType::Inclusion:
        _validate_ind(candidate);
        break;
    }
  }
  std::cout << "DependencyValidator finished in " << timer.lap_formatted() << std::endl;
}

void DependencyValidator::stop() { _running = false; }

void DependencyValidator::_validate_od(const DependencyCandidate& candidate) {
  Assert(candidate.type == DependencyType::Order, "Expected OD");
  Assert(!candidate.determinants.empty() && !candidate.dependents.empty(), "Did not expect useless UCC");
  std::unordered_set<std::string> table_names;
  for (const auto& determinant : candidate.determinants) {
    table_names.emplace(determinant.table_name);
  }
  if (table_names.size() > 1) {
    std::cout << "    SKIP: Cannot resolve OD between multiple tables" << std::endl;
    return;
  }
  if (candidate.dependents.size() > 1) {
    std::cout << "    SKIP: Cannot resolve OD with multiple dependents" << std::endl;
    return;
  }
  const auto table_name = *table_names.begin();
  const auto table = Hyrise::get().storage_manager.get_table(table_name);
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
    std::cout << "    VALID" << std::endl;
  } else {
    std::cout << "    INVALID" << std::endl;
  }
}
void DependencyValidator::_validate_fd(const DependencyCandidate& candidate) {
  Assert(candidate.type == DependencyType::Functional, "Expected FD");
  std::cout << "    (not implemented)" << std::endl;
}
void DependencyValidator::_validate_ucc(const DependencyCandidate& candidate) {
  Assert(candidate.type == DependencyType::Unique, "Expected UCC");
  Assert(!candidate.determinants.empty(), "Did not expect useless UCC");
  Assert(candidate.dependents.empty(), "Invalid dependents for UCC");
  std::unordered_set<std::string> table_names;
  for (const auto& determinant : candidate.determinants) {
    table_names.emplace(determinant.table_name);
  }
  if (table_names.size() > 1) {
    std::cout << "    SKIP: Cannot resolve UCC between multipe tables" << std::endl;
    return;
  }
  const auto table_name = *table_names.begin();
  const auto table = Hyrise::get().storage_manager.get_table(table_name);
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
    std::cout << "    FAILED" << std::endl;
  }
  const auto unique_num_rows = result->row_count();
  if (table_num_rows == unique_num_rows) {
    std::cout << "    VALID" << std::endl;
  } else {
    std::cout << "    INVALID"
              << std::endl;  //  << table_num_rows << " vs. " << unique_num_rows << " rows" << std::endl;
  }
}
void DependencyValidator::_validate_ind(const DependencyCandidate& candidate) {
  Assert(candidate.type == DependencyType::Inclusion, "Expected IND");
  std::cout << "    (not implemented)" << std::endl;
}

}  // namespace opossum
