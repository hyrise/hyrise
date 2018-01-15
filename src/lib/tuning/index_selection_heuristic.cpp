#include <algorithm>
#include <iostream>

#include "index_selection_heuristic.hpp"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"
#include "optimizer/column_statistics.hpp"
#include "optimizer/table_statistics.hpp"

namespace opossum {

IndexSelectionHeuristic::IndexSelectionHeuristic() {}

const std::vector<IndexProposal>& IndexSelectionHeuristic::recommend_changes(const SystemStatistics& statistics) {
  _access_recods.clear();
  _index_proposals.clear();

  // Investigate query cache
  const auto& recent_queries = statistics.recent_queries();

  for (const auto& cache_entry : recent_queries) {
    for (const auto& tree_root : cache_entry.query_plan.tree_roots()) {
      _inspect_operator(tree_root, cache_entry.access_frequency);
    }
  }

  _aggregate_usages();

  _estimate_cost();

  _calculate_desirability();

  // Sort by desirability, highest value first
  std::sort(_index_proposals.begin(), _index_proposals.end(), std::greater<IndexProposal>());

  return _index_proposals;
}

void IndexSelectionHeuristic::_inspect_operator(const std::shared_ptr<const AbstractOperator>& op, size_t query_frequency) {
  if (const auto& table_scan = std::dynamic_pointer_cast<const TableScan>(op)) {
    // skipped because it is skipped in lqp_translator
    //if (const auto& validate = std::dynamic_pointer_cast<const Validate>(table_scan->input_left())) {
      if (const auto& get_table = std::dynamic_pointer_cast<const GetTable>(table_scan->input_left())) {
        const auto& table_name = get_table->table_name();
        ColumnID column_id = table_scan->left_column_id();
        _index_proposals.emplace_back(IndexProposal{table_name, column_id, 0.0f, 0.0f, static_cast<int>(query_frequency), 0});
        std::cout << "TableScan on table " << table_name << " and column " << column_id << "\n";

        auto table = StorageManager::get().get_table(table_name);
        auto table_statistics = table->table_statistics();
        //auto column_statistics = table->table_statistics()->column_statistics().at(column_id);
        auto compare_value = boost::get<AllTypeVariant>(table_scan->right_parameter());
        auto predicate_statistics = table_statistics->predicate_statistics(column_id, table_scan->scan_type(), table_scan->right_parameter());
        auto selectivity = table_statistics->row_count() > 0 ? predicate_statistics->row_count() / table_statistics->row_count() : 1.0f;
        std::cout << table_name << "." << table->column_name(column_id);
        std::cout << " " << *table_statistics << "\n";
        std::cout << "predicate statistics: ";
        std::cout << " " << *predicate_statistics << "\n";
        std::cout << "-> selectivity: " << predicate_statistics->row_count() / (table_statistics->row_count() + 1) << "\n";

        _access_recods.emplace_back(AccessRecord{table_name, column_id, query_frequency, selectivity});
        _index_proposals.emplace_back(IndexProposal{table_name, column_id, 0.0f, 0.0f, static_cast<int>(query_frequency), 0});

      }
    //}
  } else {
    if (op->input_left()) {
      _inspect_operator(op->input_left(), query_frequency);
    }
    if (op->input_right()) {
      _inspect_operator(op->input_right(), query_frequency);
    }
  }
}

void IndexSelectionHeuristic::_aggregate_usages() {
  // ToDo(group01): this does not take into account if one query was called multiple times
  for (auto proposal_index = 0u; proposal_index < _index_proposals.size(); ++proposal_index) {
    auto& current_proposal = _index_proposals[proposal_index];

    for (auto candidate_index = proposal_index + 1; candidate_index < _index_proposals.size(); ++candidate_index) {
      auto& candidate = _index_proposals[candidate_index];
      // ToDo(group01): helper method referencesSameColum()
      if (candidate.table_name == current_proposal.table_name && candidate.column_id == current_proposal.column_id) {
        current_proposal.number_of_usages += candidate.number_of_usages;
        _index_proposals.erase(_index_proposals.begin() + candidate_index);
        --candidate_index;
      }
    }
  }

  for (const auto & access_record : _access_recods) {
      for (auto & index_proposal : _index_proposals) {
          if (index_proposal.table_name == access_record.table_name && index_proposal.column_id == access_record.column_id) {
              index_proposal.saved_work += (1.0 / access_record.selectivity) * access_record.number_of_usages;
          }
      }
  }
}

void IndexSelectionHeuristic::_estimate_cost() {
  // ToDo(group01): useful logic, e.g. number of chunks to estimate table size
  for (auto& proposal : _index_proposals) {
    proposal.cost = 1;
  }
}

void IndexSelectionHeuristic::_calculate_desirability() {
  // Map absolute usage + cost values to relative values (across all index proposals)
  // ToDo(group01: if we plan to continue with this approach, extract a calculateRelativeValues(accessor) method
  //               to deduplicate code.

  auto max_num_usages_element =
      std::max_element(_index_proposals.begin(), _index_proposals.end(), IndexProposal::compare_number_of_usages);
  auto max_num_usages = static_cast<float>(max_num_usages_element->number_of_usages);

  auto max_cost_element =
      std::max_element(_index_proposals.begin(), _index_proposals.end(), IndexProposal::compare_cost);
  auto min_cost_element =
      std::min_element(_index_proposals.begin(), _index_proposals.end(), IndexProposal::compare_cost);
  auto max_cost = static_cast<float>(max_cost_element->cost);
  auto min_cost = static_cast<float>(min_cost_element->cost);
  // If there is only one cost value, add one to prevent a division by zero error
  if (max_cost == min_cost) {
    max_cost += 1.0f;
  }

  for (auto& index_proposal : _index_proposals) {
    float relative_num_usages = static_cast<float>(index_proposal.number_of_usages) / max_num_usages;

    // From the cost minimum to cost maximum range, calculate where this cost value sits relatively
    float relative_cost = static_cast<float>(index_proposal.cost - min_cost) / (max_cost - min_cost);

    // ToDo(group01): better "mixdown" logic
    // Since higher cost is bad, invert that value
    float desirability = 0.5f * relative_num_usages + 0.5f * (1.0f - relative_cost);
    index_proposal.desirablility = desirability;
  }
}
}  // namespace opossum
