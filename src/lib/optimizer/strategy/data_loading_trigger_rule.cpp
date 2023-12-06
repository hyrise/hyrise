#include "data_loading_trigger_rule.hpp"

#include <iostream>

#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace std {

template <>
struct hash<std::pair<std::shared_ptr<hyrise::Table>, hyrise::ColumnID>> {
  size_t operator()(const std::pair<std::shared_ptr<hyrise::Table>, hyrise::ColumnID>& statistics_key) const {
  auto hash = size_t{0};
  boost::hash_combine(hash, statistics_key.first);
  boost::hash_combine(hash, statistics_key.second);
  return hash;
  }
};

}  // namespace std

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

}  // namespace

namespace hyrise {

DataLoadingTriggerRule::DataLoadingTriggerRule(const bool load_predicates_only) : _load_predicates_only{load_predicates_only} {}

std::string DataLoadingTriggerRule::name() const {
  static const auto name = std::string{"DataLoadingTriggerRule"};
  return name;
}

void DataLoadingTriggerRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  auto& plugin_manager = Hyrise::get().plugin_manager;
  const auto& plugins = plugin_manager.loaded_plugins();

  if (!std::binary_search(plugins.cbegin(), plugins.cend(), "hyriseDataLoadingPlugin")) {
    return;
  }

  auto columns_to_access = std::unordered_set<std::pair<std::shared_ptr<Table>, ColumnID>>{};
  columns_to_access.reserve(16);

  // auto sstream = std::stringstream{};
  const auto& storage_manager = Hyrise::get().storage_manager;
  if (_load_predicates_only) {
    visit_lqp(lqp_root, [&](const auto& node) {
      switch (node->type) {
        case LQPNodeType::Predicate: {
          const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
          const auto& predicate_expression = predicate_node->predicate();
          visit_expression(predicate_expression, [&](const auto& expression) {
            if (expression->type != ExpressionType::LQPColumn) {
              return ExpressionVisitation::VisitArguments;
            }
            const auto& column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
            const auto& stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(column_expression->original_node.lock());
            if (!stored_table_node) {
              return ExpressionVisitation::VisitArguments;
            }

            columns_to_access.emplace(storage_manager.get_table(stored_table_node->table_name), column_expression->original_column_id);
            // auto sstream = std::stringstream{};
            // sstream << *node;
            // std::cout << std::format("Added {} and column #{} for LQP node: {}", stored_table_node->table_name, static_cast<size_t>(column_expression->original_column_id), sstream.str()) << std::endl;

            return ExpressionVisitation::VisitArguments;
           });
        } break;
        case LQPNodeType::Join: {
          const auto& join_node = std::static_pointer_cast<JoinNode>(node);
          // At the very beginning, join predicates are not yet assigned to a join (i.e., all joins are cross joins).
          if (join_node->join_mode != JoinMode::Cross && join_node->join_mode != JoinMode::Left) {
            auto sstream = std::stringstream{};
            sstream << *node;
            Fail("Expected this rule to run before any other rules. Node: " + sstream.str());
          }
        } break;
        default:
          break;
      }
      return LQPVisitation::VisitInputs;
    });
    // sstream << "Loading predicate columns: ";
  } else {
    visit_lqp(lqp_root, [&](const auto& node) {
      for (const auto& node_expression : node->node_expressions) {
        visit_expression(node_expression, [&](const auto& expression) {
          if (expression->type != ExpressionType::LQPColumn) {
            return ExpressionVisitation::VisitArguments;
          }

          const auto& column_expression = std::static_pointer_cast<LQPColumnExpression>(expression);
          const auto& stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(column_expression->original_node.lock());
          if (!stored_table_node) {
            return ExpressionVisitation::VisitArguments;
          }

          // per LQP node, store table-column pair

          columns_to_access.emplace(storage_manager.get_table(stored_table_node->table_name),
                                                                            column_expression->original_column_id);
          return ExpressionVisitation::VisitArguments;
        });
      }
      return LQPVisitation::VisitInputs;
    });

    // TODO: is it worth it to only reuest columns that have not been loaded already?
    // sstream << "Loading ALL columns: ";
  }

  // const auto tables = Hyrise::get().storage_manager.tables();
  // for (const auto& [table, column_id] : columns_to_access) {
  //   auto table_found = false;
  //   auto table_name = std::string{};
  //   for (const auto& [_table_name, _table] : tables) {
  //     if (table == _table) {
  //       table_name = _table_name;
  //       table_found = true;
  //       break;
  //     }
  //   }
  //   Assert(table_found, "No table name found in storage manager.");
  //   sstream << table_name << "::" << table->column_name(column_id) << " - ";
  // }
  // sstream << "\n";
  // std::cerr << sstream.str();

  // Sort columns by table sizes (i.e., processing of large columns starts earlier).
  auto sorted_columns_to_access = std::vector(columns_to_access.begin(), columns_to_access.end());
  std::sort(sorted_columns_to_access.begin(), sorted_columns_to_access.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.first->row_count() > rhs.first->row_count();
  });

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(columns_to_access.size());
  for (const auto& [table, column_id] : sorted_columns_to_access) {
    const auto chunk = table->get_chunk(ChunkID{0});
    if (column_id > chunk->column_count()) {
      // std::cerr << std::format("Skipping the loading of columnID {}.\n", static_cast<size_t>(column_id));
      continue;
    }

    // std::cout << "We should load " << &*table << " and " << column_id << std::endl;
    const auto segment = chunk->get_segment(column_id);
    if (!std::dynamic_pointer_cast<PlaceHolderSegment>(segment)) {
      // Only access histogram, when first segment is a PlaceHolderSegment (remember, we replace all segments of a columns).
      continue;
    }

    jobs.emplace_back(std::make_shared<JobTask>([&, table=table, column_id=column_id]() {
      resolve_data_type(table->column_data_type(column_id), [&, table=table, column_id=column_id](const auto& data_type) {
        using ColumnDataType = typename std::decay_t<decltype(data_type)>::type;

        // Just use data utils load column?!?!?
        const auto column_statistics = table->table_statistics()->column_statistics;
        const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(column_statistics[column_id]);
        attribute_statistics->histogram();
      });
    }));
  }

  if (_load_predicates_only) {
    // There are currently too many parts in the optimizer that access statistics. Hence, we block here and wait as we
    // don't want to examine all paths.
    // The issue is relevant as some queries (e.g., Q7) produce massive cross joins when histograms are missing. Leading
    // to 100 GB Hyrise instances for scales factors < 1.0.
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  } else {
    Hyrise::get().scheduler()->schedule_tasks(jobs);
  }
  // auto sstream = std::stringstream{};
  // sstream << std::boolalpha << "Rule (predicates only: " << _load_predicates_only << "): we scheduled tasks IDs: ";
  // for (const auto& job : jobs) {
  //   sstream << job->id() << ", ";
  // }
  // sstream << "\n";
  // std::cerr << sstream.str();
  // std::cerr << "Optimizer issued loading for all predicate columns.\n";
}

}  // namespace hyrise
