#include "plan_exporter.hpp"

#include <boost/algorithm/string.hpp>
#include <nlohmann/json.hpp>

#include "feature_extraction/feature_nodes/aggregate_function_feature_node.hpp"
#include "feature_extraction/feature_nodes/operator_feature_node.hpp"
#include "feature_extraction/feature_nodes/predicate_feature_node.hpp"
#include "feature_extraction/feature_nodes/segment_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace hyrise {

void PlanExporter::add_plan(const std::shared_ptr<Query>& query, const std::shared_ptr<const AbstractOperator>& pqp) {
  _feature_graphs.push_back(OperatorFeatureNode::from_pqp(pqp, query));
}

void PlanExporter::export_plans(const std::string& output_directory) {
  std::cout << "export plans to " << output_directory << std::endl;
  _export_additional_operator_info(output_directory);

  auto table_file_name = output_directory + "/tables.csv";
  auto column_file_name = output_directory + "/columns.csv";
  auto segment_file_name = output_directory + "/segments.csv";
  auto operator_file_name = output_directory + "/operators.csv";
  auto predicate_file_name = output_directory + "/predicates.csv";
  auto aggregate_file_name = output_directory + "/aggregate_functions.csv";

  auto table_file = std::ofstream{table_file_name};
  auto column_file = std::ofstream{column_file_name};
  auto segment_file = std::ofstream{segment_file_name};
  auto operator_file = std::ofstream{operator_file_name};
  auto predicate_file = std::ofstream{predicate_file_name};
  auto aggregate_file = std::ofstream{aggregate_file_name};

  // clang-format off
  table_file     << "table_id";
  column_file    << "query;operator_id;expression_id;column_id";
  segment_file   << "query;operator_id;expression_id;column_id;segment_id";
  operator_file  << "query;operator_id;";
  predicate_file << "query;operator_id;expression_id";
  aggregate_file << "query;operator_id;expression_id";

  table_file     << boost::algorithm::join(AbstractTableFeatureNode::headers(), ";") << ";subqueries"<< "\n";
  column_file    << boost::algorithm::join(ColumnFeatureNode::headers(), ";") << "\n";
  segment_file   << boost::algorithm::join(SegmentFeatureNode::headers(), ";") << "\n";
  operator_file  << boost::algorithm::join(OperatorFeatureNode::headers(), ";");
  predicate_file << boost::algorithm::join(PredicateFeatureNode::headers(), ";") << "\n";
  aggregate_file << boost::algorithm::join(AggregateFunctionFeatureNode::headers(), ";") << "\n";

  operator_file  << ";left_input;right_input;runtime;estimated_cardinality\n";
  // clang-format on

  _output_files[FeatureNodeType::Table] = std::move(table_file);
  _output_files[FeatureNodeType::Column] = std::move(column_file);
  _output_files[FeatureNodeType::Segment] = std::move(segment_file);
  _output_files[FeatureNodeType::Operator] = std::move(operator_file);
  _output_files[FeatureNodeType::Predicate] = std::move(predicate_file);
  _output_files[FeatureNodeType::AggregateFunction] = std::move(aggregate_file);

  for (auto& [type, output_file] : _output_files) {
    Assert(output_file.is_open(), "File not open: " + std::string{magic_enum::enum_name(type)});
  }

  for (const auto& node : _feature_graphs) {
    const auto& root = static_cast<OperatorFeatureNode&>(*node);
    auto cardinality_estimator = CardinalityEstimator{};
    _features_to_csv(root.query()->hash, node, cardinality_estimator);
  }

  for (auto& [type, output_file] : _output_files) {
    Assert(output_file.good(), "error writing to file for " + std::string{magic_enum::enum_name(type)});
    output_file.flush();
    output_file.close();
  }
}

void PlanExporter::_features_to_csv(const std::string& query, const std::shared_ptr<AbstractFeatureNode>& graph,
                                    CardinalityEstimator& cardinality_estimator) {
  visit_feature_nodes(graph, [&](const auto& node) {
    // std::cout << "    " << node->hash() << std::endl;
    const auto node_type = node->type();
    switch (node_type) {
      case FeatureNodeType::Operator: {
        const auto& operator_node = static_cast<OperatorFeatureNode&>(*node);
        const auto hash = node->hash();

        auto& operator_file = _output_files.at(FeatureNodeType::Operator);
        operator_file << query << ";" << hash << node->to_feature_vector();
        for (const auto& input : {node->left_input(), node->right_input()}) {
          operator_file << ";";
          if (input) {
            operator_file << input->hash();
          }
        }

        const auto& subqueries = operator_node.subqueries();
        auto subquery_hashes = std::vector<std::string>{};
        subquery_hashes.reserve(subqueries.size());
        std::for_each(subqueries.cbegin(), subqueries.cend(),
                      [&](const auto& subquery) { subquery_hashes.emplace_back(std::to_string(subquery->hash())); });

        operator_file << ";" << operator_node.run_time().count() << ";"
                      << cardinality_estimator.estimate_cardinality(operator_node.get_operator()->lqp_node) << ";"
                      << boost::algorithm::join(subquery_hashes, ",") << "\n";

        const auto& expressions = operator_node.expressions();
        const auto num_expressions = expressions.size();
        for (auto expression_id = size_t{0}; expression_id < num_expressions; ++expression_id) {
          const auto& expression = expressions[expression_id];
          const auto expression_type = expression->type();
          auto& output_file = _output_files.at(expression_type);
          switch (expression_type) {
            case FeatureNodeType::Predicate: {
              auto prefix = std::stringstream{};
              prefix << query << ";" << hash << ";" << expression_id << ";";
              const auto prefix_str = prefix.str();
              output_file << prefix_str << expression->to_feature_vector() << "\n";
              auto column_id = ColumnID{0};
              for (const auto& column : {expression->left_input(), expression->right_input()}) {
                if (!column) {
                  Assert(column_id != ColumnID{0}, "expected left input column before right input column");
                  ++column_id;
                  continue;
                }
                Assert(column->type() == FeatureNodeType::Column, "expected column");
                _export_column(static_cast<ColumnFeatureNode&>(*column), prefix_str, column_id);
                ++column_id;
              }
            } break;

            case FeatureNodeType::Column: {
              auto prefix = std::stringstream{};
              prefix << query << ";" << hash << ";;";
              const auto prefix_str = prefix.str();
              _export_column(static_cast<ColumnFeatureNode&>(*expression), prefix_str, expression_id);
            } break;

            case FeatureNodeType::AggregateFunction: {
              auto prefix = std::stringstream{};
              prefix << query << ";" << hash << ";" << expression_id << ";";
              const auto prefix_str = prefix.str();
              output_file << prefix_str << expression->to_feature_vector();
              _export_column(static_cast<ColumnFeatureNode&>(*expression->left_input()), prefix_str, expression_id);

            } break;

            default:
              Fail("Unexpected feature node: " + std::string{magic_enum::enum_name(expression_type)});
          }
        }

        for (const auto& subquery : subqueries) {
          _features_to_csv(query, subquery, cardinality_estimator);
        }

        return FeatureNodeVisitation::VisitInputs;
      }

      case FeatureNodeType::Table: {
        auto& table_file = _output_files.at(FeatureNodeType::Table);
        table_file << node->hash() << ";" << node->to_feature_vector() << "\n";

        return FeatureNodeVisitation::DoNotVisitInputs;
      }

      default:
        Fail("Unexpected node in graph: " + std::string{magic_enum::enum_name(node_type)});
    }
  });
}

void PlanExporter::_export_column(ColumnFeatureNode& column_node, const std::string& prefix, const size_t column_id) {
  auto& column_file = _output_files.at(FeatureNodeType::Column);
  column_file << prefix << column_id << ";" << column_node.to_feature_vector() << "\n";

  const auto& segments = column_node.segments();
  const auto segment_count = segments.size();
  for (auto segment_id = ChunkID{0}; segment_id < segment_count; ++segment_id) {
    const auto& segment = segments[segment_id];
    auto& segment_file = _output_files.at(FeatureNodeType::Segment);
    segment_file << prefix << column_id << ";" << segment_id << ";" << segment->to_feature_vector() << "\n";
  }
}

void PlanExporter::_export_additional_operator_info(const std::string& output_directory) {
  const auto headers_per_operator = std::unordered_map<QueryOperatorType, std::vector<std::string>>{
      {QueryOperatorType::TableScan, OperatorFeatureNode::TableScanOperatorInfo::headers()},
      {QueryOperatorType::JoinHash, OperatorFeatureNode::JoinHashOperatorInfo::headers()},
      {QueryOperatorType::Aggregate, OperatorFeatureNode::AggregateHashOperatorInfo::headers()}};

  auto headers_json = nlohmann::json{};
  for (const auto& [type, headers] : headers_per_operator) {
    headers_json[std::string{magic_enum::enum_name(type)}] = headers;
  }
  std::ofstream{output_directory + "/additional_operator_headers.json"} << std::setw(2) << headers_json << std::endl;
}

}  // namespace hyrise
