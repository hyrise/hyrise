#include "plan_exporter.hpp"

#include <fstream>

#include <boost/algorithm/string.hpp>

#include "feature_extraction/feature_nodes/abstract_table_feature_node.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "feature_extraction/feature_nodes/operator_feature_node.hpp"
#include "feature_extraction/feature_nodes/predicate_feature_node.hpp"
#include "feature_extraction/feature_nodes/segment_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace opossum {

void PlanExporter::add_plan(const std::shared_ptr<Query>& query, const std::shared_ptr<const AbstractOperator>& pqp) {
  _feature_graphs.push_back(OperatorFeatureNode::from_pqp(pqp, query));
}

void PlanExporter::export_plans(const std::string& file_name) {
  std::cout << "export plans to " << file_name << std::endl;
  auto table_file_name = file_name + "/tables.csv";
  auto column_file_name = file_name + "/columns.csv";
  auto segment_file_name = file_name + "/segments.csv";
  auto operator_file_name = file_name + "/operators.csv";
  auto predicate_file_name = file_name + "/predicates.csv";

  auto table_file = std::ofstream{table_file_name};
  auto column_file = std::ofstream{column_file_name};
  auto segment_file = std::ofstream{segment_file_name};
  auto operator_file = std::ofstream{operator_file_name};
  auto predicate_file = std::ofstream{predicate_file_name};

  // clang-format off
  table_file    << "table_id";
  column_file    << "query;subquery;subquery_id;operator_id;predicate_id;column_id";
  segment_file   << "query;subquery;subquery_id;operator_id;predicate_id;column_id;segment_id";
  operator_file  << "query;subquery;subquery_id;operator_id;";
  predicate_file << "query;subquery;subquery_id;operator_id;predicate_id";

  table_file     << boost::algorithm::join(AbstractTableFeatureNode::headers(), ";") << "\n";
  column_file    << boost::algorithm::join(ColumnFeatureNode::headers(), ";") << "\n";
  segment_file   << boost::algorithm::join(SegmentFeatureNode::headers(), ";") << "\n";
  operator_file  << boost::algorithm::join(OperatorFeatureNode::headers(), ";");
  predicate_file << boost::algorithm::join(PredicateFeatureNode::headers(), ";") << "\n";

  operator_file  << ";left_input;right_input;runtime;estimated_cardinality\n";
  // clang-format on

  auto output_files = std::unordered_map<FeatureNodeType, std::ofstream>{};
  output_files[FeatureNodeType::Table] = std::move(table_file);
  output_files[FeatureNodeType::Column] = std::move(column_file);
  output_files[FeatureNodeType::Segment] = std::move(segment_file);
  output_files[FeatureNodeType::Operator] = std::move(operator_file);
  output_files[FeatureNodeType::Predicate] = std::move(predicate_file);

  for (auto& [type, output_file] : output_files) {
    Assert(output_file.is_open(), "File not open: " + std::string{magic_enum::enum_name(type)});
  }

  for (const auto& node : _feature_graphs) {
    const auto& root = static_cast<OperatorFeatureNode&>(*node);
    auto cardinality_estimator = CardinalityEstimator{};
    _features_to_csv(root.query()->hash, node, output_files, cardinality_estimator);
  }

  for (auto& [type, output_file] : output_files) {
    Assert(output_file.good(), "error writing to file for " + std::string{magic_enum::enum_name(type)});
    output_file.flush();
    output_file.close();
  }
}

void PlanExporter::_features_to_csv(const std::string& query, const std::shared_ptr<AbstractFeatureNode>& graph,
                                    std::unordered_map<FeatureNodeType, std::ofstream>& output_files,
                                    CardinalityEstimator& cardinality_estimator, const std::optional<size_t>& subquery,
                                    const std::optional<size_t>& subquery_id) const {
  const auto subquery_string = subquery ? std::to_string(*subquery) : "";
  const auto subquery_id_string = subquery ? std::to_string(*subquery) : "";

  visit_feature_nodes(graph, [&](const auto& node) {
    // std::cout << "    " << node->hash() << std::endl;
    const auto node_type = node->type();
    switch (node_type) {
      case FeatureNodeType::Operator: {
        const auto& operator_node = static_cast<OperatorFeatureNode&>(*node);
        const auto hash = node->hash();

        auto& operator_file = output_files.at(FeatureNodeType::Operator);
        operator_file << query << ";" << subquery_string << ";" << subquery_id_string << ";" << hash;
        feature_vector_to_stream(operator_file, node->to_feature_vector());
        for (const auto& input : {node->left_input(), node->right_input()}) {
          operator_file << ";";
          if (input) {
            operator_file << input->hash();
          }
        }
        operator_file << ";" << operator_node.run_time().count() << ";"
                      << cardinality_estimator.estimate_cardinality(operator_node.get_operator()->lqp_node) << "\n";

        const auto& predicates = operator_node.predicates();
        const auto num_predicates = predicates.size();
        for (auto predicate_id = size_t{0}; predicate_id < num_predicates; ++predicate_id) {
          const auto& predicate = predicates[predicate_id];
          auto& predicate_file = output_files.at(FeatureNodeType::Predicate);
          predicate_file << query << ";" << subquery_string << ";" << subquery_id_string << hash << ";" << predicate_id
                         << ";";
          feature_vector_to_stream(predicate_file, predicate->to_feature_vector());
          predicate_file << "\n";
          auto column_id = ColumnID{0};
          for (const auto& column : {predicate->left_input(), predicate->right_input()}) {
            if (!column) {
              Assert(column_id != ColumnID{0}, "expected left input column before right input column");
              ++column_id;
              continue;
            }
            Assert(column->type() == FeatureNodeType::Column, "expected column");
            auto& column_file = output_files.at(FeatureNodeType::Column);
            column_file << query << ";" << subquery_string << ";" << subquery_id_string << hash << ";" << predicate_id
                        << ";" << column_id << ";";
            feature_vector_to_stream(column_file, column->to_feature_vector());
            column_file << "\n";

            const auto& column_node = static_cast<ColumnFeatureNode&>(*column);
            const auto& segments = column_node.segments();
            const auto segment_count = segments.size();
            for (auto segment_id = ChunkID{0}; segment_id < segment_count; ++segment_id) {
              const auto& segment = segments[segment_id];
              auto& segment_file = output_files.at(FeatureNodeType::Segment);
              segment_file << query << ";" << subquery_string << ";" << subquery_id_string << hash << ";"
                           << predicate_id << ";" << column_id << ";" << segment_id << ";";
              feature_vector_to_stream(segment_file, segment->to_feature_vector());
              segment_file << "\n";
            }
            ++column_id;
          }
        }

        const auto& subqueries = operator_node.subqueries();
        const auto num_subqueries = subqueries.size();
        for (auto op_subquery_id = size_t{0}; op_subquery_id < num_subqueries; ++op_subquery_id) {
          _features_to_csv(query, subqueries[op_subquery_id], output_files, cardinality_estimator, hash,
                           op_subquery_id);
        }

        return FeatureNodeVisitation::VisitInputs;
      }

      case FeatureNodeType::Table: {
        auto& table_file = output_files.at(FeatureNodeType::Table);
        table_file << node->hash() << ";";
        feature_vector_to_stream(table_file, node->to_feature_vector());
        table_file << "\n";

        return FeatureNodeVisitation::DoNotVisitInputs;
      }

      default:
        Fail("Unexpected node in graph: " + std::string{magic_enum::enum_name(node_type)});
    }
  });
}

}  // namespace opossum
