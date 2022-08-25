#pragma once

#include "abstract_feature_node.hpp"
#include "feature_extraction/feature_nodes/result_table_feature_node.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"

namespace hyrise {

class OperatorFeatureNode : public AbstractFeatureNode {
 public:
  OperatorFeatureNode(const std::shared_ptr<const AbstractOperator>& op,
                      const std::shared_ptr<AbstractFeatureNode>& left_input,
                      const std::shared_ptr<AbstractFeatureNode>& right_input = nullptr);

  static std::shared_ptr<OperatorFeatureNode> from_pqp(const std::shared_ptr<const AbstractOperator>& op,
                                                       const std::shared_ptr<Query>& query);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

  std::chrono::nanoseconds run_time() const;

  bool is_root_node() const;

  void set_as_root_node(const std::shared_ptr<Query>& query);

  std::shared_ptr<Query> query() const;

  std::shared_ptr<const AbstractOperator> get_operator() const;

  std::shared_ptr<ResultTableFeatureNode> output_table() const;

  const std::vector<std::shared_ptr<OperatorFeatureNode>>& subqueries() const;

  const std::vector<std::shared_ptr<AbstractFeatureNode>>& expressions() const;

  void initialize();

 protected:
  friend class PlanExporter;

  struct AbstractAdditionalOperatorInfo : public Noncopyable {
    virtual ~AbstractAdditionalOperatorInfo() = default;
    virtual const FeatureVector to_feature_vector() const = 0;
    virtual size_t feature_count() const = 0;
  };

  struct TableScanOperatorInfo : public AbstractAdditionalOperatorInfo {
    static const std::vector<std::string>& headers();
    const FeatureVector to_feature_vector() const final;
    size_t feature_count() const final;

    size_t skipped_chunks_none_match{0};
    size_t skipped_chunks_all_match{0};
    size_t skipped_chunks_binary_search{0};
  };

  struct JoinHashOperatorInfo : public AbstractAdditionalOperatorInfo {
    static const std::vector<std::string>& headers();
    const FeatureVector to_feature_vector() const final;
    size_t feature_count() const final;

    size_t radix_bits{0};
    size_t build_side_materialized_value_count{0};
    size_t probe_side_materialized_value_count{0};
    size_t hash_tables_distinct_value_count{0};
    size_t hash_tables_position_count{0};
  };

  struct AggregateHashOperatorInfo : public AbstractAdditionalOperatorInfo {
    static const std::vector<std::string>& headers();
    const FeatureVector to_feature_vector() const final;
    size_t feature_count() const final;

    size_t groupby_column_count{0};
    size_t aggregate_column_count{0};
  };

 private:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;
  size_t _on_shallow_hash() const final;

  void _handle_general_operator(const AbstractOperator& op);
  void _handle_join_hash(const JoinHash& join_hash);
  void _handle_join_index(const JoinIndex& join_index);
  void _handle_table_scan(const TableScan& table_scan);
  void _handle_index_scan(const IndexScan& index_scan);
  void _handle_aggregate(const AggregateHash& aggregate);
  void _handle_projection(const Projection& projection);
  void _handle_get_table(const GetTable& get_table);

  void _add_subqueries(const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  std::shared_ptr<const AbstractOperator> _op;
  QueryOperatorType _op_type;
  std::chrono::nanoseconds _run_time;
  std::shared_ptr<Query> _query;

  std::vector<std::shared_ptr<AbstractFeatureNode>> _expressions;
  std::shared_ptr<ResultTableFeatureNode> _output_table;

  bool _is_root_node{false};

  std::vector<std::shared_ptr<OperatorFeatureNode>> _subqueries;

  std::unique_ptr<AbstractAdditionalOperatorInfo> _additional_info;
};

}  // namespace hyrise
