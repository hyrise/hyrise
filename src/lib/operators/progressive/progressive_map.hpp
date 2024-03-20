#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/progressive/chunk_sink.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "types.hpp"

namespace hyrise {

class ProgressiveMap : public AbstractReadOnlyOperator {
 public:
  ProgressiveMap(const std::shared_ptr<const AbstractOperator>& input_operator,
                 std::shared_ptr<ChunkSink>& input_chunk_sink, std::shared_ptr<ChunkSink>& output_chunk_sink,
                 const OperatorType operator_type);
  const std::string& name() const override;
  void set_table_scan_predicate(std::shared_ptr<AbstractExpression> predicate);
  void set_projection_expressions(/* const std::vector<std::shared_ptr<AbstractExpression>>& expressions */);

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  std::shared_ptr<ChunkSink>& _input_chunk_sink;
  std::shared_ptr<ChunkSink>& _output_chunk_sink;
  std::shared_ptr<AbstractExpression> _table_scan_predicate;
  OperatorType _operator_type;
};

}  // namespace hyrise
