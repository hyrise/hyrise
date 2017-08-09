#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_ast_node.hpp"
#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "common.hpp"
#include "optimizer/expression/expression_node.hpp"

namespace opossum {

class TableStatistics;

class PredicateNode : public AbstractASTNode {
 public:
  PredicateNode(const std::string& column_name, const std::shared_ptr<ExpressionNode>& predicate,
                const ScanType scan_type, const AllParameterVariant value,
                const optional<AllTypeVariant> value2 = nullopt);

  std::string description() const override;

  const std::string& column_name() const;
  const std::shared_ptr<ExpressionNode> predicate() const;
  ScanType scan_type() const;
  const AllParameterVariant& value() const;
  const optional<AllTypeVariant>& value2() const;

  void set_predicate(const std::shared_ptr<ExpressionNode>& predicate);

  const std::shared_ptr<TableStatistics> calculate_statistics_from(
      const std::shared_ptr<AbstractASTNode>& parent) const override;

 private:
  const std::shared_ptr<TableStatistics> _calculate_statistics() const override;

  const std::string _column_name;
  std::shared_ptr<ExpressionNode> _predicate;
  const ScanType _scan_type;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;
};

}  // namespace opossum
