#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "common.hpp"

#include "abstract_ast_node.hpp"
#include "expression_node.hpp"

#include "operators/table_scan.hpp"

namespace opossum {

class PredicateNode : public AbstractASTNode {
 public:
  PredicateNode(const std::string& column_name, const std::shared_ptr<ExpressionNode> predicate, ScanType scan_type,
                const AllParameterVariant value, const optional<AllTypeVariant> value2 = nullopt);

  std::string description() const override;

  const std::string& column_name() const;
  const std::shared_ptr<ExpressionNode> predicate() const;
  ScanType scan_type() const;
  const AllParameterVariant& value() const;
  const optional<AllTypeVariant>& value2() const;

 private:
  const std::string _column_name;
  const std::shared_ptr<ExpressionNode> _predicate;
  const ScanType _scan_type;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;
};

}  // namespace opossum
