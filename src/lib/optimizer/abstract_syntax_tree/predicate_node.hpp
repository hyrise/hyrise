#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_ast_node.hpp"
#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "common.hpp"
#include "optimizer/expression.hpp"

namespace opossum {

struct ColumnID;
class TableStatistics;

/**
 * This node type represents a filter.
 * The most common use case is to represent a regular TableScan,
 * but this node is also supposed to be used for IndexScans, for example.
 *
 * HAVING clauses of GROUP BY clauses will be translated to this node type as well.
 */
class PredicateNode : public AbstractASTNode {
 public:
  PredicateNode(const ColumnID column_id, const ScanType scan_type, const AllParameterVariant& value,
                const optional<AllTypeVariant>& value2 = nullopt);

  std::string description() const override;

  const ColumnID column_id() const;
  ScanType scan_type() const;
  const AllParameterVariant& value() const;
  const optional<AllTypeVariant>& value2() const;

  const std::shared_ptr<TableStatistics> get_statistics_from(
      const std::shared_ptr<AbstractASTNode>& parent) const override;

 private:
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;
};

}  // namespace opossum
