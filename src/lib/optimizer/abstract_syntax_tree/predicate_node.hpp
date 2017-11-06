#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_ast_node.hpp"
#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"

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
                const std::optional<AllTypeVariant>& value2 = std::nullopt);

  std::string description(DescriptionMode mode) const override;

  const ColumnID column_id() const;
  ScanType scan_type() const;
  const AllParameterVariant& value() const;
  const std::optional<AllTypeVariant>& value2() const;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractASTNode>& left_child,
      const std::shared_ptr<AbstractASTNode>& right_child = nullptr) const override;

  void map_column_ids(const ColumnIDMapping& column_id_mapping, ASTChildSide caller_child_side) override;

 private:
  ColumnID _column_id;
  const ScanType _scan_type;
  AllParameterVariant _value;
  const std::optional<AllTypeVariant> _value2;
};

}  // namespace opossum
