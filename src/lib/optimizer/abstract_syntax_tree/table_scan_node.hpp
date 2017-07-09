#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "common.hpp"
#include "operators/table_scan.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"

namespace opossum {

class TableScanNode : public AbstractNode {
 public:
  TableScanNode(const std::string& column_name, const ScanType& scan_type, const AllParameterVariant value,
                const optional<AllTypeVariant> value2 = nullopt);

  const std::string description() const override;

  const std::string& column_name() const;
  const ScanType& scan_type() const;
  const AllParameterVariant& value() const;
  const optional<AllTypeVariant>& value2() const;

 protected:
  std::shared_ptr<TableStatistics> create_statistics() const override;

 private:
  const std::string _column_name;
  const ScanType _scan_type;
  const AllParameterVariant _value;
  const optional<AllTypeVariant> _value2;
};

}  // namespace opossum
