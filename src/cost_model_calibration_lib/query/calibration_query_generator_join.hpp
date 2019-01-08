#pragma once

#include "../configuration/calibration_column_specification.hpp"
#include "../configuration/calibration_configuration.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace opossum {

struct CalibrationQueryGeneratorJoinConfiguration {
  const std::string left_table_name;
  const std::string right_table_name;
  const EncodingType encoding_type;
  const DataType data_type;
  const bool reference_column;
  const size_t table_ratio;
};

// So that google test, e.g., prints readable error messages
    inline std::ostream& operator<<(std::ostream& stream, const CalibrationQueryGeneratorJoinConfiguration& configuration) {

        const auto reference_column_string = configuration.reference_column ? "true" : "false";
        return stream << "CalibrationQueryGeneratorJoinConfiguration("<<
        configuration.left_table_name << " - " <<
        configuration.right_table_name << " - " <<
        encoding_type_to_string.left.at(configuration.encoding_type) << " - " <<
        data_type_to_string.left.at(configuration.data_type) << " - " <<
        reference_column_string << " - " <<
        configuration.table_ratio
        << ")";
    }

class CalibrationQueryGeneratorJoin {
 public:
  static const std::vector<CalibrationQueryGeneratorJoinConfiguration> generate_join_permutations(
      const std::vector<std::pair<std::string, size_t>>& tables, const CalibrationConfiguration& configuration);

  CalibrationQueryGeneratorJoin(const CalibrationQueryGeneratorJoinConfiguration& configuration,
                                const std::vector<CalibrationColumnSpecification>& column_definitions);

  const std::vector<std::shared_ptr<AbstractLQPNode>> generate_join(
      const std::shared_ptr<StoredTableNode>& left_table, const std::shared_ptr<StoredTableNode>& right_table) const;

 private:
  const std::shared_ptr<AbstractExpression> _generate_join_predicate(
      const std::shared_ptr<StoredTableNode>& left_table, const std::shared_ptr<StoredTableNode>& right_table) const;

  const std::optional<CalibrationColumnSpecification> _find_primary_key() const;
  const std::optional<CalibrationColumnSpecification> _find_foreign_key() const;

  const CalibrationQueryGeneratorJoinConfiguration _configuration;
  const std::vector<CalibrationColumnSpecification>& _column_definitions;
};

}  // namespace opossum
