#pragma once

#include "../configuration/calibration_column_specification.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/encoding_type.hpp"
#include "types.hpp"

namespace opossum {

struct CalibrationQueryGeneratorJoinConfiguration {
  const EncodingType encoding_type;
  const DataType data_type;
  const bool reference_column;
};

using JoinGeneratorFunctor = std::function<const std::shared_ptr<AbstractExpression>(
    const CalibrationQueryGeneratorJoinConfiguration& configuration, const std::shared_ptr<MockNode>&,
    const std::shared_ptr<MockNode>&, const std::vector<CalibrationColumnSpecification>&)>;

class CalibrationQueryGeneratorJoin {
 public:
  static const std::vector<std::shared_ptr<AbstractLQPNode>> generate_join(
      const CalibrationQueryGeneratorJoinConfiguration& configuration,
      const JoinGeneratorFunctor& join_predicate_generator, const std::shared_ptr<MockNode>& left_table,
      const std::shared_ptr<MockNode>& right_table, const std::vector<CalibrationColumnSpecification>& column_definitions);

  /*
     * Functors to generate joins.
     * They all implement 'JoinGeneratorFunctor'
     */
  static const std::shared_ptr<AbstractExpression> generate_join_predicate(
      const CalibrationQueryGeneratorJoinConfiguration& configuration, const std::shared_ptr<MockNode>& left_table,
      const std::shared_ptr<MockNode>& right_table, const std::vector<CalibrationColumnSpecification>& column_definitions);

 private:

    static const std::optional<CalibrationColumnSpecification> _find_column_for_configuration(
            const std::vector<CalibrationColumnSpecification>& column_definitions,
            const CalibrationQueryGeneratorJoinConfiguration& configuration);
  CalibrationQueryGeneratorJoin() = default;
};

}  // namespace opossum
