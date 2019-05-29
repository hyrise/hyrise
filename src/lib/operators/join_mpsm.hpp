#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "types.hpp"

namespace opossum {

/**
   * This operator joins two tables using one column of each table by performing radix-partition-sort and a merge join.
   * This is done in multiple phases to minimize random reads. This makes the Multi Phase Sort Merge (MPSM) Join more
   * efficient on NUMA systems, leading to increased performance compared to other joins, especially the Sort Merge Join.
   *
   * The output is a new table with referenced columns for all columns of the two inputs and filtered pos_lists.
   *
   * As with most operators, we do not guarantee a stable operation with regards to positions -
   * i.e., your sorting order might be disturbed.
**/
class JoinMPSM : public AbstractJoinOperator {
 public:
  static bool supports(JoinMode join_mode, PredicateCondition predicate_condition, DataType left_data_type,
                       DataType right_data_type, bool secondary_predicates);

  JoinMPSM(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
           const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
           const std::vector<OperatorJoinPredicate>& secondary_predicates = {});

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  template <typename T>
  class JoinMPSMImpl;
  template <typename T>
  friend class JoinMPSMImpl;

  std::unique_ptr<AbstractJoinOperatorImpl> _impl;
};

}  // namespace opossum
