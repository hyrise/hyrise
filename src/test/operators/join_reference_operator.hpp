#pragma once

#include "operators/abstract_join_operator.hpp"

namespace opossum {

class JoinReferenceOperator : public AbstractJoinOperator {
 public:
  JoinReferenceOperator(const std::shared_ptr<const AbstractOperator>& left,
  const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
  const OperatorJoinPredicate& primary_predicate, const std::vector<OperatorJoinPredicate>& secondary_predicates);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  bool _rows_match(size_t left_row_idx, size_t right_row_idx) const;
  bool _predicate_matches(const OperatorJoinPredicate& predicate,
  const std::vector<AllTypeVariant>& row_left,
  const std::vector<AllTypeVariant>& row_right) const;
};

}  // namespace opossum
