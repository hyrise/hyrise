#include "clustering.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {

Clustering::Clustering(const std::shared_ptr<const AbstractOperator>& referencing_table_op)
    : AbstractReadWriteOperator{OperatorType::Clustering, referencing_table_op} {}

const std::string& Clustering::name() const {
  static const auto name = std::string{"Clustering"};
  return name;
}

std::shared_ptr<const Table> Clustering::_on_execute(std::shared_ptr<TransactionContext> context) {
  
  return nullptr;
}

void Clustering::_on_commit_records(const CommitID commit_id) {
  
}

void Clustering::_on_rollback_records() {
  
}

std::shared_ptr<AbstractOperator> Clustering::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Clustering>(copied_input_left);
}

void Clustering::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
