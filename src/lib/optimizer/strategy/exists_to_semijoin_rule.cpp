#include "exists_to_semijoin_rule.hpp"

#include <unordered_map>


using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string ExistsToSemiJoin::name() const { return "Exists to Semijoin Rule"; }

bool ExistsToSemiJoin::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  std::cout << "it works!" << std::endl;
}


}  // namespace opossum
