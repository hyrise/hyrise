#include "null_tuning_operation.hpp"

namespace opossum {

void NullTuningOperation::execute() {}

void NullTuningOperation::print_on(std::ostream& output) const { output << "NullTuningOperation{}"; }

}  // namespace opossum
