#include "null_operation.hpp"

namespace opossum {

void NullOperation::execute() {}

void NullOperation::print_on(std::ostream& output) const { output << "NullOperation{}"; }

}  // namespace opossum
