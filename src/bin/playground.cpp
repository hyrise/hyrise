#include <iostream>

#include "../lib/all_parameter_variant.hpp"
#include "../lib/types.hpp"

using namespace opossum;

ColumnID foo();

int main() {
  AllParameterVariant p = foo();
  std::cout << boost::get<ColumnID>(p) << std::endl;
  return 0;
}
