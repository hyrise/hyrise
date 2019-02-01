#include <iostream>

#include "types.hpp"
#include "version.hpp"

using namespace opossum;  // NOLINT

int main() {
  std::cout << "Hello worlssd!! " << GIT_HEAD_SHA1 << " - " << GIT_IS_DIRTY << std::endl;
  return 0;
}
