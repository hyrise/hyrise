#include "jit_code_specializer_test_module.hpp"

#include <iostream>

namespace opossum {

__attribute__((noinline)) int32_t virtual_call(const AbstractOperation& op, int32_t value) { return op.apply(value); }

__attribute__((noinline)) int32_t load_replacement(const IncrementByN& op, int32_t value) { return op.apply(value); }

__attribute__((noinline)) int32_t apply_multiple_operations(
    const std::vector<std::shared_ptr<const AbstractOperation>>& ops, int32_t value) {
  auto current_value = value;
  for (auto i = 0u; i < ops.size(); ++i) {
    current_value = ops[i]->apply(current_value);
  }
  return current_value;
}

// Prevent LLVM from optimizing away most of the code during bitcode generation
void foo(int32_t value) {
  Increment inc;
  Decrement dec;

  if (value > 0) {
    std::cout << virtual_call(inc, value) << std::endl;
  } else {
    std::cout << virtual_call(dec, value) << std::endl;
  }

  IncrementByN inc_by_n(value);
  std::cout << load_replacement(inc_by_n, value) << std::endl;

  apply_multiple_operations({std::make_shared<Increment>(), std::make_shared<Increment>()}, value);
}

}  // namespace opossum
