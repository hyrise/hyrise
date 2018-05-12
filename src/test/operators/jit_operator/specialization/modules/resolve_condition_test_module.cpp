#include <iostream>

struct SomeStruct {
  int32_t a;
  int32_t b;
};

void branch_instruction(const SomeStruct& s) {
  // A fully resolvable branch condition. All operands of the condition are either constant or loaded from the
  // SomeStruct parameter.
  // The expression also requires constant folding (i.e., once all operands have been replaced by constants, the
  // addition and comparison can be pre-computed to yield a final boolean result).
  if (s.a + s.b > 5) {
    std::cout << "> 5" << std::endl;
  } else {
    std::cout << "<= 5" << std::endl;
  }
}

void branch_instruction_failing(const SomeStruct& s, const int32_t unknown_value) {
  // This branch condition depends on unknown_value, whose value will not be known to the specialization engine.
  // Hence, this condition cannot be resolved.
  if (s.a + s.b > unknown_value) {
    std::cout << "> 5" << std::endl;
  } else {
    std::cout << "<= 5" << std::endl;
  }
}

void switch_instruction(const SomeStruct& s) {
  // Simple numeric switch condition that can be resolved.
  switch (s.a) {
    case 0:
      std::cout << "0" << std::endl;
      break;
    case 1:
      std::cout << "1" << std::endl;
      break;
    default:
      std::cout << "other" << std::endl;
      break;
  }
}

// Prevent LLVM from optimizing away any code. We use argc as a parameter so the compiler cannot
// guess its value and do crazy optimizations.
void foo(int32_t value) {
  branch_instruction(SomeStruct{value, value});
  branch_instruction_failing(SomeStruct{value, value}, value);
  switch_instruction(SomeStruct{value, value});
}
