#include <iostream>

struct RuntimeContext {
  int32_t a;
  int32_t b;
};

void branch_instruction(const RuntimeContext& context) {
  // Complex resolvable branch condition with constant folding opportunities
  if (context.a + context.b > 5) {
    std::cout << "> 5" << std::endl;
  } else {
    std::cout << "<= 5" << std::endl;
  }
}

void branch_instruction_failing(const RuntimeContext& context, const int32_t unknown_value) {
  // This condition depends on unknown_value, which is not part of the runtime context and
  // thus not known by the specialization engine. Hence, this condition cannot be resolved.
  if (context.a + context.b > unknown_value) {
    std::cout << "> 5" << std::endl;
  } else {
    std::cout << "<= 5" << std::endl;
  }
}

void switch_instruction(const RuntimeContext& context) {
  // Simple numeric switch condition that can be resolved from the runtime context
  switch (context.a) {
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
int main(int argc, char* argv[]) {
  branch_instruction(RuntimeContext{argc, argc});
  branch_instruction_failing(RuntimeContext{argc, argc}, argc);
  switch_instruction(RuntimeContext{argc, argc});
}
