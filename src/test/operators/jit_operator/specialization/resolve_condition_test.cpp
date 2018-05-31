#include <llvm/IR/IRBuilder.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/jit_repository.hpp"
#include "operators/jit_operator/specialization/llvm_extensions.hpp"

namespace opossum {

// These symbols provide access to the LLVM bitcode string embedded into the hyriseTest binary during compilation.
// The symbols are defined in an assembly file that is generated in EmbedLLVM.cmake and linked into the binary.
// Please refer to EmbedLLVM.cmake / src/test/CMakeLists.txt for further details of the bitcode generation and
// embedding process.
// The C++ code used to generate the bitcode for this test is located in
// src/test/operators/jit_operator/specialization/modules/resolve_condition_test_module.cpp.
extern char resolve_condition_test_module;
extern size_t resolve_condition_test_module_size;

/* This test checks the runtime information-based resolution mechanism for condition variables.
 * LLVM supports simple branch pruning during function inlining and cloning. Branch conditions and switch instructions
 * that can statically be reduced to a single case are used to prune other branches and reduce the amount of cloned
 * code. However, the default LLVM implementation only considers the static bitcode. Our extension of this mechanism
 * also uses runtime parameters given to the specialization engine.
 * The following test contains three test cases:
 * - a branch condition that can be resolved using runtime information
 * - a branch condition that cannot be resolved since some of the runtime information is kept from the specialization
 *   engine
 * - a switch condition that can be resolved using runtime information
 */
class ResolveConditionTest : public BaseTest {
 protected:
  struct SomeStruct {
    int32_t a;
    int32_t b;
  };

  void SetUp() override {
    _repository = std::make_shared<JitRepository>(
        std::string(&resolve_condition_test_module, resolve_condition_test_module_size));

    // Locate branch instruction in the "void branch_instruction(const SomeStruct&)" function
    _branch_instruction_fn = _repository->get_function("_Z18branch_instructionRK10SomeStruct");
    _branch_instruction = llvm::dyn_cast<llvm::BranchInst>(_branch_instruction_fn->getEntryBlock().getTerminator());

    // Locate branch instruction in "void branch_instruction_failing(const SomeStruct&, int32_t)" function
    _branch_instruction_failing_fn = _repository->get_function("_Z26branch_instruction_failingRK10SomeStructi");
    _branch_instruction_failing =
        llvm::dyn_cast<llvm::BranchInst>(_branch_instruction_failing_fn->getEntryBlock().getTerminator());

    // Locate switch instruction in "void switch_instruction(const SomeStruct&)" function
    _switch_instruction_fn = _repository->get_function("_Z18switch_instructionRK10SomeStruct");
    _switch_instruction = llvm::dyn_cast<llvm::SwitchInst>(_switch_instruction_fn->getEntryBlock().getTerminator());

    _specialization_context.module = _repository->module();
  }

  std::shared_ptr<JitRepository> _repository;
  SpecializationContext _specialization_context;
  llvm::Function* _branch_instruction_fn;
  llvm::Function* _branch_instruction_failing_fn;
  llvm::Function* _switch_instruction_fn;
  llvm::BranchInst* _branch_instruction;
  llvm::BranchInst* _branch_instruction_failing;
  llvm::SwitchInst* _switch_instruction;
};

TEST_F(ResolveConditionTest, BranchConditionsAreResolved) {
  {
    // No runtime information is set in the specialization context. Thus the resolution of the condition must fail.
    const auto resolved_condition = ResolveCondition(_branch_instruction->getCondition(), _specialization_context);
    ASSERT_EQ(resolved_condition, nullptr);
  }
  {
    // Create runtime information (i.e., an instance of the SomeStruct parameter).
    const auto runtime_parameter = SomeStruct{1, 2};
    // Initialize the first function argument to this value in the specialization context. This make the value
    // accessible to the specialization engine.
    _specialization_context.runtime_value_map.clear();
    _specialization_context.runtime_value_map[_branch_instruction_fn->arg_begin()] =
        std::make_shared<JitConstantRuntimePointer>(&runtime_parameter);

    const auto resolved_condition = ResolveCondition(_branch_instruction->getCondition(), _specialization_context);

    // The condition should now be replaced by a "false" constant (which is represented as a 1-bit integer in LLVM),
    // since the resolved expression "s.a + s.b > 5" = "1 + 2 > 5" is false.
    ASSERT_NE(resolved_condition, nullptr);
    ASSERT_EQ(resolved_condition->getType(), llvm::Type::getInt1Ty(*_repository->llvm_context()));
    const auto resolved_int_condition = llvm::dyn_cast<llvm::ConstantInt>(resolved_condition);
    ASSERT_TRUE(resolved_int_condition->isZero());
  }
  {
    // We repeat the above procedure with a value that will cause the condition expression to evaluate to "true".
    const auto runtime_parameter = SomeStruct{4, 5};
    _specialization_context.runtime_value_map.clear();
    _specialization_context.runtime_value_map[_branch_instruction_fn->arg_begin()] =
        std::make_shared<JitConstantRuntimePointer>(&runtime_parameter);

    const auto resolved_condition = ResolveCondition(_branch_instruction->getCondition(), _specialization_context);
    ASSERT_NE(resolved_condition, nullptr);
    ASSERT_EQ(resolved_condition->getType(), llvm::Type::getInt1Ty(*_repository->llvm_context()));
    const auto resolved_int_condition = llvm::dyn_cast<llvm::ConstantInt>(resolved_condition);
    ASSERT_TRUE(resolved_int_condition->isOne());
  }
}

TEST_F(ResolveConditionTest, UnresolvableBranchConditionsAreNotResolved) {
  // The condition expression contains an operand that is not given to the specialization engine.
  // Thus the resolution pof the entire expression fails.
  const auto runtime_parameter = SomeStruct{1, 2};
  _specialization_context.runtime_value_map.clear();
  _specialization_context.runtime_value_map[_branch_instruction_failing_fn->arg_begin()] =
      std::make_shared<JitConstantRuntimePointer>(&runtime_parameter);

  const auto resolved_condition =
      ResolveCondition(_branch_instruction_failing->getCondition(), _specialization_context);
  ASSERT_EQ(resolved_condition, nullptr);
}

TEST_F(ResolveConditionTest, SwitchConditionsAreResolved) {
  {
    // No runtime information is set in the specialization context. Thus the resolution of the condition must fail.
    const auto resolved_condition = ResolveCondition(_switch_instruction->getCondition(), _specialization_context);
    ASSERT_EQ(resolved_condition, nullptr);
  }
  {
    // Create runtime information (i.e., an instance of the SomeStruct parameter).
    const auto runtime_parameter = SomeStruct{std::rand() % 100, std::rand() % 100};

    // Initialize the first function argument to this value in the specialization context. This make the value
    // accessible to the specialization engine.
    _specialization_context.runtime_value_map.clear();
    _specialization_context.runtime_value_map[_switch_instruction_fn->arg_begin()] =
        std::make_shared<JitConstantRuntimePointer>(&runtime_parameter);

    const auto resolved_condition = ResolveCondition(_switch_instruction->getCondition(), _specialization_context);

    // The condition should now be replaced by an integer constant with the correct value from the runtime parameter.
    ASSERT_NE(resolved_condition, nullptr);
    ASSERT_EQ(resolved_condition->getType(), llvm::Type::getInt32Ty(*_repository->llvm_context()));
    const auto resolved_int_condition = llvm::dyn_cast<llvm::ConstantInt>(resolved_condition);
    const auto resolved_condition_value =
        static_cast<int32_t>(resolved_int_condition->getLimitedValue(std::numeric_limits<uint64_t>::max()));
    ASSERT_EQ(resolved_condition_value, runtime_parameter.a);
  }
}

}  // namespace opossum
