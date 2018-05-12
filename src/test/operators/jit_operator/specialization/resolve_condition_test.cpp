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

class ResolveConditionTest : public BaseTest {
 protected:
  struct RuntimeContext {
    int32_t a;
    int32_t b;
  };

  void SetUp() override {
    _repository = std::make_shared<JitRepository>(
        std::string(&resolve_condition_test_module, resolve_condition_test_module_size));

    // Locate branch instruction in "void branch_instruction(RuntimeContext const&)" function
    _branch_instruction_fn = _repository->get_function("_Z18branch_instructionRK14RuntimeContext");
    _branch_instruction = llvm::dyn_cast<llvm::BranchInst>(_branch_instruction_fn->getEntryBlock().getTerminator());

    // Locate branch instruction in "void branch_instruction_failing(RuntimeContext const&, int)" function
    _branch_instruction_failing_fn = _repository->get_function("_Z26branch_instruction_failingRK14RuntimeContexti");
    _branch_instruction_failing =
        llvm::dyn_cast<llvm::BranchInst>(_branch_instruction_failing_fn->getEntryBlock().getTerminator());

    // Locate switch instruction in "void switch_instruction(RuntimeContext const&)" function
    _switch_instruction_fn = _repository->get_function("_Z18switch_instructionRK14RuntimeContext");
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

TEST_F(ResolveConditionTest, Test4) {
  {
    const auto resolved_condition = ResolveCondition(_branch_instruction->getCondition(), _specialization_context);
    ASSERT_EQ(resolved_condition, nullptr);
  }
  {
    const auto runtime_context = RuntimeContext{1, 2};
    _specialization_context.runtime_value_map.clear();
    _specialization_context.runtime_value_map[_branch_instruction_fn->arg_begin()] =
        std::make_shared<JitConstantRuntimePointer>(&runtime_context);
    const auto resolved_condition = ResolveCondition(_branch_instruction->getCondition(), _specialization_context);
    ASSERT_NE(resolved_condition, nullptr);
    ASSERT_EQ(resolved_condition->getType(), llvm::Type::getInt1Ty(*_repository->llvm_context()));
    const auto resolved_int_condition = llvm::dyn_cast<llvm::ConstantInt>(resolved_condition);
    ASSERT_TRUE(resolved_int_condition->isZero());
  }
  {
    const auto runtime_context = RuntimeContext{4, 5};
    _specialization_context.runtime_value_map.clear();
    _specialization_context.runtime_value_map[_branch_instruction_fn->arg_begin()] =
        std::make_shared<JitConstantRuntimePointer>(&runtime_context);
    const auto resolved_condition = ResolveCondition(_branch_instruction->getCondition(), _specialization_context);
    ASSERT_NE(resolved_condition, nullptr);
    ASSERT_EQ(resolved_condition->getType(), llvm::Type::getInt1Ty(*_repository->llvm_context()));
    const auto resolved_int_condition = llvm::dyn_cast<llvm::ConstantInt>(resolved_condition);
    ASSERT_TRUE(resolved_int_condition->isOne());
  }
}

TEST_F(ResolveConditionTest, Test5) {
  {
    const auto resolved_condition =
        ResolveCondition(_branch_instruction_failing->getCondition(), _specialization_context);
    ASSERT_EQ(resolved_condition, nullptr);
  }
  {
    const auto runtime_context = RuntimeContext{1, 2};
    _specialization_context.runtime_value_map.clear();
    _specialization_context.runtime_value_map[_branch_instruction_failing_fn->arg_begin()] =
        std::make_shared<JitConstantRuntimePointer>(&runtime_context);
    const auto resolved_condition =
        ResolveCondition(_branch_instruction_failing->getCondition(), _specialization_context);
    ASSERT_EQ(resolved_condition, nullptr);
  }
}

TEST_F(ResolveConditionTest, Test3) {
  {
    const auto resolved_condition = ResolveCondition(_switch_instruction->getCondition(), _specialization_context);
    ASSERT_EQ(resolved_condition, nullptr);
  }
  {
    const auto runtime_context = RuntimeContext{std::rand() % 100, std::rand() % 100};
    _specialization_context.runtime_value_map.clear();
    _specialization_context.runtime_value_map[_switch_instruction_fn->arg_begin()] =
        std::make_shared<JitConstantRuntimePointer>(&runtime_context);
    const auto resolved_condition = ResolveCondition(_switch_instruction->getCondition(), _specialization_context);
    ASSERT_NE(resolved_condition, nullptr);
    ASSERT_EQ(resolved_condition->getType(), llvm::Type::getInt32Ty(*_repository->llvm_context()));
    const auto resolved_int_condition = llvm::dyn_cast<llvm::ConstantInt>(resolved_condition);
    const auto resolved_condition_value =
        static_cast<int32_t>(resolved_int_condition->getLimitedValue(std::numeric_limits<uint64_t>::max()));
    ASSERT_EQ(resolved_condition_value, runtime_context.a);
  }
}

}  // namespace opossum
