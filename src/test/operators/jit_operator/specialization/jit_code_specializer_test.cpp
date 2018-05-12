#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/jit_code_specializer.hpp"
#include "operators/jit_operator/specialization/modules/jit_code_specializer_test_module.hpp"

namespace opossum {

// These symbols provide access to the LLVM bitcode string embedded into the hyriseTest binary during compilation.
// The symbols are defined in an assembly file that is generated in EmbedLLVM.cmake and linked into the binary.
// Please refer to EmbedLLVM.cmake / src/test/CMakeLists.txt for further details of the bitcode generation and
// embedding process.
// The C++ code used to generate the bitcode for this test is located in
// src/test/operators/jit_operator/specialization/modules/jit_code_specializer_test_module.cpp.
extern char jit_code_specializer_test_module;
extern size_t jit_code_specializer_test_module_size;

class JitCodeSpecializerTest : public BaseTest {
 protected:
  void SetUp() override {
    _repository = std::make_shared<JitRepository>(
        std::string(&jit_code_specializer_test_module, jit_code_specializer_test_module_size));
  }

  template <typename T>
  uint32_t count_instructions(const llvm::Function& function) {
    auto counter = 0;
    for (const auto& block : function) {
      for (const auto& inst : block) {
        if (llvm::isa<T>(inst)) {
          counter++;
        }
      }
    }
    return counter;
  }

  std::shared_ptr<JitRepository> _repository;
};

TEST_F(JitCodeSpecializerTest, InlinesVirtualCalls) {
  // int32_t opossum::virtual_call(const opossum::AbstractOperation&, const int32_t);
  std::string virtual_call_fn_symbol = "_ZN7opossum12virtual_callERKNS_17AbstractOperationEi";

  {
    JitCodeSpecializer code_specializer(*_repository);
    const auto specialized_module =
        code_specializer.specialize_function(virtual_call_fn_symbol, std::make_shared<JitRuntimePointer>());
    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_virtual_call_fn = specialized_module->begin();
    const auto num_call_instructions = count_instructions<llvm::CallInst>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_call_instructions, 1u);
  }
  {
    JitCodeSpecializer code_specializer(*_repository);
    Decrement dec;
    const auto specialized_module =
        code_specializer.specialize_function(virtual_call_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&dec));
    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_virtual_call_fn = specialized_module->begin();
    const auto num_call_instructions = count_instructions<llvm::CallInst>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_call_instructions, 0u);
    const auto num_instructions = count_instructions<llvm::Instruction>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_instructions, 2u);

    const auto compiled_virtual_call_fn =
        code_specializer.specialize_and_compile_function<int32_t(const AbstractOperation&, int32_t)>(
            virtual_call_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&dec));
    const auto value = 123;
    ASSERT_EQ(compiled_virtual_call_fn(dec, value), value - 1);
  }
  {
    JitCodeSpecializer code_specializer(*_repository);
    Increment inc;
    const auto specialized_module =
        code_specializer.specialize_function(virtual_call_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&inc));
    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_virtual_call_fn = specialized_module->begin();
    const auto num_call_instructions = count_instructions<llvm::CallInst>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_call_instructions, 0u);
    const auto num_instructions = count_instructions<llvm::Instruction>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_instructions, 2u);

    const auto compiled_virtual_call_fn =
        code_specializer.specialize_and_compile_function<int32_t(const AbstractOperation&, int32_t)>(
            virtual_call_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&inc));
    const auto value = 123;
    ASSERT_EQ(compiled_virtual_call_fn(inc, value), value + 1);
  }
}

TEST_F(JitCodeSpecializerTest, ReplacesLoadInstructions) {
  // int32_t opossum::load_replacement(const opossum::IncrementByN&, const int32_t);
  std::string load_replacement_fn_symbol = "_ZN7opossum16load_replacementERKNS_12IncrementByNEi";

  {
    JitCodeSpecializer code_specializer(*_repository);
    const auto specialized_module =
        code_specializer.specialize_function(load_replacement_fn_symbol, std::make_shared<JitRuntimePointer>());
    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_load_replacement_fn = specialized_module->begin();
    const auto num_load_instructions = count_instructions<llvm::LoadInst>(*specialized_load_replacement_fn);
    ASSERT_EQ(num_load_instructions, 1u);
  }
  {
    JitCodeSpecializer code_specializer(*_repository);
    IncrementByN inc_by_5(5);
    const auto specialized_module = code_specializer.specialize_function(
        load_replacement_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&inc_by_5));
    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_load_replacement_fn = specialized_module->begin();
    const auto num_load_instructions = count_instructions<llvm::LoadInst>(*specialized_load_replacement_fn);
    ASSERT_EQ(num_load_instructions, 0u);
    const auto num_instructions = count_instructions<llvm::Instruction>(*specialized_load_replacement_fn);
    ASSERT_EQ(num_instructions, 2u);

    const auto compiled_load_replacement_fn =
        code_specializer.specialize_and_compile_function<int32_t(const IncrementByN&, int32_t)>(
            load_replacement_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&inc_by_5));
    const auto value = 123;
    ASSERT_EQ(compiled_load_replacement_fn(inc_by_5, value), value + 5);
  }
}

TEST_F(JitCodeSpecializerTest, UnrollsLoops) {
  // int32_t opossum::apply_multiple_operations(const std::vector<std::shared_ptr<const AbstractOperation>>&, int32_t);
  std::string apply_multiple_operations_fn_symbol =
      "_ZN7opossum25apply_multiple_operationsERKNSt3__16vectorINS0_10shared_ptrIKNS_17AbstractOperationEEENS0_"
      "9allocatorIS5_EEEEi";

  {
    JitCodeSpecializer code_specializer(*_repository);
    const auto operations = std::vector<std::shared_ptr<const AbstractOperation>>{
        std::make_shared<IncrementByN>(10),
        std::make_shared<Decrement>(),
        std::make_shared<Decrement>(),
    };
    const auto specialized_module = code_specializer.specialize_function(
        apply_multiple_operations_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&operations), false);
    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_apply_multiple_operations_fn = specialized_module->begin();
    const auto num_blocks = specialized_apply_multiple_operations_fn->size();
    ASSERT_GT(num_blocks, 1u);
    const auto num_phi_nodes = count_instructions<llvm::PHINode>(*specialized_apply_multiple_operations_fn);
    ASSERT_GT(num_phi_nodes, 1u);
  }
  {
    JitCodeSpecializer code_specializer(*_repository);
    const auto operations = std::vector<std::shared_ptr<const AbstractOperation>>{
        std::make_shared<IncrementByN>(10),
        std::make_shared<Decrement>(),
        std::make_shared<Decrement>(),
    };
    const auto specialized_module = code_specializer.specialize_function(
        apply_multiple_operations_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&operations), true);
    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_apply_multiple_operations_fn = specialized_module->begin();
    const auto num_blocks = specialized_apply_multiple_operations_fn->size();
    ASSERT_EQ(num_blocks, 1u);
    const auto num_phi_nodes = count_instructions<llvm::PHINode>(*specialized_apply_multiple_operations_fn);
    ASSERT_EQ(num_phi_nodes, 0u);
    const auto num_instructions = count_instructions<llvm::Instruction>(*specialized_apply_multiple_operations_fn);
    ASSERT_EQ(num_instructions, 2u);

    const auto compiled_apply_multiple_operations_fn = code_specializer.specialize_and_compile_function<int32_t(
        const std::vector<std::shared_ptr<const AbstractOperation>>&, int32_t)>(
        apply_multiple_operations_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&operations));
    const auto value = 123;
    ASSERT_EQ(compiled_apply_multiple_operations_fn(operations, value), value + 10 - 1 - 1);
  }
}

}  // namespace opossum
