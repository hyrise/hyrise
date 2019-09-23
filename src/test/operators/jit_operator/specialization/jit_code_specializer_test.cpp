#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>

#include "base_test.hpp"
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

/* The following test cases test individual operations of the code specialization engine.
 * - resolution and inlining of virtual functions
 * - replacement of load instructions by constants
 * - unrolling of loops to facilitate further specialization opportunities
 */
class JitCodeSpecializerTest : public BaseTest {
 protected:
  void SetUp() override {
    _repository = std::make_shared<JitRepository>(
        std::string(&jit_code_specializer_test_module, jit_code_specializer_test_module_size));
  }

  // Counts the number of instructions of a specific type in a function
  template <typename T>
  uint32_t count_instructions(const llvm::Function& function) {
    auto counter = 0u;
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
    // We "specialize" the function without providing any runtime information
    JitCodeSpecializer code_specializer(_repository);
    const auto specialized_module =
        code_specializer.specialize_function(virtual_call_fn_symbol, std::make_shared<JitRuntimePointer>());

    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_virtual_call_fn = specialized_module->begin();

    // There is still one call instruction (the virtual call) in the specialized module
    const auto num_call_instructions = count_instructions<llvm::CallInst>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_call_instructions, 1u);
  }
  {
    // Create an operation instance
    Decrement dec;

    JitCodeSpecializer code_specializer(_repository);
    // Pass a pointer of the instance to the specialization engine for specializing the module.
    const auto specialized_module =
        code_specializer.specialize_function(virtual_call_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&dec));

    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_virtual_call_fn = specialized_module->begin();

    // The specialized module contains NO call instruction, since the virtual call was resolved and inlined.
    const auto num_call_instructions = count_instructions<llvm::CallInst>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_call_instructions, 0u);

    // The specialized and optimized module only contains two instructions:
    // define i32 @_ZN7opossum12virtual_callERKNS_17AbstractOperationEi_(...) {
    //   %3 = add nsw i32 %1, -1
    //   ret i32 %3
    // }
    const auto num_instructions = count_instructions<llvm::Instruction>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_instructions, 2u);

    // When fully compiling and executing the specialized function, it still produces the correct result.
    const auto compiled_virtual_call_fn =
        code_specializer.specialize_and_compile_function<int32_t(const AbstractOperation&, int32_t)>(
            virtual_call_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&dec));
    const auto value = 123;
    ASSERT_EQ(compiled_virtual_call_fn(dec, value), value - 1);
  }
  {
    // We repeat the above test with a different type of operation.
    Increment inc;
    JitCodeSpecializer code_specializer(_repository);
    const auto specialized_module =
        code_specializer.specialize_function(virtual_call_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&inc));

    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_virtual_call_fn = specialized_module->begin();

    const auto num_call_instructions = count_instructions<llvm::CallInst>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_call_instructions, 0u);

    // The specialized and optimized module only contains two instructions:
    // define i32 @_ZN7opossum12virtual_callERKNS_17AbstractOperationEi_(...) {
    //   %3 = add nsw i32 %1, 1
    //   ret i32 %3
    // }
    const auto num_instructions = count_instructions<llvm::Instruction>(*specialized_virtual_call_fn);
    ASSERT_EQ(num_instructions, 2u);

    // When fully compiling and executing the specialized function, it still produces the correct result.
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
    // We "specialize" the function without providing any runtime information
    JitCodeSpecializer code_specializer(_repository);
    const auto specialized_module =
        code_specializer.specialize_function(load_replacement_fn_symbol, std::make_shared<JitRuntimePointer>());

    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_load_replacement_fn = specialized_module->begin();

    // There is still one load instruction (to load the _n member from the IncrementByN instance) in the module
    const auto num_load_instructions = count_instructions<llvm::LoadInst>(*specialized_load_replacement_fn);
    ASSERT_EQ(num_load_instructions, 1u);
  }
  {
    // Create the operation instance
    IncrementByN inc_by_5(5);

    JitCodeSpecializer code_specializer(_repository);
    // Pass a pointer of the instance to the specialization engine for specializing the module.
    const auto specialized_module = code_specializer.specialize_function(
        load_replacement_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&inc_by_5));

    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_load_replacement_fn = specialized_module->begin();

    // The load instruction has been replaced by a constant value that has been fetched from the instance by the
    // specialization engine
    const auto num_load_instructions = count_instructions<llvm::LoadInst>(*specialized_load_replacement_fn);
    ASSERT_EQ(num_load_instructions, 0u);

    // The specialized and optimized module only contains two instructions:
    // define i32 @_ZN7opossum16load_replacementERKNS_12IncrementByNEi_(...) {
    //   %3 = add nsw i32 %1, 5
    //   ret i32 %3
    // }
    const auto num_instructions = count_instructions<llvm::Instruction>(*specialized_load_replacement_fn);
    ASSERT_EQ(num_instructions, 2u);

    // When fully compiling and executing the specialized function, it still produces the correct result.
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
      "_ZN7opossum25apply_multiple_operationsERKNS_18MultipleOperationsEi";

  // Create three operations that should be executed consecutively
  const auto multiple_operations = MultipleOperations(std::vector<std::shared_ptr<const AbstractOperation>>({
      std::make_shared<IncrementByN>(10),
      std::make_shared<Decrement>(),
      std::make_shared<Decrement>(),
  }));

  {
    JitCodeSpecializer code_specializer(_repository);
    // Run the code specialization WITHOUT loop unrolling
    const auto specialized_module = code_specializer.specialize_function(
        apply_multiple_operations_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&multiple_operations), false);

    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_apply_multiple_operations_fn = specialized_module->begin();

    // The loop has not been unrolled and there is still control flow (i.e., multiple basic blocks and phi nodes), and
    // function calls in the function.
    const auto num_blocks = specialized_apply_multiple_operations_fn->size();
    ASSERT_GT(num_blocks, 1u);
    const auto num_phi_nodes = count_instructions<llvm::PHINode>(*specialized_apply_multiple_operations_fn);
    ASSERT_GT(num_phi_nodes, 1u);
    const auto num_call_instructions = count_instructions<llvm::CallInst>(*specialized_apply_multiple_operations_fn);
    ASSERT_GE(num_call_instructions, 1u);
  }
  {
    JitCodeSpecializer code_specializer(_repository);
    const auto specialized_module = code_specializer.specialize_function(
        apply_multiple_operations_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&multiple_operations), true);

    ASSERT_EQ(specialized_module->size(), 1u);
    const auto specialized_apply_multiple_operations_fn = specialized_module->begin();

    // The specialized module contains only one basic block ...
    const auto num_blocks = specialized_apply_multiple_operations_fn->size();
    ASSERT_EQ(num_blocks, 1u);

    // ... no control flow and thus no loop ...
    const auto num_phi_nodes = count_instructions<llvm::PHINode>(*specialized_apply_multiple_operations_fn);
    ASSERT_EQ(num_phi_nodes, 0u);

    // ... no function call ...
    const auto num_call_instructions = count_instructions<llvm::CallInst>(*specialized_apply_multiple_operations_fn);
    ASSERT_EQ(num_call_instructions, 0u);

    // ... and only two instructions:
    // define i32 @_ZN7opossum25apply_multiple_operationsERKNSt3__16vectorINS0_10shared_ptrIKNS_17AbstractOper...(...) {
    //   %2 = add nsw i32 %1, 8
    //   ret i32 %2
    // }
    const auto num_instructions = count_instructions<llvm::Instruction>(*specialized_apply_multiple_operations_fn);
    ASSERT_EQ(num_instructions, 2u);

    // When fully compiling and executing the specialized function, it still produces the correct result.
    const auto compiled_apply_multiple_operations_fn =
        code_specializer.specialize_and_compile_function<int32_t(const MultipleOperations&, int32_t)>(
            apply_multiple_operations_fn_symbol, std::make_shared<JitConstantRuntimePointer>(&multiple_operations));
    const auto value = 123;
    ASSERT_EQ(compiled_apply_multiple_operations_fn(multiple_operations, value), value + 10 - 1 - 1);
  }
}

}  // namespace opossum
