#pragma once

#include <llvm/Transforms/Utils/Cloning.h>

#include <unordered_map>

#include "jit_runtime_pointer.hpp"

/* The code specialization uses cloning and inlining functionality from LLVM, but requires two small modifications to
 * the implementation provided by LLVM:
 * 1) When cloning functions, LLVM maintains a map of corresponding values in the old and new function. This map is used
 * to substitute instruction operands during cloning.
 * When cloning functions across module boundaries this map needs to be initialized with all global objects to ensure
 * references to such objects in the cloned function refer to the correct module.
 * While the CloneFunction API accepts an initialized method as a parameter, the InlineFunction (which uses
 * CloneFunction internally) does not expose this parameter. Our modified implementation adds this parameter and passes
 * it through to the CloneFunction function.
 * 2) LLVM supports pruning of basic blocks during function cloning. The conditions of switch and conditional branch
 * instructions are evaluated. In case of a constant conditions, the branch target can be pre-computed and the branch
 * instruction be eliminated. However, LLVM only performs a static analysis of the LLVM bitcode. We thus extended the
 * pruning mechanism to also consider runtime information.
 *
 * Two LLVM files (CloneFunction.cpp and InlineFunction.cpp) have been copied from the LLVM repository located at
 * https://github.com/llvm-mirror/llvm/tree/release_50 to the llvm/ subdirectory.
 * The changes made to these files are documented by the corresponding diff files.
 * The files are excluded from automatic formatting and linting to keep them as close to the original source code as
 * possible.
 * The changes to these files are kept to a minimum. Most of the extensions made to LLVM can be found in
 * llvm_extensions.cpp.
 */

namespace opossum {

// Data necessary to specialize the current module. This data is kept in a separate structure outside the
// JitCodeSpecializer so it can be passed to the modified LLVM functions.
struct SpecializationContext {
  std::string root_function_name;
  std::unique_ptr<llvm::Module> module;
  llvm::Function* root_function;
  // Maps values from the source function to the target function during function cloning
  llvm::ValueToValueMapTy llvm_value_map;
  // Maps LLVM values to runtime memory locations during code specialization
  std::unordered_map<const llvm::Value*, std::shared_ptr<const JitRuntimePointer>> runtime_value_map;
};

// Inlines a function call
// This method extends the corresponding function in the LLVM framework
bool InlineFunction(llvm::CallSite CS, llvm::InlineFunctionInfo& IFI, llvm::AAResults* CalleeAAR, bool InsertLifetime,
                    SpecializationContext& Context);

// Clones a function body into a new empty function while pruning switch and branch instructions
// This method extends the corresponding function in the LLVM framework
void CloneAndPruneFunctionInto(llvm::Function* NewFunc, const llvm::Function* OldFunc, llvm::ValueToValueMapTy& VMap,
                               bool ModuleLevelChanges, llvm::SmallVectorImpl<llvm::ReturnInst*>& Returns,
                               const char* NameSuffix, llvm::ClonedCodeInfo* CodeInfo, llvm::Instruction* TheCall,
                               SpecializationContext& Context);

// Clones a function body into a new empty function while pruning switch and branch instructions
// This method extends the corresponding function in the LLVM framework
void CloneAndPruneIntoFromInst(llvm::Function* NewFunc, const llvm::Function* OldFunc,
                               const llvm::Instruction* StartingInst, llvm::ValueToValueMapTy& VMap,
                               bool ModuleLevelChanges, llvm::SmallVectorImpl<llvm::ReturnInst*>& Returns,
                               const char* NameSuffix, llvm::ClonedCodeInfo* CodeInfo, SpecializationContext& Context);

// Gets a runtime memory location (is possible) for an LLVM bitcode value
const std::shared_ptr<const JitRuntimePointer>& GetRuntimePointerForValue(const llvm::Value* value, SpecializationContext& context);

// Tries to determine the runtime value for a given condition variable of a switch or branch instruction.
// Operands of the value are recursively resolved until they are either constants or load instructions.
// If all loaded operands can be resolved to runtime values the condition can be reduced to a constant value by
// performing constant folding
llvm::Constant* ResolveCondition(llvm::Value* Value, SpecializationContext& Context);

}  // namespace opossum
