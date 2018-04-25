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
 * 2)
 *
 * We use a SpecializationContext structure to pass additional data through these functions.
 */

namespace opossum {

struct SpecializationContext {
  std::string root_function_name;
  std::unique_ptr<llvm::Module> module;
  llvm::Function* root_function;
  llvm::ValueToValueMapTy llvm_value_map;
  std::unordered_map<const llvm::Value*, JitRuntimePointer::Ptr> runtime_value_map;
};

bool InlineFunction(llvm::CallSite CS, llvm::InlineFunctionInfo& IFI, llvm::AAResults* CalleeAAR, bool InsertLifetime,
                    SpecializationContext& Context);

void CloneAndPruneFunctionInto(llvm::Function* NewFunc, const llvm::Function* OldFunc, llvm::ValueToValueMapTy& VMap,
                               bool ModuleLevelChanges, llvm::SmallVectorImpl<llvm::ReturnInst*>& Returns,
                               const char* NameSuffix, llvm::ClonedCodeInfo* CodeInfo, llvm::Instruction* TheCall,
                               SpecializationContext& Context);

void CloneAndPruneIntoFromInst(llvm::Function* NewFunc, const llvm::Function* OldFunc,
                               const llvm::Instruction* StartingInst, llvm::ValueToValueMapTy& VMap,
                               bool ModuleLevelChanges, llvm::SmallVectorImpl<llvm::ReturnInst*>& Returns,
                               const char* NameSuffix, llvm::ClonedCodeInfo* CodeInfo, SpecializationContext& Context);


llvm::Constant* ResolveCondition(llvm::Value* Value, SpecializationContext& Context);

}  // namespace opossum
