#pragma once

#include <unordered_map>

#include <llvm/Transforms/Utils/Cloning.h>

#include "../jit_runtime_pointer.hpp"

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

void CloneAndPruneIntoFromInst(llvm::Function* NewFunc, const llvm::Function* OldFunc, const llvm::Instruction* StartingInst,
                               llvm::ValueToValueMapTy& VMap, bool ModuleLevelChanges,
                               llvm::SmallVectorImpl<llvm::ReturnInst*>& Returns, const char* NameSuffix,
                               llvm::ClonedCodeInfo* CodeInfo, SpecializationContext& Context);

llvm::Constant* ResolveCondition(llvm::Value* Value, SpecializationContext& Context);

}  // namespace opossum
