#pragma once

#include <llvm/Transforms/Utils/Cloning.h>

#include <unordered_map>

#include "jit_runtime_pointer.hpp"

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
const std::shared_ptr<const JitRuntimePointer>& GetRuntimePointerForValue(const llvm::Value* value,
                                                                          SpecializationContext& context);

// Tries to determine the runtime value for a given condition variable of a switch or branch instruction.
// Operands of the value are recursively resolved until they are either constants or load instructions.
// If all loaded operands can be resolved to runtime values the condition can be reduced to a constant value by
// performing constant folding
llvm::Constant* ResolveCondition(llvm::Value* Value, SpecializationContext& Context);

}  // namespace opossum
