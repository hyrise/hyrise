#include "llvm_extensions.hpp"

#include <llvm/Analysis/ConstantFolding.h>
#include <llvm/IR/Constants.h>

#include <unordered_set>

namespace opossum {

const std::shared_ptr<const JitRuntimePointer>& GetRuntimePointerForValue(const llvm::Value* value,
                                                                          SpecializationContext& context) {
  // If the value exists in the value map, use the mapped value (i.e., the value in the cloned function) for the lookup
  auto mapped_value = value;
  if (context.llvm_value_map.count(value)) {
    mapped_value = context.llvm_value_map.lookup(value);
  }

  // Try serving results from cache
  if (context.runtime_value_map.count(mapped_value)) {
    return context.runtime_value_map[mapped_value];
  }

  if (const auto constant_expr = llvm::dyn_cast<llvm::ConstantExpr>(mapped_value)) {
    // Case 1: The value is an IntToPtr instruction embedded in a ConstExpr:
    // Take the constant integer from the instruction (operand 0), and convert it to a constant runtime pointer
    if (constant_expr->getType()->isPointerTy()) {
      if (constant_expr->getOpcode() == llvm::Instruction::IntToPtr) {
        if (const auto address = llvm::dyn_cast<llvm::ConstantInt>(constant_expr->getOperand(0))) {
          context.runtime_value_map[mapped_value] =
              std::make_shared<JitConstantRuntimePointer>(address->getValue().getLimitedValue());
        }
      }
    }
  } else if (const auto load_inst = llvm::dyn_cast<llvm::LoadInst>(mapped_value)) {
    if (load_inst->getType()->isPointerTy()) {
      // Case 2: The value is a Load instruction:
      // Try to resolve the pointer this instruction loads its value from and create a runtime pointer object from it
      if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
              GetRuntimePointerForValue(load_inst->getPointerOperand(), context))) {
        context.runtime_value_map[mapped_value] = std::make_shared<JitDereferencedRuntimePointer>(base);
      }
    }
  } else if (const auto gep_inst = llvm::dyn_cast<llvm::GetElementPtrInst>(mapped_value)) {
    // Case 3: The value is a GetElementPtr instruction:
    // Try to get 1) the constant offset this instructions applies, and 2) the pointer the offset is applied to
    // If both values can be obtained create a corresponding runtime pointer object
    llvm::APInt offset(64, 0);
    if (gep_inst->accumulateConstantOffset(context.module->getDataLayout(), offset)) {
      if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
              GetRuntimePointerForValue(gep_inst->getPointerOperand(), context))) {
        context.runtime_value_map[mapped_value] =
            std::make_shared<JitOffsetRuntimePointer>(base, offset.getLimitedValue());
      }
    }
  } else if (const auto bitcast_inst = llvm::dyn_cast<llvm::BitCastInst>(mapped_value)) {
    // Case 3: The value is a BitCast instruction:
    // BitCast instructions only change the type, but never the value of a pointer value.
    // We can thus ignore the type-cast and simply continue with the pointer operand
    if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
            GetRuntimePointerForValue(bitcast_inst->getOperand(0), context))) {
      context.runtime_value_map[mapped_value] = std::make_shared<JitOffsetRuntimePointer>(base, 0L);
    }
  }

  // If we could not resolve the value until here we mark it as unresolvable in the runtime value map to prevent further
  // resolution attempts
  if (!context.runtime_value_map.count(mapped_value)) {
    context.runtime_value_map[mapped_value] = std::make_shared<JitRuntimePointer>();
  }

  return context.runtime_value_map[mapped_value];
}

// Preforms constant folding (i.e., recursively simplifies operations on constant values by simulating the operation and
// substituting the computation with the constant result)
llvm::Constant* ConstantFoldInstruction(llvm::Instruction* inst, llvm::ArrayRef<llvm::Constant*> operands,
                                        const llvm::DataLayout& data_layout,
                                        const llvm::TargetLibraryInfo* target_library_info) {
  // PHI nodes cannot be constant folded
  if (auto* phi_node = llvm::dyn_cast<llvm::PHINode>(inst)) {
    return nullptr;
  }

  // Load Instructions cannot be constant folded
  if (const auto* load_inst = llvm::dyn_cast<llvm::LoadInst>(inst)) {
    return nullptr;
  }

  // Individually handle instructions that are not captured by the generic llvm::ConstantFoldInstOperands function
  if (const auto* compare_inst = llvm::dyn_cast<llvm::CmpInst>(inst)) {
    return llvm::ConstantFoldCompareInstOperands(compare_inst->getPredicate(), operands[0], operands[1], data_layout,
                                                 target_library_info);
  } else if (auto* insert_value_inst = llvm::dyn_cast<llvm::InsertValueInst>(inst)) {
    return llvm::ConstantExpr::getInsertValue(llvm::cast<llvm::Constant>(insert_value_inst->getAggregateOperand()),
                                              llvm::cast<llvm::Constant>(insert_value_inst->getInsertedValueOperand()),
                                              insert_value_inst->getIndices());
  } else if (auto* extract_value_inst = llvm::dyn_cast<llvm::ExtractValueInst>(inst)) {
    return llvm::ConstantExpr::getExtractValue(llvm::cast<llvm::Constant>(extract_value_inst->getAggregateOperand()),
                                               extract_value_inst->getIndices());
  }

  // Forward all other cases to LLVM
  return llvm::ConstantFoldInstOperands(inst, operands, data_layout, target_library_info);
}

llvm::Constant* ResolveConditionRec(llvm::Value* value, SpecializationContext& context,
                                    std::unordered_set<llvm::Value*>& failed) {
  // If resolving the value failed already we fail early here
  if (failed.count(value)) {
    return nullptr;
  }

  // If the value is already a constant simply return the same value
  if (auto const_value = llvm::dyn_cast<llvm::Constant>(value)) {
    return const_value;
  }

  if (auto load = llvm::dyn_cast<llvm::LoadInst>(value)) {
    const auto runtime_pointer = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
        GetRuntimePointerForValue(load->getPointerOperand(), context));
    if (runtime_pointer && runtime_pointer->is_valid() && load->getType()->isIntegerTy()) {
      const auto address = runtime_pointer->address();
      const auto int_type = llvm::dyn_cast<llvm::IntegerType>(load->getType());
      const auto bit_width = int_type->getIntegerBitWidth();
      const auto mask = bit_width == 64 ? 0xffffffffffffffff : (static_cast<uint64_t>(1) << bit_width) - 1;
      const auto value = *reinterpret_cast<uint64_t*>(address) & mask;
      return llvm::ConstantInt::get(int_type, value, int_type->getSignBit() > 0);
    }
  } else if (auto inst = llvm::dyn_cast<llvm::Instruction>(value)) {
    failed.insert(value);
    std::vector<llvm::Constant*> ops;
    for (auto& use : inst->operands()) {
      auto op = ResolveConditionRec(use.get(), context, failed);
      if (!op) {
        return nullptr;
      }
      ops.push_back(op);
    }
    const llvm::Triple module_triple(context.module->getTargetTriple());
    const llvm::TargetLibraryInfoImpl x(module_triple);
    const llvm::TargetLibraryInfo target_lib_info(x);
    return ConstantFoldInstruction(inst, ops, context.module->getDataLayout(), &target_lib_info);
  }

  return nullptr;
}

llvm::Constant* ResolveCondition(llvm::Value* Value, SpecializationContext& Context) {
  // Keeps track of all operands that failed to resolve to a constant to avoid duplicate lookups
  std::unordered_set<llvm::Value*> resolution_failed;
  return ResolveConditionRec(Value, Context, resolution_failed);
}

}  // namespace opossum
