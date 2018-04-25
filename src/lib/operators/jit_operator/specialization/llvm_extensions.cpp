#include "llvm_extensions.hpp"

#include <llvm/Analysis/ConstantFolding.h>
#include <llvm/IR/Constants.h>

#include <unordered_set>

namespace opossum {

const JitRuntimePointer::Ptr& _get_runtime_value(const llvm::Value* c_value, SpecializationContext& context) {
  auto value = c_value;
  if (context.llvm_value_map.count(c_value)) {
    value = context.llvm_value_map.lookup(c_value);
  }

  // try serving from cache
  if (context.runtime_value_map.count(value)) {
    return context.runtime_value_map[value];
  }

  if (const auto constant_expr = llvm::dyn_cast<llvm::ConstantExpr>(value)) {
    if (constant_expr->getType()->isPointerTy()) {
      switch (constant_expr->getOpcode()) {
        case llvm::Instruction::IntToPtr:
          if (const auto address = llvm::dyn_cast<llvm::ConstantInt>(constant_expr->getOperand(0))) {
            context.runtime_value_map[value] =
                std::make_shared<JitConstantRuntimePointer>(address->getValue().getLimitedValue());
          }
          break;
        default:
          break;
      }
    }
  } else if (const auto load_inst = llvm::dyn_cast<llvm::LoadInst>(value)) {
    if (load_inst->getType()->isPointerTy()) {
      if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
              _get_runtime_value(load_inst->getPointerOperand(), context))) {
        context.runtime_value_map[value] = std::make_shared<JitDereferencedRuntimePointer>(base);
      }
    }
  } else if (const auto gep_inst = llvm::dyn_cast<llvm::GetElementPtrInst>(value)) {
    llvm::APInt offset(64, 0);
    if (gep_inst->accumulateConstantOffset(context.module->getDataLayout(), offset)) {
      if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
              _get_runtime_value(gep_inst->getPointerOperand(), context))) {
        context.runtime_value_map[value] = std::make_shared<JitOffsetRuntimePointer>(base, offset.getLimitedValue());
      }
    }
  } else if (const auto bitcast_inst = llvm::dyn_cast<llvm::BitCastInst>(value)) {
    if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
            _get_runtime_value(bitcast_inst->getOperand(0), context))) {
      context.runtime_value_map[value] = std::make_shared<JitOffsetRuntimePointer>(base, 0L);
    }
  }

  if (!context.runtime_value_map.count(value)) {
    context.runtime_value_map[value] = std::make_shared<JitRuntimePointer>();
  }

  return context.runtime_value_map[value];
}

llvm::Constant* ConstantFoldInstruction(llvm::Instruction* I, llvm::ArrayRef<llvm::Constant*> Ops,
                                          const llvm::DataLayout& DL, const llvm::TargetLibraryInfo* TLI) {
  // Handle PHI nodes quickly here...
  if (auto* PN = llvm::dyn_cast<llvm::PHINode>(I)) {
    return nullptr;
  }

  if (const auto* CI = llvm::dyn_cast<llvm::CmpInst>(I))
    return llvm::ConstantFoldCompareInstOperands(CI->getPredicate(), Ops[0], Ops[1], DL, TLI);

  if (const auto* LI = llvm::dyn_cast<llvm::LoadInst>(I)) return nullptr;

  if (auto* IVI = llvm::dyn_cast<llvm::InsertValueInst>(I)) {
    return llvm::ConstantExpr::getInsertValue(llvm::cast<llvm::Constant>(IVI->getAggregateOperand()),
                                              llvm::cast<llvm::Constant>(IVI->getInsertedValueOperand()),
                                              IVI->getIndices());
  }

  if (auto* EVI = llvm::dyn_cast<llvm::ExtractValueInst>(I)) {
    return llvm::ConstantExpr::getExtractValue(llvm::cast<llvm::Constant>(EVI->getAggregateOperand()),
                                               EVI->getIndices());
  }

  return llvm::ConstantFoldInstOperands(I, Ops, DL, TLI);
}

llvm::Constant* ResolveCondition(llvm::Value* value, SpecializationContext& context,
                              std::unordered_set<llvm::Value*>& failed) {
  if (failed.count(value)) return nullptr;
  if (auto const_value = llvm::dyn_cast<llvm::Constant>(value)) return const_value;
  if (auto load = llvm::dyn_cast<llvm::LoadInst>(value)) {
    const auto runtime_pointer =
        std::dynamic_pointer_cast<const JitKnownRuntimePointer>(_get_runtime_value(load->getPointerOperand(), context));
    if (runtime_pointer && runtime_pointer->is_valid()) {
      const auto address = runtime_pointer->address();
      if (load->getType()->isIntegerTy()) {
        const auto int_type = llvm::dyn_cast<llvm::IntegerType>(load->getType());
        const auto bit_width = int_type->getIntegerBitWidth();
        const auto mask = bit_width == 64 ? 0xffffffffffffffff : (static_cast<uint64_t>(1) << bit_width) - 1;
        const auto value = *reinterpret_cast<uint64_t*>(address) & mask;
        return llvm::ConstantInt::get(int_type, value, int_type->getSignBit() > 0);
      }
    }
  } else if (auto inst = llvm::dyn_cast<llvm::Instruction>(value)) {
    failed.insert(value);
    std::vector<llvm::Constant*> ops;
    for (auto& use : inst->operands()) {
      auto op = ResolveCondition(use.get(), context, failed);
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
  std::unordered_set<llvm::Value*> failed;
  return ResolveCondition(Value, Context, failed);
}

}  // namespace opossum
