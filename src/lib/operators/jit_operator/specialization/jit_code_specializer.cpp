#include "jit_code_specializer.hpp"

#include <queue>

#include <boost/algorithm/string/predicate.hpp>

#include <llvm/Analysis/GlobalsModRef.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Linker/IRMover.h>
#include <llvm/Support/YAMLTraits.h>
#include <llvm/Transforms/IPO/ForceFunctionAttrs.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "llvm_extensions.hpp"

namespace opossum {

JitCodeSpecializer::JitCodeSpecializer()
    : _repository{JitRepository::get()}, _llvm_context{_repository.llvm_context()}, _compiler{_llvm_context} {}

void JitCodeSpecializer::_specialize_function_impl(const std::string& root_function_name,
                                                   const JitRuntimePointer::Ptr& runtime_this, const bool two_passes) {
  SpecializationContext context;
  context.root_function_name = root_function_name;
  context.module = std::make_unique<llvm::Module>(root_function_name, *_llvm_context);
  context.module->setDataLayout(_compiler.data_layout());

  const auto root_function = _repository.get_function(root_function_name);
  DebugAssert(root_function, "Root function not found in repository.");
  context.root_function = _clone_function(context, *root_function, "_");

  if (two_passes) {
    context.runtime_value_map[context.root_function->arg_begin()] = runtime_this;
    _inline_function_calls(context, false);
    _perform_load_substitution(context);
    _optimize(context, true);
    context.runtime_value_map.clear();
    context.runtime_value_map[context.root_function->arg_begin()] = runtime_this;
    _inline_function_calls(context, true);
    _perform_load_substitution(context);
    _optimize(context, false);
  } else {
    context.runtime_value_map[context.root_function->arg_begin()] = runtime_this;
    _inline_function_calls(context, false);
    _perform_load_substitution(context);
    _optimize(context, false);
  }

  // TESTING ONLY
  int counter = 0;
  _visit<llvm::Instruction>(*context.root_function, [&](llvm::Instruction& inst) { counter++; });
  std::cout << "TestCounter: " << counter << std::endl;

  _compiler.add_module(std::move(context.module));
}

void JitCodeSpecializer::_inline_function_calls(SpecializationContext& context, const bool two_passes) const {
  std::queue<llvm::CallSite> call_sites;

  _visit<llvm::CallInst>(*context.root_function, [&](llvm::CallInst& inst) { call_sites.push(llvm::CallSite(&inst)); });
  _visit<llvm::InvokeInst>(*context.root_function,
                           [&](llvm::InvokeInst& inst) { call_sites.push(llvm::CallSite(&inst)); });

  while (!call_sites.empty()) {
    auto& call_site = call_sites.front();
    if (call_site.isIndirectCall()) {
      const auto called_value = call_site.getCalledValue();
      const auto called_runtime_value =
          std::dynamic_pointer_cast<const JitKnownRuntimePointer>(_get_runtime_value(context, called_value));
      if (called_runtime_value && called_runtime_value->is_valid()) {
        const auto vtable_index =
            called_runtime_value->up().total_offset() / context.module->getDataLayout().getPointerSize();
        const auto instance = reinterpret_cast<JitRTTIHelper*>(called_runtime_value->up().up().base().address());
        const auto class_name = typeid(*instance).name();
        if (const auto repo_function = _repository.get_vtable_entry(class_name, vtable_index)) {
          call_site.setCalledFunction(repo_function);
        }
      } else {
        // The virtual call could not be resolved. There is nothing we can inline so we might as well move on.
        call_sites.pop();
        continue;
      }
    }

    auto& function = *call_site.getCalledFunction();
    auto function_name = function.getName().str();
    auto x = !boost::starts_with(function.getName().str(), "_ZNK7opossum") &&
             !boost::starts_with(function.getName().str(), "_ZN7opossum") &&
             function.getName().str() != "__clang_call_terminate";
    if (x) {
      context.llvm_value_map[&function] = _create_function_declaration(context, function);
      call_sites.pop();
      continue;
    }

    auto arg = call_site.arg_begin();
    auto y = arg->get()->getType()->isPointerTy() && !_get_runtime_value(context, arg->get())->is_valid() &&
             function.getName().str() != "__clang_call_terminate";

    if (y && !two_passes) {
      call_sites.pop();
      continue;
    }

    context.llvm_value_map.clear();

    // map called functions
    _visit<const llvm::Function>(function, [&](const auto& fn) {
      if (fn.isDeclaration() && !context.llvm_value_map.count(&fn)) {
        context.llvm_value_map[&fn] = _create_function_declaration(context, fn);
      }
    });

    // map global variables
    _visit<const llvm::GlobalVariable>(function, [&](auto& global) {
      if (!context.llvm_value_map.count(&global)) {
        context.llvm_value_map[&global] = _clone_global_variable(context, global);
      }
    });

    // map function arguments
    auto function_arg = function.arg_begin();
    auto call_arg = call_site.arg_begin();
    for (; function_arg != function.arg_end() && call_arg != call_site.arg_end(); ++function_arg, ++call_arg) {
      context.llvm_value_map[function_arg] = call_arg->get();
    }

    llvm::InlineFunctionInfo info;
    // SpecializationContext ctx{context.module.get(), context.runtime_value_map, context.llvm_value_map};
    if (InlineFunction(call_site, info, nullptr, false, context)) {
      for (const auto& new_call_site : info.InlinedCallSites) {
        call_sites.push(new_call_site);
      }
    }

    call_sites.pop();
  }
}

void JitCodeSpecializer::_perform_load_substitution(SpecializationContext& context) const {
  _visit<llvm::LoadInst>(*context.root_function, [&](llvm::LoadInst& inst) {
    const auto runtime_pointer =
        std::dynamic_pointer_cast<const JitKnownRuntimePointer>(_get_runtime_value(context, inst.getPointerOperand()));
    if (!runtime_pointer || !runtime_pointer->is_valid()) {
      return;
    }
    const auto address = runtime_pointer->address();
    if (inst.getType()->isIntegerTy()) {
      const auto bit_width = inst.getType()->getIntegerBitWidth();
      const auto mask =
          bit_width == 64 ? 0xffffffffffffffff : (static_cast<uint64_t>(1) << inst.getType()->getIntegerBitWidth()) - 1;
      const auto value = *reinterpret_cast<uint64_t*>(address) & mask;
      inst.replaceAllUsesWith(llvm::ConstantInt::get(inst.getType(), value));
    } else if (inst.getType()->isFloatTy()) {
      const auto value = *reinterpret_cast<float*>(address);
      inst.replaceAllUsesWith(llvm::ConstantFP::get(inst.getType(), value));
    } else if (inst.getType()->isDoubleTy()) {
      const auto value = *reinterpret_cast<double*>(address);
      inst.replaceAllUsesWith(llvm::ConstantFP::get(inst.getType(), value));
    }
  });
}

void JitCodeSpecializer::_optimize(SpecializationContext& context, const bool unroll_loops) const {
  _visit<llvm::BranchInst>(*context.root_function, [&](llvm::BranchInst& branch_inst) {
    // TODO(johannes) properly identify unrolling metadata
    branch_inst.setMetadata(18, nullptr);
  });

  const llvm::Triple module_triple(context.module->getTargetTriple());
  const llvm::TargetLibraryInfoImpl target_lib_info(module_triple);

  llvm::legacy::PassManager pass_manager;
  pass_manager.add(new llvm::TargetLibraryInfoWrapperPass(target_lib_info));
  pass_manager.add(llvm::createTargetTransformInfoWrapperPass(_compiler.target_machine().getTargetIRAnalysis()));
  llvm::PassManagerBuilder pass_builder;
  pass_builder.OptLevel = 1;
  pass_builder.SizeLevel = 0;
  pass_builder.DisableUnitAtATime = true;
  pass_builder.DisableUnrollLoops = true;
  pass_builder.LoopVectorize = false;
  pass_builder.SLPVectorize = false;

  _compiler.target_machine().adjustPassManager(pass_builder);
  pass_builder.addFunctionSimplificationPasses(pass_manager);
  if (unroll_loops) {
    pass_manager.add(llvm::createLoopUnrollPass(3, 1000000000, -1, 0));
  }
  pass_manager.run(*context.module);
}

llvm::Function* JitCodeSpecializer::_create_function_declaration(SpecializationContext& context,
                                                                 const llvm::Function& function,
                                                                 const std::string& suffix) const {
  if (auto fn = context.module->getFunction(function.getName())) {
    return fn;
  }
  const auto declaration =
      llvm::Function::Create(llvm::cast<llvm::FunctionType>(function.getValueType()), function.getLinkage(),
                             function.getName() + suffix, context.module.get());
  declaration->copyAttributesFrom(&function);
  return declaration;
}

llvm::Function* JitCodeSpecializer::_clone_function(SpecializationContext& context, const llvm::Function& function,
                                                    const std::string& suffix) const {
  const auto cloned_function = _create_function_declaration(context, function, suffix);

  // map personality function
  if (function.hasPersonalityFn()) {
    _visit<llvm::Function>(*function.getPersonalityFn(), [&](const auto& fn) {
      if (!context.llvm_value_map.count(&fn)) {
        context.llvm_value_map[&fn] = _create_function_declaration(context, fn);
      }
    });
  }

  // map functions called
  _visit<const llvm::Function>(function, [&](const auto& fn) {
    if (fn.isDeclaration() && !context.llvm_value_map.count(&fn)) {
      context.llvm_value_map[&fn] = _create_function_declaration(context, fn);
    }
  });

  // map global variables
  _visit<const llvm::GlobalVariable>(function, [&](auto& global) {
    if (!context.llvm_value_map.count(&global)) {
      context.llvm_value_map[&global] = _clone_global_variable(context, global);
    }
  });

  // map function args
  auto arg = function.arg_begin();
  auto cloned_arg = cloned_function->arg_begin();
  for (; arg != function.arg_end() && cloned_arg != cloned_function->arg_end(); ++arg, ++cloned_arg) {
    cloned_arg->setName(arg->getName());
    context.llvm_value_map[arg] = cloned_arg;
  }

  llvm::SmallVector<llvm::ReturnInst*, 8> returns;
  llvm::CloneFunctionInto(cloned_function, &function, context.llvm_value_map, true, returns);

  // TODO remove?
  if (function.hasPersonalityFn()) {
    cloned_function->setPersonalityFn(llvm::MapValue(function.getPersonalityFn(), context.llvm_value_map));
  }

  return cloned_function;
}

llvm::GlobalVariable* JitCodeSpecializer::_clone_global_variable(SpecializationContext& context,
                                                                 const llvm::GlobalVariable& global_variable) const {
  if (auto gb = context.module->getGlobalVariable(global_variable.getName())) {
    return gb;
  }

  const auto cloned_global =
      new llvm::GlobalVariable(*context.module, global_variable.getValueType(), global_variable.isConstant(),
                               global_variable.getLinkage(), nullptr, global_variable.getName(), nullptr,
                               global_variable.getThreadLocalMode(), global_variable.getType()->getAddressSpace());

  cloned_global->copyAttributesFrom(&global_variable);

  if (!global_variable.isDeclaration()) {
    if (global_variable.hasInitializer()) {
      cloned_global->setInitializer(llvm::MapValue(global_variable.getInitializer(), context.llvm_value_map));
    }

    llvm::SmallVector<std::pair<uint32_t, llvm::MDNode*>, 1> metadata_nodes;
    global_variable.getAllMetadata(metadata_nodes);
    for (const auto& metadata_node : metadata_nodes) {
      cloned_global->addMetadata(metadata_node.first,
                                 *MapMetadata(metadata_node.second, context.llvm_value_map, llvm::RF_MoveDistinctMDs));
    }
  }

  return cloned_global;
}

const JitRuntimePointer::Ptr& JitCodeSpecializer::_get_runtime_value(SpecializationContext& context,
                                                                     const llvm::Value* value) const {
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
              _get_runtime_value(context, load_inst->getPointerOperand()))) {
        context.runtime_value_map[value] = std::make_shared<JitDereferencedRuntimePointer>(base);
      }
    }
  } else if (const auto gep_inst = llvm::dyn_cast<llvm::GetElementPtrInst>(value)) {
    llvm::APInt offset(64, 0);
    if (gep_inst->accumulateConstantOffset(context.module->getDataLayout(), offset)) {
      if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
              _get_runtime_value(context, gep_inst->getPointerOperand()))) {
        context.runtime_value_map[value] = std::make_shared<JitOffsetRuntimePointer>(base, offset.getLimitedValue());
      }
    }
  } else if (const auto bitcast_inst = llvm::dyn_cast<llvm::BitCastInst>(value)) {
    if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
            _get_runtime_value(context, bitcast_inst->getOperand(0)))) {
      context.runtime_value_map[value] = std::make_shared<JitOffsetRuntimePointer>(base, 0L);
    }
  }

  if (!context.runtime_value_map.count(value)) {
    context.runtime_value_map[value] = std::make_shared<JitRuntimePointer>();
  }

  return context.runtime_value_map[value];
}

template <typename T, typename U>
void JitCodeSpecializer::_visit(U& element, std::function<void(T&)> fn) const {
  // clang-format off
  if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::Module>) {
    for (auto& function : element) {
      _visit(function, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::Function>) {
    for (auto& block : element) {
      _visit(block, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::BasicBlock>) {
    for (auto& inst : element) {
      _visit(inst, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::Instruction>) {
    if constexpr(std::is_base_of_v<llvm::Instruction, T>) {
      if (auto inst = llvm::dyn_cast<T>(&element)) {
        fn(*inst);
      }
    } else {
      for (auto& op : element.operands()) {
        _visit(*op.get(), fn);
      }
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::ConstantExpr>) {
    for (auto& op : element.operands()) {
      _visit(*op.get(), fn);
    }
  } else {
    if (auto op = llvm::dyn_cast<T>(&element)) {
      fn(*op);
    } else if (auto const_expr = llvm::dyn_cast<llvm::ConstantExpr>(&element)) {
      _visit(*const_expr, fn);
    }
  }
  // clang-format on
}

}  // namespace opossum
