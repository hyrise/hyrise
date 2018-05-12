#include "jit_code_specializer.hpp"

#include <llvm/Analysis/GlobalsModRef.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Linker/IRMover.h>
#include <llvm/Support/YAMLTraits.h>
#include <llvm/Transforms/IPO/ForceFunctionAttrs.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>

#include <boost/algorithm/string/predicate.hpp>

#include <queue>

#include "llvm_extensions.hpp"

namespace opossum {

JitCodeSpecializer::JitCodeSpecializer(JitRepository& repository)
    : _repository{repository}, _llvm_context{_repository.llvm_context()}, _compiler{_llvm_context} {}

std::shared_ptr<llvm::Module> JitCodeSpecializer::specialize_function(
    const std::string& root_function_name, const std::shared_ptr<const JitRuntimePointer>& runtime_this,
    const bool two_passes) {
  std::lock_guard<std::mutex> lock(_repository.specialization_mutex());

  SpecializationContext context;
  context.root_function_name = root_function_name;
  context.module = std::make_shared<llvm::Module>(root_function_name, *_llvm_context);
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

  return context.module;
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
          std::dynamic_pointer_cast<const JitKnownRuntimePointer>(GetRuntimePointerForValue(called_value, context));
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
    auto y = arg->get()->getType()->isPointerTy() && !GetRuntimePointerForValue(arg->get(), context)->is_valid() &&
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
    const auto runtime_pointer = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
        GetRuntimePointerForValue(inst.getPointerOperand(), context));
    if (!runtime_pointer || !runtime_pointer->is_valid()) {
      return;
    }
    const auto address = runtime_pointer->address();
    if (inst.getType()->isIntegerTy()) {
      const auto bit_width = inst.getType()->getIntegerBitWidth();
      const auto mask = bit_width == 64 ? 0xffffffffffffffff : (static_cast<uint64_t>(1) << bit_width) - 1;
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
  // Create a pass manager that handles dependencies between the optimization passes.
  // The selection of optimization passes is a manual trail-and-error-based selection and provides a "good" balance
  // between the result and runtime of the optimization.
  llvm::legacy::PassManager pass_manager;

  // Removes common subexpressions
  pass_manager.add(llvm::createEarlyCSEPass(true));
  // Attempts to remove as much code from the body of a loop to either before or after the loop
  pass_manager.add(llvm::createLICMPass());

  if (unroll_loops) {
    _visit<llvm::BranchInst>(*context.root_function, [&](llvm::BranchInst& branch_inst) {
      // Remove metadata that prevents loop unrolling
      branch_inst.setMetadata(llvm::LLVMContext::MD_loop, nullptr);
    });
    // Add a loop unroll pass. The maximum threshold is necessary to force LLVM to unroll loops even if it judges the
    // unrolling operation as disadvantageous. Loop unrolling (in most cases for the current jit operators) allows
    // further specialization of the code that leads to much shorter and more efficient code in the end.
    // But LLVM cannot know that and thus the internal cost model for loop unrolling is disabled.
    pass_manager.add(llvm::createLoopUnrollPass(3, std::numeric_limits<int>::max(), -1, 0));
  }

  // Removes dead code (e.g., unreachable instructions or instructions whose result is unused)
  pass_manager.add(llvm::createAggressiveDCEPass());
  // Simplifies the control flow by combining basic blocks, removing unreachable blocks, etc.
  pass_manager.add(llvm::createCFGSimplificationPass());
  // Combines instructions to fold computation of expressions with many constants
  pass_manager.add(llvm::createInstructionCombiningPass(false));
  pass_manager.run(*context.module);
}

llvm::Function* JitCodeSpecializer::_create_function_declaration(SpecializationContext& context,
                                                                 const llvm::Function& function,
                                                                 const std::string& suffix) const {
  // If the function is already declared (or even defined) in the module, we should not add a redundant declaration
  if (auto fn = context.module->getFunction(function.getName())) {
    return fn;
  }

  // Create the declaration with correct signature, linkage, and attributes
  const auto declaration =
      llvm::Function::Create(llvm::cast<llvm::FunctionType>(function.getValueType()), function.getLinkage(),
                             function.getName() + suffix, context.module.get());
  declaration->copyAttributesFrom(&function);
  return declaration;
}

llvm::Function* JitCodeSpecializer::_clone_function(SpecializationContext& context, const llvm::Function& function,
                                                    const std::string& suffix) const {
  // First create an appropriate function declaration that we can later clone the function into
  const auto cloned_function = _create_function_declaration(context, function, suffix);

  // We create a mapping from values in the source module to values in the target module.
  // This mapping is passed to LLVM's cloning function and ensures that all references to other functions, global
  // variables, and function arguments refer to values in the target module and NOT in the source module.
  // This way the module stays self-contained and valid.

  // Map functions called
  _visit<const llvm::Function>(function, [&](const auto& fn) {
    if (!context.llvm_value_map.count(&fn)) {
      context.llvm_value_map[&fn] = _create_function_declaration(context, fn);
    }
  });

  // Map global variables accessed
  _visit<const llvm::GlobalVariable>(function, [&](auto& global) {
    if (!context.llvm_value_map.count(&global)) {
      context.llvm_value_map[&global] = _clone_global_variable(context, global);
    }
  });

  // Map function arguments
  auto arg = function.arg_begin();
  auto cloned_arg = cloned_function->arg_begin();
  for (; arg != function.arg_end() && cloned_arg != cloned_function->arg_end(); ++arg, ++cloned_arg) {
    cloned_arg->setName(arg->getName());
    context.llvm_value_map[arg] = cloned_arg;
  }

  llvm::SmallVector<llvm::ReturnInst*, 8> returns;
  llvm::CloneFunctionInto(cloned_function, &function, context.llvm_value_map, true, returns);

  return cloned_function;
}

llvm::GlobalVariable* JitCodeSpecializer::_clone_global_variable(SpecializationContext& context,
                                                                 const llvm::GlobalVariable& global_variable) const {
  if (auto global = context.module->getGlobalVariable(global_variable.getName())) {
    return global;
  }

  //
  //  context.module->getOrInsertGlobal()
  //  auto cloned_global =
  //  gVar->setAlignment(4);

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

template <typename T, typename U>
void JitCodeSpecializer::_visit(U& element, std::function<void(T&)> fn) const {
  // clang-format off
  if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::Module>) {
    for (auto& function : element) {
      _visit(function, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::Function>) {                                // NOLINT
    for (auto& block : element) {
      _visit(block, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::BasicBlock>) {                              // NOLINT
    for (auto& inst : element) {
      _visit(inst, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::Instruction>) {                             // NOLINT
    if constexpr(std::is_base_of_v<llvm::Instruction, T>) {
      if (auto inst = llvm::dyn_cast<T>(&element)) {
        fn(*inst);
      }
    } else {
      for (auto& op : element.operands()) {
        _visit(*op.get(), fn);
      }
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::ConstantExpr>) {                            // NOLINT
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
