#include "jit_code_specializer.hpp"

#include <llvm/Analysis/GlobalsModRef.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Linker/IRMover.h>
#include <llvm/Support/YAMLTraits.h>
#include <llvm/Transforms/IPO/ForceFunctionAttrs.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>

#include <boost/algorithm/string/predicate.hpp>

#include <queue>

#include "jit_runtime_pointer.hpp"
#include "llvm_extensions.hpp"

namespace opossum {

JitCodeSpecializer::JitCodeSpecializer(std::shared_ptr<JitRepository>& repository)
    : _repository{repository ? repository : JitRepository::_make_shared()}, _llvm_context{_repository->llvm_context()} {
  if (!Hyrise::get().jit_repository) {
    Hyrise::get().jit_repository = _repository;
  }
}

std::unique_ptr<llvm::Module> JitCodeSpecializer::specialize_function(
    const std::string& root_function_name, const std::shared_ptr<const JitRuntimePointer>& runtime_this,
    const bool two_passes) {
  // The LLVMContext does not support concurrent access, so we only allow one specialization operation at a time for
  // each JitRepository (each bitcode repository has its own LLVMContext).
  std::lock_guard<std::mutex> lock(_repository->specialization_mutex());

  // Initialize specialization context with the function name and an empty module
  SpecializationContext context;
  context.root_function_name = root_function_name;
  context.module = std::make_unique<llvm::Module>(root_function_name, *_llvm_context);
  context.module->setDataLayout(_compiler.data_layout());

  // Locate and clone the root function from the bitcode repository
  const auto root_function = _repository->get_function(root_function_name);
  Assert(root_function, "Root function not found in repository.");
  context.root_function = _clone_root_function(context, *root_function);

  // The code specialization can run one or two specialization passes depending on its configuration.
  // For most simple operator pipelines, a single specialization pass should be sufficient. However, more complex
  // operators such as JitAggregate contain a number of loops. Properly specializing these loops requires a four-step
  // process:
  // - a specialization pass (that replaces the loop condition with a constant value)
  // - an optimization that includes loop unrolling
  // - a second specialization pass that can now specialize the unrolled loop bodies
  // - a final optimization pass

  // Run the first specialization and optimization pass
  context.runtime_value_map[context.root_function->arg_begin()] = runtime_this;
  _inline_function_calls(context);
  _perform_load_substitution(context);
  // Unroll loops only if two passes are selected
  _optimize(context, two_passes);

  // Conditionally run a second pass
  if (two_passes) {
    context.runtime_value_map.clear();
    context.runtime_value_map[context.root_function->arg_begin()] = runtime_this;
    _inline_function_calls(context);
    _perform_load_substitution(context);
    _optimize(context, false);
  }

  return std::move(context.module);
}

void JitCodeSpecializer::_inline_function_calls(SpecializationContext& context) const {
  // This method implements the main code fusion functionality.
  // It works as follows:
  // Throughout the fusion process, a working list of call sites (i.e., function calls) is maintained.
  // This queue is initialized with all call instructions from the initial root function.
  // Each call site is then handled in one of the following ways:
  // - Direct Calls:
  //   Direct calls explicitly encode the name of the called function. If a function implementation can be located in
  //   the JitRepository repository by that name the function is inlined (i.e., the call instruction is replaced with
  //   the function body from the repository. Calls to functions not available in the repository (e.g., calls into the
  //   C++ standard library) require a corresponding function declaration to be inserted into the module.
  //   This is necessary to avoid any unresolved function calls, which would invalidate the module and cause the
  //   just-in-time compiler to reject it. With an appropriate declaration, however, the just-in-time compiler is able
  //   to locate the machine code of these functions in the Hyrise binary when compiling and linking the module.
  // - Indirect Calls
  //   Indirect calls do not reference their called function by name. Instead, the address of the target function is
  //   either loaded from memory or computed. The code specialization unit analyzes the instructions surrounding the
  //   call and tries to infer the name of the called function. If this succeeds, the call is handled like a regular
  //   direct call. If not, no specialization of the call can be performed.
  //   In this case, the target address computed for the call at runtime will point to the correct machine code in the
  //   Hyrise binary and the pipeline will still execute successfully.
  //
  // Whenever a call instruction is replaced by the corresponding function body, the inlining algorithm reports back
  // any new call sites encountered in the process. These are pushed to the end of the working queue. Once this queue
  // is empty, the operator fusion process is completed.

  std::queue<llvm::CallSite> call_sites;

  // Initialize the call queue with all call and invoke instructions from the root function.
  _visit<llvm::CallInst>(*context.root_function, [&](llvm::CallInst& inst) { call_sites.push(llvm::CallSite(&inst)); });
  _visit<llvm::InvokeInst>(*context.root_function,
                           [&](llvm::InvokeInst& inst) { call_sites.push(llvm::CallSite(&inst)); });

  while (!call_sites.empty()) {
    auto& call_site = call_sites.front();

    // Resolve indirect (virtual) function calls
    if (call_site.isIndirectCall()) {
      const auto called_value = call_site.getCalledValue();
      // Get the runtime location of the called function (i.e., the compiled machine code of the function)
      const auto called_runtime_value =
          std::dynamic_pointer_cast<const JitKnownRuntimePointer>(GetRuntimePointerForValue(called_value, context));
      if (called_runtime_value && called_runtime_value->is_valid()) {
        // LLVM implements virtual function calls via virtual function tables
        // (see https://en.wikipedia.org/wiki/Virtual_method_table).
        // The resolution of virtual calls starts with a pointer to the object that the virtual call should be performed
        // on. This object contains a pointer to its vtable (usually at offset 0). The vtable contains a list of
        // function pointers (one for each virtual function).
        // In the LLVM bitcode, a virtual call is resolved through a number of LLVM pointer operations:
        // - a load instruction to dereference the vtable pointer in the object
        // - a getelementptr instruction to select the correct virtual function in the vtable
        // - another load instruction to dereference the virtual function pointer from the table
        // When analyzing a virtual call, the code specializer works backwards starting from the pointer to the called
        // function (called_runtime_value).
        // Moving "up" one level (undoing one load operation) yields the pointer to the function pointer in the
        // vtable. The total_offset of this pointer corresponds to the index in the vtable that is used for the virtual
        // call.
        // Moving "up" another level yields the pointer to the object that the virtual function is called on. Using RTTI
        // the class name of the object can be determined.
        // These two pieces of information (the class name and the vtable index) are sufficient to unambiguously
        // identify the called virtual function and locate it in the bitcode repository.

        // Determine the vtable index and class name of the virtual call
        const auto vtable_index =
            called_runtime_value->up().total_offset() / context.module->getDataLayout().getPointerSize();
        const auto instance = reinterpret_cast<JitRTTIHelper*>(called_runtime_value->up().up().base().address());
        const auto class_name = typeid(*instance).name();

        // If the called function can be located in the repository, the virtual call is replaced by a direct call to
        // that function.
        if (const auto repo_function = _repository->get_vtable_entry(class_name, vtable_index)) {
          call_site.setCalledFunction(repo_function);
        }
      } else {
        // The virtual call could not be resolved. There is nothing we can inline so we move on.
        call_sites.pop();
        continue;
      }
    }

    auto function = call_site.getCalledFunction();
    // ignore invalid functions
    if (!function) {
      call_sites.pop();
      continue;
    }

    const auto function_name = function->getName().str();

    const auto function_has_opossum_namespace =
        boost::starts_with(function_name, "_ZNK7opossum") || boost::starts_with(function_name, "_ZN7opossum");

    // A note about "__clang_call_terminate":
    // __clang_call_terminate is generated / used internally by clang to call the std::terminate function when exception
    // handling fails. For some unknown reason this function cannot be resolved in the Hyrise binary when jit-compiling
    // bitcode that uses the function. The function is, however, present in the bitcode repository.
    // We thus always inline this function from the repository.

    // All function that are not in the opossum:: namespace are not considered for inlining. Instead, a function
    // declaration (without a function body) is created.
    if (!function_has_opossum_namespace && function_name != "__clang_call_terminate") {
      context.llvm_value_map[function] = _create_function_declaration(context, *function, function->getName());
      call_sites.pop();
      continue;
    }

    // Determine whether the first function argument is a pointer/reference to an object, but the runtime location
    // for this object cannot be determined.
    // This is the case for member functions that are called within a loop body. These functions may be called on
    // different objects in different loop iterations.
    // If two specialization passes are performed, these functions should be inlined after loop unrolling has been
    // performed (i.e., during the second pass).
    auto first_argument = call_site.arg_begin();
    auto first_argument_cannot_be_resolved = first_argument->get()->getType()->isPointerTy() &&
                                             !GetRuntimePointerForValue(first_argument->get(), context)->is_valid();

    if (first_argument_cannot_be_resolved && function_name != "__clang_call_terminate") {
      call_sites.pop();
      continue;
    }

    // We create a mapping from values in the source module to values in the target module.
    // This mapping is passed to LLVM's cloning function and ensures that all references to other functions, global
    // variables, and function arguments refer to values in the target module and NOT in the source module.
    // This way the module stays self-contained and valid.
    context.llvm_value_map.clear();

    // Map called functions
    _visit<const llvm::Function>(*function, [&](const auto& fn) {
      if (fn.isDeclaration() && !context.llvm_value_map.count(&fn)) {
        context.llvm_value_map[&fn] = _create_function_declaration(context, fn, fn.getName());
      }
    });

    // Map global variables
    _visit<const llvm::GlobalVariable>(*function, [&](auto& global) {
      if (!context.llvm_value_map.count(&global)) {
        context.llvm_value_map[&global] = _clone_global_variable(context, global);
      }
    });

    // Map function arguments
    auto function_arg = function->arg_begin();
    auto call_arg = call_site.arg_begin();
    for (; function_arg != function->arg_end() && call_arg != call_site.arg_end(); ++function_arg, ++call_arg) {
      context.llvm_value_map[function_arg] = call_arg->get();
    }

    // Instruct LLVM to perform the function inlining and push all new call sites to the working queue
    llvm::InlineFunctionInfo info;
    if (InlineFunction(call_site, info, nullptr, false, nullptr, context)) {
      for (const auto& new_call_site : info.InlinedCallSites) {
        call_sites.push(new_call_site);
      }
    }

    // clear runtime_value_map to allow multiple inlining of same function
    auto runtime_this = context.runtime_value_map[context.root_function->arg_begin()];
    context.runtime_value_map.clear();
    context.runtime_value_map[context.root_function->arg_begin()] = runtime_this;

    call_sites.pop();
  }
}

void JitCodeSpecializer::_perform_load_substitution(SpecializationContext& context) const {
  // Iterate over all load instructions
  _visit<llvm::LoadInst>(*context.root_function, [&](llvm::LoadInst& inst) {
    // Try to get the runtime location of the value that is loaded by the current load instruction
    const auto runtime_pointer = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
        GetRuntimePointerForValue(inst.getPointerOperand(), context));
    if (!runtime_pointer || !runtime_pointer->is_valid()) {
      return;
    }

    // Depending of the data type of the load instruction the memory location of the substitute value is casted to the
    // same type to retrieve the runtime value.
    // A constant is created from that value and all uses of the original load instructions are replaced by that
    // constant.
    const auto address = runtime_pointer->address();
    if (inst.getType()->isIntegerTy()) {
      const auto value = dereference_flexible_width_int_pointer(address, inst.getType()->getIntegerBitWidth());
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
                                                                 const std::string& cloned_function_name) const {
  // If the function is already declared (or even defined) in the module, we should not add a redundant declaration
  if (auto fn = context.module->getFunction(cloned_function_name)) {
    return fn;
  }

  // Create the declaration with correct signature, linkage, and attributes
  const auto declaration = llvm::Function::Create(llvm::cast<llvm::FunctionType>(function.getValueType()),
                                                  function.getLinkage(), cloned_function_name, context.module.get());
  declaration->copyAttributesFrom(&function);
  return declaration;
}

llvm::Function* JitCodeSpecializer::_clone_root_function(SpecializationContext& context,
                                                         const llvm::Function& function) const {
  // First create an appropriate function declaration that we can later clone the function body into
  const auto cloned_function = _create_function_declaration(context, function, _specialized_root_function_name);

  // We create a mapping from values in the source module to values in the target module.
  // This mapping is passed to LLVM's cloning function and ensures that all references to other functions, global
  // variables, and function arguments refer to values in the target module and NOT in the source module.
  // This way the module stays self-contained and valid.

  // Map functions called
  _visit<const llvm::Function>(function, [&](const auto& fn) {
    if (!context.llvm_value_map.count(&fn)) {
      context.llvm_value_map[&fn] = _create_function_declaration(context, fn, fn.getName());
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

  // Instruct LLVM to perform the actual cloning
  llvm::SmallVector<llvm::ReturnInst*, 8> returns;
  llvm::CloneFunctionInto(cloned_function, &function, context.llvm_value_map, true, returns);

  return cloned_function;
}

llvm::GlobalVariable* JitCodeSpecializer::_clone_global_variable(SpecializationContext& context,
                                                                 const llvm::GlobalVariable& global_variable) const {
  // If the global variable already exists in the module, there is no point in cloning it again
  if (auto global = context.module->getGlobalVariable(global_variable.getName())) {
    return global;
  }

  // Create a new global variable with the same type, attributes, and properties
  const auto cloned_global = llvm::dyn_cast<llvm::GlobalVariable>(
      context.module->getOrInsertGlobal(global_variable.getName(), global_variable.getValueType()));
  cloned_global->setLinkage(global_variable.getLinkage());
  cloned_global->setThreadLocalMode(global_variable.getThreadLocalMode());
  cloned_global->copyAttributesFrom(&global_variable);

  if (!global_variable.isDeclaration()) {
    if (global_variable.hasInitializer()) {
      // Instruct LLVM to perform the cloning of the actual value (i.e., initializer) of the variable
      cloned_global->setInitializer(llvm::MapValue(global_variable.getInitializer(), context.llvm_value_map));
    }

    // Clone LLVM metadata for the global variable
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
    // visit all functions in a module
    for (auto& function : element) {
      _visit(function, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::Function>) {                                // NOLINT
    // visit all basic blocks in a function
    for (auto& block : element) {
      _visit(block, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::BasicBlock>) {                              // NOLINT
    // visit all instructions in a basic block
    for (auto& inst : element) {
      _visit(inst, fn);
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::Instruction>) {                             // NOLINT
    // if we are searching for a specific type of instruction ...
    if constexpr(std::is_base_of_v<llvm::Instruction, T>) {
      // ... cast the instruction to that type and call the callback if successful
      if (auto inst = llvm::dyn_cast<T>(&element)) {
        fn(*inst);
      }
    } else {
      // visit all operands of the instruction otherwise
      for (auto& op : element.operands()) {
        _visit(*op.get(), fn);
      }
    }
  } else if constexpr(std::is_same_v<std::remove_const_t<U>, llvm::ConstantExpr>) {                            // NOLINT
    // visit all operands of const-expressions to recursively search nested expression trees
    for (auto& op : element.operands()) {
      _visit(*op.get(), fn);
    }
  // finally try to cast to whatever type we are looking for
  } else if (auto op = llvm::dyn_cast<T>(&element)) {
    fn(*op);
  }
  // clang-format on
}

}  // namespace opossum
