#include "jit_module.hpp"

#include <queue>

#include <llvm/Analysis/GlobalsModRef.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Linker/IRMover.h>
#include <llvm/Support/YAMLTraits.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <llvm/Transforms/IPO/ForceFunctionAttrs.h>

#include "utils/llvm_utils.hpp"
#include "utils/my_llvm.hpp"


namespace opossum {

JitModule::JitModule()
        : _repository{JitRepository::get()},
          _llvm_context{_repository.llvm_context()},
          _module{std::make_unique<llvm::Module>("jit_module", *_llvm_context)},
          _compiler{_llvm_context} {
  _module->setDataLayout(_compiler.data_layout());
}

void JitModule::_specialize_impl(const std::string& root_function_name, const JitRuntimePointer::Ptr& runtime_this, const bool second_pass) {
  const auto root_function = _repository.get_function(root_function_name);
  DebugAssert(root_function, "Root function not found in repository.");
  _root_function = _clone_function(*root_function, "_");

  _runtime_values[_root_function->arg_begin()] = runtime_this;
  _resolve_virtual_calls(false);
  _replace_loads_with_runtime_values();

  if (second_pass) {
    _optimize(true);
    _runtime_values[&*_root_function->arg_begin()] = runtime_this;
    _resolve_virtual_calls(true);
    _replace_loads_with_runtime_values();
  }

  _optimize(false);
}

void JitModule::_resolve_virtual_calls(const bool second_pass) {
  std::queue<llvm::CallSite> call_sites;
  _visit<llvm::CallInst>([&](llvm::CallInst& inst) { call_sites.push(llvm::CallSite(&inst)); });
  _visit<llvm::InvokeInst>([&](llvm::InvokeInst& inst) { call_sites.push(llvm::CallSite(&inst)); });
  //uint32_t counter = 0;
  //llvm_utils::module_to_file("/tmp/after_" + std::to_string(counter++) + ".ll", *_module);
  while (!call_sites.empty()) {
    auto& call_site = call_sites.front();
    if (call_site.isIndirectCall()) {
      //call_site.getCalledValue()->print(llvm::outs(), true);
      //std::cout << std::endl;
      // attempt to resolve virtual function call
      const auto called_value = call_site.getCalledValue();
      const auto called_runtime_value =
              std::dynamic_pointer_cast<const JitKnownRuntimePointer>(_get_runtime_value(called_value));
      if (called_runtime_value && called_runtime_value->is_valid()) {
        const auto vtable_index = called_runtime_value->up().total_offset() / _module->getDataLayout().getPointerSize();
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
    //std::cerr << "name: " << function_name << std::endl;
    auto x = !boost::starts_with(function.getName().str(), "_ZNK7opossum") && !boost::starts_with(function.getName().str(), "_ZN7opossum") && function.getName().str() != "__clang_call_terminate";
    //std::cerr << "X: " << x << std::endl;
    if (x) {
      _llvm_value_map[&function] = _create_function_declaration(function);
      call_sites.pop();
      continue;
    }

    //std::cerr << "about to inline " << function.getName().str() << std::endl;

    auto arg = call_site.arg_begin();
    auto y = arg->get()->getType()->isPointerTy() && !_get_runtime_value(arg->get())->is_valid() && function.getName().str() != "__clang_call_terminate";
    //std::cerr << "Y: " << y << std::endl;
    if (y && !second_pass) {
      call_sites.pop();
      continue;
    }

    _llvm_value_map.clear();
    // map personality function
    //if (function.hasPersonalityFn()) {
    //  _visit<llvm::Function>(*function.getPersonalityFn(), [&](const auto& fn) {
    //    if (!_llvm_value_map.count(&fn)) {
    //      _llvm_value_map[&fn] = _create_function_declaration(fn);
    //    }
    //  });
    // }

    // map called functions
    _visit<const llvm::Function>(function, [&](const auto& fn) {
      if (fn.isDeclaration() && !_llvm_value_map.count(&fn)) {
        _llvm_value_map[&fn] = _create_function_declaration(fn);
      }
    });

    // map global variables
    _visit<const llvm::GlobalVariable>(function, [&](auto& global) {
      if (!_llvm_value_map.count(&global)) {
        _llvm_value_map[&global] = _clone_global(global);
      }
    });

    // map function args
    auto function_arg = function.arg_begin();
    auto call_arg = call_site.arg_begin();
    for (; function_arg != function.arg_end() && call_arg != call_site.arg_end(); ++function_arg, ++call_arg) {
      _llvm_value_map[function_arg] = call_arg->get();
    }

    llvm::InlineFunctionInfo info;
    InlineContext ctx{_module.get(), _runtime_values, _llvm_value_map};
    std::cerr << "inlining: " << function.getName().str() << std::endl;
    if (llvm::MyInlineFunction(call_site, info, nullptr, false, ctx)) {
      for (const auto& new_call_site : info.InlinedCallSites) {
        call_sites.push(new_call_site);
      }
    }

    //if (function.hasPersonalityFn()) {
    //      _root_function->setPersonalityFn(_llvm_value_map[function.getPersonalityFn()]);
    //  }

    call_sites.pop();
    //llvm_utils::module_to_file("/tmp/after_" + std::to_string(counter++) + ".ll", *_module);
  }
}

void JitModule::_optimize(bool with_unroll) {
  _runtime_values.clear();

  _visit<llvm::BranchInst>([&](llvm::BranchInst& branch_inst) {
    // TODO(johannes) properly identify unrolling metadata
    branch_inst.setMetadata(18, nullptr);
  });

  //  const auto before_path = "/tmp/before.ll";
  //  const auto after_path = "/tmp/after.ll";
  //const auto remarks_path = "/tmp/remarks.yml";

  //  std::cout << "Running optimization" << std::endl
  //            << "  before:  " << before_path << std::endl
  //            << "  after:   " << after_path << std::endl
  //            << "  remarks: " << remarks_path << std::endl;

  //  _rename_values();
  //  llvm_utils::module_to_file(before_path, *_module);

  const llvm::Triple module_triple(_module->getTargetTriple());
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
  if (with_unroll) {
    pass_manager.add(llvm::createLoopUnrollPass(3, 1000000000, -1, 0));
  }
  pass_manager.run(*_module);
}

void JitModule::_replace_loads_with_runtime_values() {
  _visit<llvm::LoadInst>([&](llvm::LoadInst& inst) {
    const auto runtime_pointer =
            std::dynamic_pointer_cast<const JitKnownRuntimePointer>(_get_runtime_value(inst.getPointerOperand()));
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

llvm::Function* JitModule::_create_function_declaration(const llvm::Function& function, const std::string& suffix) {
  if (auto fn = _module->getFunction(function.getName())) {
    return fn;
  }
  const auto declaration = llvm::Function::Create(llvm::cast<llvm::FunctionType>(function.getValueType()),
                                                  function.getLinkage(), function.getName() + suffix, _module.get());
  declaration->copyAttributesFrom(&function);
  return declaration;
}

llvm::Function* JitModule::_clone_function(const llvm::Function& function, const std::string& suffix) {
  const auto cloned_function = _create_function_declaration(function, suffix);

  // map personality function
  if (function.hasPersonalityFn()) {
    _visit<llvm::Function>(*function.getPersonalityFn(), [&](const auto& fn) {
      if (!_llvm_value_map.count(&fn)) {
        _llvm_value_map[&fn] = _create_function_declaration(fn);
      }
    });
  }

  // map functions called
  _visit<const llvm::Function>(function, [&](const auto& fn) {
    if (fn.isDeclaration() && !_llvm_value_map.count(&fn)) {
      _llvm_value_map[&fn] = _create_function_declaration(fn);
    }
  });

  // map global variables
  _visit<const llvm::GlobalVariable>(function, [&](auto& global) {
    if (!_llvm_value_map.count(&global)) {
      _llvm_value_map[&global] = _clone_global(global);
    }
  });

  // map function args
  auto arg = function.arg_begin();
  auto cloned_arg = cloned_function->arg_begin();
  for (; arg != function.arg_end() && cloned_arg != cloned_function->arg_end(); ++arg, ++cloned_arg) {
    cloned_arg->setName(arg->getName());
    _llvm_value_map[arg] = cloned_arg;
  }

  llvm::SmallVector<llvm::ReturnInst*, 8> returns;
  llvm::CloneFunctionInto(cloned_function, &function, _llvm_value_map, true, returns);

  if (function.hasPersonalityFn()) {
    cloned_function->setPersonalityFn(llvm::MapValue(function.getPersonalityFn(), _llvm_value_map));
  }

  return cloned_function;
}

llvm::GlobalVariable* JitModule::_clone_global(const llvm::GlobalVariable& global) {
  if (auto gb = _module->getGlobalVariable(global.getName())) {
    return gb;
  }

  const auto cloned_global = new llvm::GlobalVariable(*_module, global.getValueType(), global.isConstant(),
                                                      global.getLinkage(), nullptr, global.getName(), nullptr,
                                                      global.getThreadLocalMode(), global.getType()->getAddressSpace());

  cloned_global->copyAttributesFrom(&global);

  if (!global.isDeclaration()) {
    if (global.hasInitializer()) {
      cloned_global->setInitializer(llvm::MapValue(global.getInitializer(), _llvm_value_map));
    }

    llvm::SmallVector<std::pair<uint32_t, llvm::MDNode*>, 1> metadata_nodes;
    global.getAllMetadata(metadata_nodes);
    for (const auto& metadata_node : metadata_nodes) {
      cloned_global->addMetadata(metadata_node.first,
                                 *MapMetadata(metadata_node.second, _llvm_value_map, llvm::RF_MoveDistinctMDs));
    }
  }

  return cloned_global;
}

const JitRuntimePointer::Ptr& JitModule::_get_runtime_value(const llvm::Value* value) {
  //value->print(llvm::outs(), true);
  // std::cout << std::endl;
  // try serving from cache
  if (_runtime_values.count(value)) {
    return _runtime_values[value];
  }

  if (const auto constant_expr = llvm::dyn_cast<llvm::ConstantExpr>(value)) {
    if (constant_expr->getType()->isPointerTy()) {
      switch (constant_expr->getOpcode()) {
        case llvm::Instruction::IntToPtr:
          if (const auto address = llvm::dyn_cast<llvm::ConstantInt>(constant_expr->getOperand(0))) {
            _runtime_values[value] = std::make_shared<JitConstantRuntimePointer>(address->getValue().getLimitedValue());
          }
          break;
        default:
          break;
      }
    }
  } else if (const auto load_inst = llvm::dyn_cast<llvm::LoadInst>(value)) {
    if (load_inst->getType()->isPointerTy()) {
      if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
              _get_runtime_value(load_inst->getPointerOperand()))) {
        _runtime_values[value] = std::make_shared<JitDereferencedRuntimePointer>(base);
      }
    }
  } else if (const auto gep_inst = llvm::dyn_cast<llvm::GetElementPtrInst>(value)) {
    llvm::APInt offset(64, 0);
    if (gep_inst->accumulateConstantOffset(_module->getDataLayout(), offset)) {
      if (const auto base = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(
              _get_runtime_value(gep_inst->getPointerOperand()))) {
        _runtime_values[value] = std::make_shared<JitOffsetRuntimePointer>(base, offset.getLimitedValue());
      }
    }
  } else if (const auto bitcast_inst = llvm::dyn_cast<llvm::BitCastInst>(value)) {
    if (const auto base =
            std::dynamic_pointer_cast<const JitKnownRuntimePointer>(_get_runtime_value(bitcast_inst->getOperand(0)))) {
      _runtime_values[value] = std::make_shared<JitOffsetRuntimePointer>(base, 0L);
    }
  }

  if (!_runtime_values.count(value)) {
    _runtime_values[value] = std::make_shared<JitRuntimePointer>();
  }

  return _runtime_values[value];
}

template <typename T, typename U>
void JitModule::_visit(U& element, std::function<void(T&)> fn) {
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

template <typename T>
void JitModule::_visit(std::function<void(T&)> fn) {
  _visit(*_root_function, fn);
}

}  // namespace opossum