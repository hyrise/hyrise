#pragma once

#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

#include <stack>

#include "jit_repository.hpp"
#include "jit_compiler.hpp"
#include "jit_runtime_pointer.hpp"

namespace opossum {

class JitRTTIHelper {
 private:
  virtual void _() const {}
};

class JitModule {
 public:
  explicit JitModule(const std::string& root_function_name);

  void specialize(const JitRuntimePointer::Ptr& runtime_this);

  template <typename T>
  std::function<T> compile() {
    const auto function_name = _root_function_name + "_";

    // note: strangely, llvm::verifyModule returns false for valid modules
    Assert(!llvm::verifyModule(*_module, &llvm::dbgs()), "Module is invalid.");

    _compiler.add_module(std::move(_module));
    return _compiler.find_symbol<T>(function_name);
  }

 private:
  bool _specialize(const JitRuntimePointer::Ptr& runtime_this);

  void _optimize();

  void _replace_loads_with_runtime_values();

  llvm::Function* _create_function_declaration(const llvm::Function& function, const std::string& suffix = "");

  llvm::Function* _clone_function(const llvm::Function& function, const std::string& suffix = "");

  llvm::GlobalVariable* _clone_global(const llvm::GlobalVariable& global);

  const JitRuntimePointer::Ptr& _get_runtime_value(const llvm::Value* value);

  void _rename_values();

  template <typename T, typename U>
  void _visit(U& function, std::function<void(T&)> fn);

  template <typename T>
  void _visit(std::function<void(T&)> fn);

  const JitRepository& _repository;
  std::unique_ptr<llvm::Module> _module;
  JitCompiler _compiler;

  const std::string _root_function_name;
  llvm::Function* _root_function;
  bool _modified;
  llvm::ValueToValueMapTy _llvm_value_map;
  std::unordered_map<const llvm::Value*, JitRuntimePointer::Ptr> _runtime_values;
};

}  // namespace opossum
