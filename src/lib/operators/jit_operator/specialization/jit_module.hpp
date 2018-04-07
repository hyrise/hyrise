#pragma once

#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

#include <operators/jit_operator/specialization/utils/llvm_utils.hpp>
#include <stack>

#include "jit_compiler.hpp"
#include "jit_repository.hpp"
#include "jit_runtime_pointer.hpp"

namespace opossum {

class JitRTTIHelper {
 private:
  virtual void _() const {}
};

class JitModule {
 public:
  JitModule();

  template <typename T>
  std::function<T> specialize(const std::string& root_function_name, const JitRuntimePointer::Ptr& runtime_this, const bool second_pass = false) {
    const auto function_name = root_function_name + "_";
    _specialize_impl(root_function_name, runtime_this, second_pass);
    _compiler.add_module(std::move(_module));
    return _compiler.find_symbol<T>(function_name);
  }

 private:
  void _specialize_impl(const std::string& root_function_name, const JitRuntimePointer::Ptr& runtime_this, const bool second_pass = false);

  void _optimize(bool with_unroll);

  void _resolve_virtual_calls(const bool second_pass);

  void _replace_loads_with_runtime_values();

  llvm::Function* _create_function_declaration(const llvm::Function& function, const std::string& suffix = "");

  llvm::Function* _clone_function(const llvm::Function& function, const std::string& suffix = "");

  llvm::GlobalVariable* _clone_global(const llvm::GlobalVariable& global);

  const JitRuntimePointer::Ptr& _get_runtime_value(const llvm::Value* value);

  template <typename T, typename U>
  void _visit(U& function, std::function<void(T&)> fn);

  template <typename T>
  void _visit(std::function<void(T&)> fn);

  const JitRepository& _repository;
  const std::shared_ptr<llvm::LLVMContext> _llvm_context;
  std::unique_ptr<llvm::Module> _module;
  JitCompiler _compiler;

  llvm::Function* _root_function;
  llvm::ValueToValueMapTy _llvm_value_map;
  std::unordered_map<const llvm::Value*, JitRuntimePointer::Ptr> _runtime_values;
};

}  // namespace opossum
