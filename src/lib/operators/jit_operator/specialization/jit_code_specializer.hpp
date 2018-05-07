#pragma once

#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

#include <stack>

#include "jit_compiler.hpp"
#include "jit_repository.hpp"
#include "jit_runtime_pointer.hpp"
#include "llvm_extensions.hpp"

namespace opossum {

/* This is a helper class to retrieve the class name from a raw pointer using RTTI.
 * The class has a single virtual method.
 * A raw pointer can be cast to a pointer of this class to be used with RTTI.
 */
class JitRTTIHelper {
 private:
  virtual void _() const {}
};

class JitCodeSpecializer {
 public:
  JitCodeSpecializer();

  template <typename T>
  std::function<T> specialize_function(const std::string& root_function_name,
                                       const std::shared_ptr<const JitRuntimePointer>& runtime_this,
                                       const bool two_passes) {
    _repository.specialization_mutex().lock();
    _specialize_function_impl(root_function_name, runtime_this, two_passes);
    _repository.specialization_mutex().unlock();
    return _compiler.find_symbol<T>(root_function_name + "_");
  }

 private:
  void _specialize_function_impl(const std::string& root_function_name,
                                 const std::shared_ptr<const JitRuntimePointer>& runtime_this, const bool two_passes);

  void _inline_function_calls(SpecializationContext& context, const bool two_passes) const;

  void _perform_load_substitution(SpecializationContext& context) const;

  void _optimize(SpecializationContext& context, const bool unroll_loops) const;

  llvm::Function* _create_function_declaration(SpecializationContext& context, const llvm::Function& function,
                                               const std::string& suffix = "") const;

  llvm::Function* _clone_function(SpecializationContext& context, const llvm::Function& function,
                                  const std::string& suffix = "") const;

  llvm::GlobalVariable* _clone_global_variable(SpecializationContext& context,
                                               const llvm::GlobalVariable& global_variable) const;

  // Recursively traverses an element of the LLVM module hierarchy and calls the given lambda on each element of type T
  // found in the process.
  // This helper can e.g. be used to iterate over all instructions (or specific types of instructions) in a module,
  // function, or basic block, or to recursively traverse nested llvm::ConstExpr structures.
  template <typename T, typename U>
  void _visit(U& element, std::function<void(T&)> fn) const;

  JitRepository& _repository;
  const std::shared_ptr<llvm::LLVMContext> _llvm_context;
  JitCompiler _compiler;
};

}  // namespace opossum
