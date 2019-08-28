#pragma once

#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>

#include <stack>

#include "hyrise.hpp"
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
  virtual ~JitRTTIHelper() = default;
  virtual void _() const {}
};

class JitCodeSpecializer {
 public:
  explicit JitCodeSpecializer(std::shared_ptr<JitRepository>& repository = Hyrise::get().jit_repository);

  // Specializes the given function with the provided runtime information.
  // The root_function_name must be the mangled name of the function to be specialized. This function must exist in the
  // bitcode repository.
  // The runtime_this parameter is a JitRuntimePointer to the first pointer argument of this function. For member
  // functions this is the implicit "this" parameter.
  // The function is only specialized and the LLVM module with the specialized function is returned.
  std::unique_ptr<llvm::Module> specialize_function(
      const std::string& root_function_name,
      const std::shared_ptr<const JitRuntimePointer>& runtime_this = std::make_shared<JitRuntimePointer>(),
      const bool two_passes = false);

  // Specializes and compiles the given function with the provided runtime information.
  // The root_function_name must be the mangled name of the function to be specialized. This function must exist in the
  // bitcode repository.
  // The runtime_this parameter is a JitRuntimePointer to the first pointer argument of this function. For member
  // functions this is the implicit "this" parameter.
  // A function pointer to the compiled and executable function is returned.
  template <typename T>
  std::function<T> specialize_and_compile_function(
      const std::string& root_function_name,
      const std::shared_ptr<const JitRuntimePointer>& runtime_this = std::make_shared<JitRuntimePointer>(),
      const bool two_passes = false) {
    auto module = specialize_function(root_function_name, runtime_this, two_passes);
    _compiler.add_module(std::move(module));
    return _compiler.find_symbol<T>(_specialized_root_function_name);
  }

 private:
  // Undefined Behavior Sanitizer disabled here to prevent false positive through reinterpret_cast.
  __attribute__((no_sanitize("vptr"))) void _inline_function_calls(SpecializationContext& context) const;

  // Iterates over all load instruction in the function and tries to determine their value from the provided runtime
  // information. If this succeeds, the load instruction is replaced by a constant value.
  // Only boolean, integer, float and double values are substituted for now.
  void _perform_load_substitution(SpecializationContext& context) const;

  // Run the LLVM optimizer on the specialized module.
  void _optimize(SpecializationContext& context, const bool unroll_loops) const;

  // Creates a function declaration (i.e., a function signature without a function body) for the given function.
  llvm::Function* _create_function_declaration(SpecializationContext& context, const llvm::Function& function,
                                               const std::string& cloned_function_name) const;

  // Clones the root function function from the JitRepository to the current module.
  llvm::Function* _clone_root_function(SpecializationContext& context, const llvm::Function& function) const;

  // Clones a global variable from the JitRepository to the current module.
  llvm::GlobalVariable* _clone_global_variable(SpecializationContext& context,
                                               const llvm::GlobalVariable& global_variable) const;

  // Recursively traverses an element of the LLVM module hierarchy and calls the given lambda on each element of type T
  // found in the process.
  // This helper can e.g. be used to iterate over all instructions (or specific types of instructions) in a module,
  // function, or basic block, or to recursively traverse nested llvm::ConstExpr structures.
  template <typename T, typename U>
  void _visit(U& element, std::function<void(T&)> fn) const;

  std::shared_ptr<JitRepository> _repository;
  std::shared_ptr<llvm::LLVMContext> _llvm_context;
  JitCompiler _compiler;
  const std::string _specialized_root_function_name = "jit_module_root_function";
};

}  // namespace opossum
