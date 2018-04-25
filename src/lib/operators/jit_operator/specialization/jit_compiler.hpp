#pragma once

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/CompileOnDemandLayer.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/ExecutionUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/IRTransformLayer.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/IR/Mangler.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Error.h>
#include <llvm/Target/TargetMachine.h>
#include <utils/assert.hpp>

namespace opossum {

/* A wrapper for the just-in-time features provided by the LLVM framework.
 * The wrapper only provides a minimal compiler configuration without an optimization layer - modules must be optimized
 * outside the JitCompiler.
 * The interface of the JitCompiler is rather simple: Modules (LLVM's compilation unit) can be added and removed from
 * the compiler; the compiler takes ownership of all modules.
 * When a module is added to the compiler, it is immediately compiled to machine code.
 * This machine code can be accessed for a symbol defined by a module by passing the MANGLED! name of the symbol to the
 * find_symbol function.
 * By providing the correct template parameters, the function returns a properly-typed function pointer that can be
 * called like a regular function. The caller is responsible for providing template arguments that match the symbols
 * signature in the LLVM module.
 */
class JitCompiler {
 protected:
  using ObjectLayer = llvm::orc::RTDyldObjectLinkingLayer;
  using CompileLayer = llvm::orc::IRCompileLayer<ObjectLayer, llvm::orc::SimpleCompiler>;
  using ModuleHandle = CompileLayer::ModuleHandleT;

 public:
  explicit JitCompiler(const std::shared_ptr<llvm::LLVMContext>& context);
  ~JitCompiler();

  // Adds a module to the LLVM just-in-time compiler and compiles it to machine code immediately
  ModuleHandle add_module(const std::shared_ptr<llvm::Module>& module);

  // Removes a module from the LLVM just-in-time compiler and cleans up all related resources
  void remove_module(const ModuleHandle& handle);

  // Locates a symbol in one of the modules previously added to the JIT by its MANGLED name
  template <typename T>
  std::function<T> find_symbol(const std::string& name) {
    const auto target_address = _handle_error(_compile_layer.findSymbol(_mangle(name), true).getAddress());

    Assert(target_address, "Symbol " + name + " could not be found");
    return reinterpret_cast<T*>(target_address);
  }

  // Exports the data layout of the JIT to provide other code specialization components with target-specific information
  // (e.g., data types sizes)
  const llvm::DataLayout& data_layout() const;

 private:
  const std::string _mangle(const std::string& name) const;

  // Unpacks LLVM results and fails on errors
  template <typename T>
  T _handle_error(llvm::Expected<T> value_or_error) {
    if (value_or_error) {
      return value_or_error.get();
    }

    llvm::logAllUnhandledErrors(value_or_error.takeError(), llvm::errs(), "");
    Fail("An LLVM error occured");
  }

  // Checks LLVM error codes and fails on actual errors
  void _handle_error(llvm::Error error);

  const std::shared_ptr<llvm::LLVMContext> _context;
  const std::unique_ptr<llvm::TargetMachine> _target_machine;
  const llvm::DataLayout _data_layout;
  ObjectLayer _object_layer;
  CompileLayer _compile_layer;
  llvm::orc::LocalCXXRuntimeOverrides _cxx_runtime_overrides;
  std::vector<ModuleHandle> _modules;
};

}  // namespace opossum
