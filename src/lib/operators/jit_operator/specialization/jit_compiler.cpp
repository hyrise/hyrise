#include "jit_compiler.hpp"

#include <llvm/ExecutionEngine/RTDyldMemoryManager.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/TargetSelect.h>

namespace opossum {

JitCompiler::JitCompiler(const std::shared_ptr<llvm::LLVMContext>& context)
    : _context{context},
      _target_machine{!llvm::InitializeNativeTarget() && !llvm::InitializeNativeTargetAsmPrinter()
                          ? llvm::EngineBuilder().selectTarget()
                          : nullptr},
      _data_layout{_target_machine->createDataLayout()},
      _object_layer{[]() { return std::make_shared<llvm::SectionMemoryManager>(); }},
      _compile_layer{_object_layer, llvm::orc::SimpleCompiler(*_target_machine)},
      _cxx_runtime_overrides{[this](const std::string& symbol) { return _mangle(symbol); }} {
  // make exported symbols of the current process available for execution
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

JitCompiler::~JitCompiler() { _cxx_runtime_overrides.runDestructors(); }

JitCompiler::ModuleHandle JitCompiler::add_module(const std::shared_ptr<llvm::Module>& module) {
  const auto resolver = llvm::orc::createLambdaResolver(
      [&](const std::string& name) -> llvm::JITSymbol {
        if (auto symbol = _compile_layer.findSymbol(name, true)) {
          return symbol;
        } else if (auto symbol = _cxx_runtime_overrides.searchOverrides(name)) {
          return symbol;
        } else {
          return llvm::JITSymbol(llvm::RTDyldMemoryManager::getSymbolAddressInProcess(name),
                                 llvm::JITSymbolFlags::Exported);
        }
      },
      [](const std::string& name) {
        return llvm::JITSymbol(llvm::RTDyldMemoryManager::getSymbolAddressInProcess(name),
                               llvm::JITSymbolFlags::Exported);
      });

  const auto handle = _handle_error(_compile_layer.addModule(std::move(module), std::move(resolver)));
  _modules.push_back(handle);
  return handle;
}

void JitCompiler::remove_module(const JitCompiler::ModuleHandle& handle) {
  _modules.erase(std::find(_modules.cbegin(), _modules.cend(), handle));
  _handle_error(_compile_layer.removeModule(handle));
}

llvm::TargetMachine& JitCompiler::target_machine() const { return *_target_machine; }

const llvm::DataLayout& JitCompiler::data_layout() const { return _data_layout; }

const std::string JitCompiler::_mangle(const std::string& name) const {
  std::string mangled_name;
  llvm::raw_string_ostream mangled_name_stream(mangled_name);
  llvm::Mangler::getNameWithPrefix(mangled_name_stream, name, _data_layout);
  return mangled_name;
}

void JitCompiler::_handle_error(llvm::Error error) {
  if (error) {
    llvm::logAllUnhandledErrors(std::move(error), llvm::errs(), "");
    llvm_unreachable("");
  }
}

}  // namespace opossum
