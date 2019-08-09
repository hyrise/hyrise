#include "jit_compiler.hpp"

#include <llvm/ExecutionEngine/RTDyldMemoryManager.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Passes/PassBuilder.h>

namespace opossum {

JitCompiler::JitCompiler()
    : _target_machine{llvm::EngineBuilder().selectTarget()},
      _data_layout{_target_machine->createDataLayout()},
      _resolver{createLegacyLookupResolver(
          _execution_session,
          [&](const std::string& name) -> llvm::JITSymbol {
            // We first try to locate symbols in the modules added to the JIT, then in runtime overrides and finally in
            // the running process.
            if (auto compile_layer_symbol = _compile_layer.findSymbol(name, true)) {
              return compile_layer_symbol;
            } else if (auto runtime_override_symbol = _cxx_runtime_overrides.searchOverrides(name)) {
              return runtime_override_symbol;
            } else {
              return llvm::JITSymbol(llvm::RTDyldMemoryManager::getSymbolAddressInProcess(name),
                                     llvm::JITSymbolFlags::Exported);
            }
          },
          [](llvm::Error error) { _handle_error(std::move(error)); })},
      _object_layer{_execution_session,
                    [&](llvm::orc::VModuleKey module_key) {
                      return llvm::orc::RTDyldObjectLinkingLayer::Resources{
                          std::make_shared<llvm::SectionMemoryManager>(), _resolver};
                    }},
      _compile_layer{_object_layer, llvm::orc::SimpleCompiler(*_target_machine)},
      _cxx_runtime_overrides{[this](const std::string& symbol) { return _mangle(symbol); }} {
  // Make exported symbols of the current process available to the JIT
  llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

JitCompiler::~JitCompiler() {
  // Run destructors for static global objects in jitted modules before destructing the JIT itself
  _cxx_runtime_overrides.runDestructors();
}

llvm::orc::VModuleKey JitCompiler::add_module(std::unique_ptr<llvm::Module> module) {
  // Add the module to the JIT with a new VModuleKey.
  auto module_key = _execution_session.allocateVModule();
  _handle_error(_compile_layer.addModule(module_key, std::move(module)));
  _modules.push_back(module_key);
  return module_key;
}

void JitCompiler::remove_module(llvm::orc::VModuleKey module_key) {
  _modules.erase(std::find(_modules.cbegin(), _modules.cend(), module_key));
  _handle_error(_compile_layer.removeModule(module_key));
}

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
    Fail("An LLVM error occured");
  }
}

}  // namespace opossum
