#include "jit_repository.hpp"

#include <boost/algorithm/string/predicate.hpp>

#include <llvm/ADT/SetVector.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DebugInfo.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "llvm_utils.hpp"

namespace opossum {

llvm::Function* JitRepository::get_function(const std::string& name) const {
  return _functions.count(name) ? _functions.at(name) : nullptr;
}

llvm::Function* JitRepository::get_vtable_entry(const std::string& class_name, const size_t index) const {
  const auto vtable_name = vtable_prefix + class_name;
  if (_vtables.count(vtable_name) && _vtables.at(vtable_name).size() > index) {
    return _vtables.at(vtable_name)[index];
  }
  return nullptr;
}

std::shared_ptr<llvm::LLVMContext> JitRepository::llvm_context() const { return _llvm_context; }

std::mutex& JitRepository::specialization_mutex() { return _specialization_mutex; }

JitRepository::JitRepository() : JitRepository(std::string(&jit_llvm_bundle, jit_llvm_bundle_size)) {}

JitRepository::JitRepository(const std::string& module_string)
    : _llvm_context{std::make_shared<llvm::LLVMContext>()}, _module{parse_llvm_module(module_string, *_llvm_context)} {
  // Global LLVM initializations
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();

  llvm::StripDebugInfo(*_module);

  // Extract functions
  for (auto& function : *_module) {
    const auto function_name = function.getName().str();
    if (!function.isDeclaration()) {
      _functions[function_name] = &function;
    }
  }

  // Extract virtual functions
  for (const auto& global : _module->globals()) {
    if (boost::starts_with(global.getName().str(), vtable_prefix)) {
      if (!global.hasInitializer()) {
        continue;
      }
      if (auto array = llvm::dyn_cast<llvm::ConstantArray>(global.getInitializer()->getOperand(0))) {
        std::vector<llvm::Function*> vtable;
        // LLVM vtables do not contain function references but other RTTI related information at indices 0 and 1
        for (uint32_t index = 2; index < array->getNumOperands(); ++index) {
          vtable.push_back(_functions[array->getOperand(index)->getOperand(0)->getName().str()]);
        }
        _vtables[global.getName().str()] = vtable;
      }
    }
  }
}

std::shared_ptr<JitRepository> JitRepository::_make_shared() {
  // We need this struct because it enables the shared pointer to create a JitRepository since it provides a public
  // constructor. With this solution we don't need to make JitRepository's standard constructor public.
  struct MakeSharedEnabler : public JitRepository {};
  return std::make_shared<MakeSharedEnabler>();
}

}  // namespace opossum
