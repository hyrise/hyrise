#include "jit_repository.hpp"

#include <boost/algorithm/string/predicate.hpp>

#include <llvm/ADT/SetVector.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DebugInfo.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Transforms/Utils/Cloning.h>

namespace opossum {

// singleton
JitRepository& JitRepository::get() {
  static JitRepository instance;
  return instance;
}

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

JitRepository::JitRepository()
    : _llvm_context{std::make_shared<llvm::LLVMContext>()},
      _module{_parse_module(std::string(&jit_llvm_bundle, jit_llvm_bundle_size), *_llvm_context)} {
  llvm::StripDebugInfo(*_module);

  // extract functions
  for (auto& function : *_module) {
    const auto function_name = function.getName().str();
    if (!function.isDeclaration()) {
      _functions[function_name] = &function;
    }
  }

  // extract vtables
  for (const auto& global : _module->globals()) {
    if (boost::starts_with(global.getName().str(), vtable_prefix)) {
      if (!global.hasInitializer()) {
        continue;
      }
      if (auto array = llvm::dyn_cast<llvm::ConstantArray>(global.getInitializer()->getOperand(0))) {
        std::vector<llvm::Function*> vtable;
        for (uint32_t index = 2; index < array->getNumOperands(); ++index) {
          vtable.push_back(_functions[array->getOperand(index)->getOperand(0)->getName().str()]);
        }
        _vtables[global.getName().str()] = vtable;
      }
    }
  }

  // _dump(std::cout);
}

std::unique_ptr<llvm::Module> JitRepository::_parse_module(const std::string& module_string,
                                                          llvm::LLVMContext& context) const {
  llvm::SMDiagnostic error;
  const auto buffer = llvm::MemoryBuffer::getMemBuffer(llvm::StringRef(module_string));
  auto module = llvm::parseIR(*buffer, error, context);

  if (error.getFilename() != "") {
    error.print("", llvm::errs(), true);
    Fail("an LLVM error occured while parsing the embedded LLVM-IR");
  }
  return module;
}

void JitRepository::_dump(std::ostream& os) const {
  os << "IR Repository" << std::endl;
  os << "--- functions ---" << std::endl;
  for (const auto& fn : _functions) {
    os << fn.first << std::endl;
  }

  os << std::endl << "--- vtables ---" << std::endl;
  for (const auto& vtable : _vtables) {
    os << vtable.first << std::endl;
    for (const auto& function : vtable.second) {
      if (function) {
        os << "  " << function->getName().str() << std::endl;
      } else {
        os << "  -" << std::endl;
      }
    }
    os << std::endl;
  }
}

}  // namespace opossum
