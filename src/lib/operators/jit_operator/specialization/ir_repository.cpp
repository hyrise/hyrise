#include "ir_repository.hpp"

#include <boost/algorithm/string/predicate.hpp>

#include <llvm/ADT/SetVector.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DebugInfo.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "utils/llvm_utils.hpp"

namespace opossum {

// singleton
IRRepository& IRRepository::get() {
  static IRRepository instance;
  return instance;
}

const llvm::Function* IRRepository::get_function(const std::string& name) const {
  return _functions.count(name) ? _functions.at(name) : nullptr;
}

const llvm::Function* IRRepository::get_vtable_entry(const std::string& class_name, const size_t index) const {
  const auto vtable_name = vtable_prefix + class_name;
  if (_vtables.count(vtable_name) && _vtables.at(vtable_name).size() > index) {
    return _vtables.at(vtable_name)[index];
  }
  return nullptr;
}

std::shared_ptr<llvm::LLVMContext> IRRepository::llvm_context() const { return _llvm_context; }

IRRepository::IRRepository()
    : _llvm_context{std::make_shared<llvm::LLVMContext>()},
      _module{llvm_utils::module_from_string(std::string(&jit_llvm_bundle, jit_llvm_bundle_size), *_llvm_context)} {
  llvm::StripDebugInfo(*_module);

  // extract functions
  for (const auto& function : *_module) {
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
      if (const auto const_array = llvm::dyn_cast<llvm::ConstantArray>(global.getInitializer()->getOperand(0))) {
        std::vector<const llvm::Function*> vtable;
        for (uint32_t index = 2; index < const_array->getNumOperands(); ++index) {
          vtable.push_back(_functions[const_array->getOperand(index)->getOperand(0)->getName().str()]);
        }
        _vtables[global.getName().str()] = vtable;
      }
    }
  }

  // _dump(std::cout);
}

void IRRepository::_dump(std::ostream& os) const {
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
