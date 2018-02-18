#pragma once

#include <llvm/IR/Module.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.hpp"

namespace opossum {

extern char jit_llvm_bundle;
extern size_t jit_llvm_bundle_size;

// Singleton
class IRRepository : private Noncopyable {
 public:
  static IRRepository& get();

  const llvm::Function* get_function(const std::string& name) const;
  const llvm::Function* get_vtable_entry(const std::string& class_name, const size_t index) const;

  std::shared_ptr<llvm::LLVMContext> llvm_context() const;

 private:
  IRRepository();

  void _dump(std::ostream& os) const;

  std::shared_ptr<llvm::LLVMContext> _llvm_context;
  std::unique_ptr<llvm::Module> _module;
  std::unordered_map<std::string, const llvm::Function*> _functions;
  std::unordered_map<std::string, std::vector<const llvm::Function*>> _vtables;

  const std::string vtable_prefix = "_ZTV";
};

}  // namespace opossum
