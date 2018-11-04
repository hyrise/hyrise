#pragma once

#include <llvm/IR/Module.h>
#include <llvm/IRReader/IRReader.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "types.hpp"
#include "utils/singleton.hpp"

namespace opossum {

// These symbols provide access to the LLVM bitcode string embedded into the Hyrise library during compilation.
// The symbols are defined in an assembly file that is generated in EmbedLLVM.cmake and linked into the lib.
// Please refer to EmbedLLVM.cmake for further details of the bitcode generation and embedding process.
extern char jit_llvm_bundle;
extern size_t jit_llvm_bundle_size;

/* The JitRepository is responsible for organizing and managing LLVM bitcode for code specialization.
 * The bitcode is parsed from a bitcode string embedded into the library.
 * Two data structures are maintained to provide access to 1) all functions by name, and 2) all virtual functions by
 * class name and vtable index.
 * The repository is implemented as a singleton. As such it also provides the global LLVMContext, calls global
 * initializations of the LLVM framework, and provides a mutex for synchronized access to LLVM data structures.
 */
class JitRepository : public Singleton<JitRepository> {
 public:
  // Create a repository from the given module string
  explicit JitRepository(const std::string& module_string);

  // Returns a virtual function implementation given a function name
  llvm::Function* get_function(const std::string& name) const;

  // Returns a virtual function implementation given a class name and the vtable index
  llvm::Function* get_vtable_entry(const std::string& class_name, const size_t index) const;

  std::shared_ptr<llvm::LLVMContext> llvm_context() const;
  std::shared_ptr<llvm::Module> module() const;
  std::mutex& specialization_mutex();

 private:
  JitRepository();

  friend class Singleton;

  std::shared_ptr<llvm::LLVMContext> _llvm_context;
  std::shared_ptr<llvm::Module> _module;
  std::unordered_map<std::string, llvm::Function*> _functions;
  std::unordered_map<std::string, std::vector<llvm::Function*>> _vtables;
  std::mutex _specialization_mutex;

  const std::string vtable_prefix = "_ZTV";
};

}  // namespace opossum
