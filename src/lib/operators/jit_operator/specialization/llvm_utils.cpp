#include "llvm_utils.hpp" // NEEDEDINCLUDE

#include <llvm/IRReader/IRReader.h> // NEEDEDINCLUDE
#include <llvm/IR/Module.h> // NEEDEDINCLUDE
#include <llvm/Support/Error.h> // NEEDEDINCLUDE
#include <llvm/Support/SourceMgr.h> // NEEDEDINCLUDE

#include "utils/assert.hpp" // NEEDEDINCLUDE

namespace opossum {

std::unique_ptr<llvm::Module> parse_llvm_module(const std::string& module_string, llvm::LLVMContext& context) {
  llvm::SMDiagnostic error;
  const auto buffer = llvm::MemoryBuffer::getMemBuffer(llvm::StringRef(module_string));
  auto module = llvm::parseIR(*buffer, error, context);

  if (error.getFilename() != "") {
    error.print("", llvm::errs(), true);
    Fail("An LLVM error occured while parsing the embedded LLVM bitcode.");
  }

  return module;
}

}  // namespace opossum
