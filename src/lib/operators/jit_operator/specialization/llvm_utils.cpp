#include "llvm_utils.hpp"

#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/SourceMgr.h>

#include "utils/assert.hpp"

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
