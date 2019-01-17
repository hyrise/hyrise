#pragma once

#include <memory>

namespace llvm {
class Module;
class LLVMContext;
}  // namespace llvm

namespace opossum {

// Parses an LLVM module from a string.
// This method can be used to transform embedded LLVM bitcode strings into the LLVM in-memory representation that can be
// operated on with APIs provided by LLVM
std::unique_ptr<llvm::Module> parse_llvm_module(const std::string& module_string, llvm::LLVMContext& context);

}  // namespace opossum
