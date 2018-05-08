#pragma once

#include <llvm/IR/Module.h>

namespace opossum {

std::unique_ptr<llvm::Module> parse_llvm_module(const std::string& module_string, llvm::LLVMContext& context);

}  // namespace opossum
