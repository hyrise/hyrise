#pragma once

#include <llvm/IR/Module.h>

#include <memory>

namespace opossum {

std::unique_ptr<llvm::Module> load_module(const std::string path, llvm::LLVMContext& context);

}  // namespace opossum
