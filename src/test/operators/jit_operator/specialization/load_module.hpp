#pragma once

#include <memory>

#include <llvm/IR/Module.h>

namespace opossum {

std::unique_ptr<llvm::Module> load_module(const std::string path, llvm::LLVMContext& context);

} // namespace opossum
