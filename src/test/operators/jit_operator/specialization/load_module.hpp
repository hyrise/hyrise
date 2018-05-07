#pragma once

namespace opossum {

std::unique_ptr<llvm::Module> load_module(const std::string path, llvm::LLVMContext& context) {
  llvm::SMDiagnostic diagnostic;
#ifdef __APPLE__
  std::string suffix = ".mac";
#else
  std::string suffix = ".linux";
#endif
  return llvm::parseIRFile(path + suffix, diagnostic, context);
}

} // namespace opossum
