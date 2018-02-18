#pragma once

#include <boost/algorithm/string/predicate.hpp>

#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/IR/Module.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/FileSystem.h>
#include <llvm/Support/Regex.h>

#include <fstream>

#include "error_utils.hpp"

namespace opossum {

struct llvm_utils {
  static void module_to_file(const std::string& path, const llvm::Module& module) {
    if (boost::ends_with(path, ".ll")) {
      std::string content;
      llvm::raw_string_ostream sos(content);
      module.print(sos, nullptr, false, true);
      std::ofstream ofs(path);
      ofs << content;
    } else if (boost::ends_with(path, ".bc")) {
      std::error_code error_code;
      llvm::raw_fd_ostream os(path, error_code, llvm::sys::fs::F_None);
      llvm::WriteBitcodeToFile(&module, os);
      os.flush();
      error_utils::handle_error(error_code.value());
    } else {
      throw std::invalid_argument("invalid file extension for LLVM bitcode file");
    }
  }

  static std::unique_ptr<llvm::Module> module_from_file(const std::string& path, llvm::LLVMContext& context) {
    llvm::SMDiagnostic error;
    auto module = llvm::parseIRFile(path, error, context);
    error_utils::handle_error(error);
    return module;
  }

  static std::unique_ptr<llvm::Module> module_from_string(const std::string& str, llvm::LLVMContext& context) {
    llvm::SMDiagnostic error;
    const auto buffer = llvm::MemoryBuffer::getMemBuffer(llvm::StringRef(str));
    auto module = llvm::parseIR(*buffer, error, context);
    error_utils::handle_error(error);
    return module;
  }
};

}  // namespace opossum
