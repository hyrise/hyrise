#pragma once

#include <llvm/Support/Error.h>
#include <llvm/Support/SourceMgr.h>

namespace opossum {

struct error_utils {
  template <typename T>
  static T handle_error(llvm::Expected<T> value_or_error) {
    if (value_or_error) {
      return value_or_error.get();
    }

    llvm::logAllUnhandledErrors(value_or_error.takeError(), llvm::errs(), "");
    llvm_unreachable("");
  }

  static void handle_error(llvm::Error error) {
    if (error) {
      llvm::logAllUnhandledErrors(std::move(error), llvm::errs(), "");
      llvm_unreachable("");
    }
  }

  static void handle_error(const llvm::SMDiagnostic& error) {
    if (error.getFilename() != "") {
      error.print("", llvm::errs(), true);
      llvm_unreachable("");
    }
  }

  static void handle_error(const int error) {
    if (error) {
      llvm_unreachable("");
    }
  }

  static void handle_error(const std::error_code& error) {
    if (error) {
      std::cerr << error.message() << std::endl;
      llvm_unreachable("");
    }
  }
};

}  // namespace opossum
