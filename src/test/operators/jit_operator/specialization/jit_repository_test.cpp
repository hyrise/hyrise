#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/jit_repository.hpp"

namespace opossum {

class JitRepositoryTest : public BaseTest {
 protected:
  void SetUp() override {
    _context = std::make_shared<llvm::LLVMContext>();
    llvm::SMDiagnostic error;
    _module = llvm::parseIRFile("src/test/llvm/virtual_methods.ll", error, *_context);
    //_repository = JitRepository(std::move(_module), _context);
  }

  std::shared_ptr<llvm::LLVMContext> _context;
  std::unique_ptr<llvm::Module> _module;
  //JitRepository _repository;
};

TEST_F(JitRepositoryTest, CompilesAddedModules) {

}

TEST_F(JitRepositoryTest, ThrowsOnInvalidSymbolLookup) {

}

TEST_F(JitRepositoryTest, AddsAndRemovesModules) {

}

}  // namespace opossum
