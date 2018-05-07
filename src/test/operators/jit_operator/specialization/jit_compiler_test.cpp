#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/jit_compiler.hpp"

namespace opossum {

class JitCompilerTest : public BaseTest {
 protected:
  void SetUp() override {
    _context = std::make_shared<llvm::LLVMContext>();
    llvm::SMDiagnostic error;
    _module = llvm::parseIRFile("src/test/llvm/add.ll", error, *_context);
  }

  std::shared_ptr<llvm::LLVMContext> _context;
  std::unique_ptr<llvm::Module> _module;
  std::string _add_symbol = "_Z3addii";
};

TEST_F(JitCompilerTest, CompilesAddedModules) {
  JitCompiler compiler(_context);
  compiler.add_module(std::move(_module));
  auto add_fn = compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_symbol);
  ASSERT_EQ(add_fn(1, 5), 1 + 5);
}

TEST_F(JitCompilerTest, ThrowsOnInvalidSymbolLookup) {
  JitCompiler compiler(_context);
  compiler.add_module(std::move(_module));
  ASSERT_THROW(compiler.find_symbol<void()>("some_nonexisting_symbol"), std::logic_error);
}

TEST_F(JitCompilerTest, AddsAndRemovesModules) {
  JitCompiler compiler(_context);

  ASSERT_THROW(compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_symbol), std::logic_error);
  auto module_handle = compiler.add_module(std::move(_module));
  compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_symbol);
  compiler.remove_module(module_handle);
  ASSERT_THROW(compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_symbol), std::logic_error);
}

}  // namespace opossum
