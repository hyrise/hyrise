#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>

#include "../../../base_test.hpp"
#include "load_module.hpp"
#include "operators/jit_operator/specialization/jit_compiler.hpp"
#include "operators/jit_operator/specialization/jit_compiler.hpp"

namespace opossum {

class JitCompilerTest : public BaseTest {
 protected:
  void SetUp() override {
    // Global LLVM initializations
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();

    _context = std::make_shared<llvm::LLVMContext>();
    _module = load_module("src/test/llvm/add.ll", *_context);
  }

  std::shared_ptr<llvm::LLVMContext> _context;
  std::unique_ptr<llvm::Module> _module;
  std::string _add_fn_symbol = "_Z3addii";
};

TEST_F(JitCompilerTest, CompilesAddedModules) {
  JitCompiler compiler(_context);
  compiler.add_module(std::move(_module));
  auto add_fn = compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_fn_symbol);
  ASSERT_EQ(add_fn(1, 5), 1 + 5);
}

TEST_F(JitCompilerTest, ThrowsOnInvalidSymbolLookup) {
  JitCompiler compiler(_context);
  compiler.add_module(std::move(_module));
  ASSERT_THROW(compiler.find_symbol<void()>("some_nonexisting_symbol"), std::logic_error);
}

TEST_F(JitCompilerTest, AddsAndRemovesModules) {
  JitCompiler compiler(_context);
  ASSERT_THROW(compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_fn_symbol), std::logic_error);
  auto module_handle = compiler.add_module(std::move(_module));
  compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_fn_symbol);
  compiler.remove_module(module_handle);
  ASSERT_THROW(compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_fn_symbol), std::logic_error);
}

}  // namespace opossum
