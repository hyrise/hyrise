#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/jit_compiler.hpp"
#include "operators/jit_operator/specialization/llvm_utils.hpp"

namespace opossum {

// These symbols provide access to the LLVM bitcode string embedded into the hyriseTest binary during compilation.
// The symbols are defined in an assembly file that is generated in EmbedLLVM.cmake and linked into the binary.
// Please refer to EmbedLLVM.cmake / src/test/CMakeLists.txt for further details of the bitcode generation and
// embedding process.
extern char llvm_extensions_test_module;
extern size_t llvm_extensions_test_module_size;

class LLVMExtensionTest : public BaseTest {
 protected:
  void SetUp() override {

  }
};

TEST_F(LLVMExtensionTest, Test1) {
  ASSERT_TRUE(false);
}

}  // namespace opossum
