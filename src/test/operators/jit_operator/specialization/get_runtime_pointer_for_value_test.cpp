#include <llvm/IR/IRBuilder.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/llvm_extensions.hpp"

namespace opossum {

/* The code specialization engine uses the JitRuntimePointer class hierarchy to simulate LLVM pointer operations.
 * The GetRuntimePointerForValue function, which is tested in the following test cases, returns the physical runtime
 * memory location for an abstract LLVM value, if possible.
 * The test cases creates a simple bitcode function with a number of pointer instructions. The physical address of
 * the first function argument is given to the GetRuntimePointerForValue function. Based on this information, it should
 * be able to compute the physical runtime addresses for the other bitcode values by simulating the coresponding
 * instructions.
 */
class GetRuntimePointerForValueTest : public BaseTest {
 protected:
  void SetUp() override {
    _llvm_context = std::make_shared<llvm::LLVMContext>();

    // Create an LLVM module with the following bitcode
    //
    // define void @foo(i64*** %0) {
    // entry:
    //   %1 = getelementptr i64**, i64*** %0, i32 1
    //   %2 = load i64**, i64*** %0
    //   %3 = bitcast i64*** %0 to i32*
    //   %4 = getelementptr i32, i32* %3, i32 2
    //   %5 = getelementptr i32, i32* %4, i32 3
    //   %6 = getelementptr i64*, i64** %2, i32 4
    //   %7 = load i64*, i64** %2
    // }
    _context.module = std::make_unique<llvm::Module>("test_module", *_llvm_context);
    auto argument_type =
        llvm::PointerType::get(llvm::PointerType::get(llvm::Type::getInt64PtrTy(*_llvm_context), 0), 0);
    auto foo_fn = llvm::dyn_cast<llvm::Function>(
        _context.module->getOrInsertFunction("foo", llvm::Type::getVoidTy(*_llvm_context), argument_type));

    // Create a single basic block as the function's body
    auto block = llvm::BasicBlock::Create(*_llvm_context, "entry", foo_fn);
    llvm::IRBuilder<> builder(block);

    // Create instructions
    _value_0 = foo_fn->arg_begin();
    _value_1 = builder.CreateConstGEP1_32(_value_0, 1);
    _value_2 = builder.CreateLoad(_value_0);
    _value_3 = builder.CreateBitCast(_value_0, llvm::Type::getInt32PtrTy(*_llvm_context));
    _value_4 = builder.CreateConstGEP1_32(_value_3, 2);
    _value_5 = builder.CreateConstGEP1_32(_value_4, 3);
    _value_6 = builder.CreateConstGEP1_32(_value_2, 4);
    _value_7 = builder.CreateLoad(_value_2);
  }

  // Assert that the address returned from a JitRuntimePointer equals the expected address
  void assert_address_eq(const std::shared_ptr<const JitRuntimePointer>& runtime_pointer,
                         const uint64_t expected_address) const {
    auto known_runtime_pointer = std::dynamic_pointer_cast<const JitKnownRuntimePointer>(runtime_pointer);
    ASSERT_NE(known_runtime_pointer, nullptr);
    ASSERT_TRUE(known_runtime_pointer->is_valid());
    ASSERT_EQ(known_runtime_pointer->address(), expected_address);
  }

  __attribute__((no_sanitize_address)) void bitcode_pointer_test() {
    // We need this method so that we can set the no-sanitize attribute on it. We don't want the sanitizer to run here
    // because the pointers do not point to actual memory

    // Create a set of valid pointers that the function can work on
    int64_t some_value;
    int64_t* some_pointer_1 = &some_value;
    int64_t** some_pointer_2 = &some_pointer_1;
    int64_t*** some_pointer_3 = &some_pointer_2;

    // Initialize the first function argument of the foo function to the above pointer
    _context.runtime_value_map[_value_0] = std::make_shared<JitConstantRuntimePointer>(some_pointer_3);

    // Compare simulated and actual pointer values
    assert_address_eq(GetRuntimePointerForValue(_value_0, _context), reinterpret_cast<uint64_t>(some_pointer_3));
    assert_address_eq(GetRuntimePointerForValue(_value_1, _context),
                      reinterpret_cast<uint64_t>(some_pointer_3) + 1 * sizeof(int64_t));
    assert_address_eq(GetRuntimePointerForValue(_value_2, _context), reinterpret_cast<uint64_t>(some_pointer_2));
    assert_address_eq(GetRuntimePointerForValue(_value_3, _context), reinterpret_cast<uint64_t>(some_pointer_3));
    assert_address_eq(GetRuntimePointerForValue(_value_4, _context),
                      reinterpret_cast<uint64_t>(some_pointer_3) + 2 * sizeof(int32_t));
    assert_address_eq(GetRuntimePointerForValue(_value_5, _context),
                      reinterpret_cast<uint64_t>(some_pointer_3) + (2 + 3) * sizeof(int32_t));
    assert_address_eq(GetRuntimePointerForValue(_value_6, _context),
                      reinterpret_cast<uint64_t>(some_pointer_2) + 4 * sizeof(int64_t));
    assert_address_eq(GetRuntimePointerForValue(_value_7, _context), reinterpret_cast<uint64_t>(some_pointer_1));
  }

  std::shared_ptr<llvm::LLVMContext> _llvm_context;
  SpecializationContext _context;
  llvm::Value* _value_0;
  llvm::Value* _value_1;
  llvm::Value* _value_2;
  llvm::Value* _value_3;
  llvm::Value* _value_4;
  llvm::Value* _value_5;
  llvm::Value* _value_6;
  llvm::Value* _value_7;
};

TEST_F(GetRuntimePointerForValueTest, BitcodePointerInstructionsAreProperlySimulated) { bitcode_pointer_test(); }

TEST_F(GetRuntimePointerForValueTest, RuntimePointersAreInvalidWithoutInitialAddress) {
  //
  ASSERT_FALSE(GetRuntimePointerForValue(_value_0, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_1, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_2, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_3, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_4, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_5, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_6, _context)->is_valid());
  ASSERT_FALSE(GetRuntimePointerForValue(_value_7, _context)->is_valid());
}

}  // namespace opossum
