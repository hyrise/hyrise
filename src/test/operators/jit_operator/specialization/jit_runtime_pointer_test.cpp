#include <cstddef>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/jit_runtime_pointer.hpp"

namespace opossum {

#define ASSERT_PTR_EQ(val1, val2) ASSERT_EQ(&(val1), &(val2))

class JitRuntimePointerTest : public BaseTest {};

TEST_F(JitRuntimePointerTest, PointerIsInvalidByDefault) {
  auto invalid_pointer = std::make_shared<JitRuntimePointer>();
  ASSERT_FALSE(invalid_pointer->is_valid());
}

TEST_F(JitRuntimePointerTest, JitConstantRuntimePointerReturnsConstantAddress) {
  auto pointer = std::make_shared<JitConstantRuntimePointer>(123456ul);
  ASSERT_EQ(pointer->address(), 123456ul);
  ASSERT_EQ(pointer->total_offset(), 0ul);
}

TEST_F(JitRuntimePointerTest, JitOffsetRuntimePointerAppliesOffset) {
  auto pointer = std::make_shared<JitConstantRuntimePointer>(123456ul);
  auto offset_pointer_1 = std::make_shared<JitOffsetRuntimePointer>(pointer, 100ul);
  auto offset_pointer_2 = std::make_shared<JitOffsetRuntimePointer>(offset_pointer_1, 10ul);

  ASSERT_EQ(offset_pointer_1->address(), 123456ul + 100ul);
  ASSERT_EQ(offset_pointer_1->total_offset(), 100ul);
  ASSERT_EQ(offset_pointer_2->address(), 123456ul + 100ul + 10ul);
  ASSERT_EQ(offset_pointer_2->total_offset(), 100ul + 10ul);
}

TEST_F(JitRuntimePointerTest, JitDereferencedRuntimePointerDereferences) {
  int64_t some_value;
  int64_t* some_pointer_1 = &some_value;
  int64_t** some_pointer_2 = &some_pointer_1;

  auto pointer = std::make_shared<JitConstantRuntimePointer>(&some_pointer_2);
  auto dereferenced_pointer_1 = std::make_shared<JitDereferencedRuntimePointer>(pointer);
  auto dereferenced_pointer_2 = std::make_shared<JitDereferencedRuntimePointer>(dereferenced_pointer_1);

  ASSERT_EQ(dereferenced_pointer_1->address(), reinterpret_cast<uint64_t>(some_pointer_2));
  ASSERT_EQ(dereferenced_pointer_2->address(), reinterpret_cast<uint64_t>(some_pointer_1));
}

TEST_F(JitRuntimePointerTest, RuntimePointerReferencingInvalidMemoryAddressIsInvalid) {
  int64_t some_value;
  auto valid_pointer = std::make_shared<JitConstantRuntimePointer>(&some_value);
  auto invalid_pointer = std::make_shared<JitConstantRuntimePointer>(0x00000000);
  auto offset_pointer = std::make_shared<JitOffsetRuntimePointer>(invalid_pointer, 100ul);
  auto dereferenced_pointer = std::make_shared<JitDereferencedRuntimePointer>(invalid_pointer);

  ASSERT_TRUE(valid_pointer->is_valid());
  ASSERT_FALSE(invalid_pointer->is_valid());
  ASSERT_FALSE(offset_pointer->is_valid());
  ASSERT_FALSE(dereferenced_pointer->is_valid());
}

TEST_F(JitRuntimePointerTest, BaseReturnsPointerWithNoOffset) {
  auto pointer = std::make_shared<JitConstantRuntimePointer>(123456ul);
  auto offset_pointer_1 = std::make_shared<JitOffsetRuntimePointer>(pointer, 100ul);
  auto offset_pointer_2 = std::make_shared<JitOffsetRuntimePointer>(offset_pointer_1, 10ul);

  ASSERT_PTR_EQ(pointer->base(), *pointer);
  ASSERT_PTR_EQ(offset_pointer_1->base(), *pointer);
  ASSERT_PTR_EQ(offset_pointer_2->base(), *pointer);
}

TEST_F(JitRuntimePointerTest, UpReturnsCorrectPointer) {
  int64_t some_value;
  int64_t* some_pointer_1 = &some_value;
  int64_t** some_pointer_2 = &some_pointer_1;

  auto pointer = std::make_shared<JitConstantRuntimePointer>(&some_pointer_2);
  auto dereferenced_pointer_1 = std::make_shared<JitDereferencedRuntimePointer>(pointer);
  auto dereferenced_pointer_2 = std::make_shared<JitDereferencedRuntimePointer>(dereferenced_pointer_1);
  auto dereferenced_and_offset_pointer = std::make_shared<JitOffsetRuntimePointer>(dereferenced_pointer_2, 100ul);

  ASSERT_PTR_EQ(dereferenced_and_offset_pointer->up(), *dereferenced_pointer_1);
  ASSERT_PTR_EQ(dereferenced_pointer_2->up(), *dereferenced_pointer_1);
  ASSERT_PTR_EQ(dereferenced_pointer_1->up(), *pointer);
  ASSERT_THROW(pointer->up(), std::logic_error);
}

TEST_F(JitRuntimePointerTest, NavigatingANestedDataStructure) {
  // Build a linked-list data structure with three nodes and a complex value
  struct Value {
    int32_t a;
    int32_t b;
  };

  struct ListNode {
    Value value;
    ListNode* next;
  };

  ListNode first;
  ListNode second;
  ListNode third;

  first.value = {1, 1};
  first.next = &second;
  second.value = {2, 4};
  second.next = &third;
  third.value = {3, 9};
  third.next = nullptr;

  // Create a hierarchy of runtime pointers that mimics the memory layout of the data structure
  auto first_pointer = std::make_shared<JitConstantRuntimePointer>(&first);
  auto first_next_member_pointer = std::make_shared<JitOffsetRuntimePointer>(first_pointer, offsetof(ListNode, next));
  auto second_pointer = std::make_shared<JitDereferencedRuntimePointer>(first_next_member_pointer);
  auto second_next_member_pointer = std::make_shared<JitOffsetRuntimePointer>(second_pointer, offsetof(ListNode, next));
  auto third_pointer = std::make_shared<JitDereferencedRuntimePointer>(second_next_member_pointer);

  ASSERT_TRUE(first_pointer->is_valid());
  ASSERT_TRUE(second_pointer->is_valid());
  ASSERT_TRUE(third_pointer->is_valid());

  ASSERT_EQ(first_pointer->address(), reinterpret_cast<uint64_t>(&first));
  ASSERT_EQ(second_pointer->address(), reinterpret_cast<uint64_t>(&second));
  ASSERT_EQ(third_pointer->address(), reinterpret_cast<uint64_t>(&third));

  ASSERT_GT(first_next_member_pointer->total_offset(), 0ul);
  ASSERT_PTR_EQ(first_next_member_pointer->base(), *first_pointer);

  auto third_value_member_pointer = std::make_shared<JitOffsetRuntimePointer>(third_pointer, offsetof(ListNode, value));
  auto third_value_member_a_member_pointer =
      std::make_shared<JitOffsetRuntimePointer>(third_value_member_pointer, offsetof(Value, a));
  auto third_value_member_b_member_pointer =
      std::make_shared<JitOffsetRuntimePointer>(third_value_member_pointer, offsetof(Value, b));

  ASSERT_EQ(third_value_member_a_member_pointer->address(), reinterpret_cast<uint64_t>(&third.value.a));
  ASSERT_EQ(third_value_member_b_member_pointer->address(), reinterpret_cast<uint64_t>(&third.value.b));
  ASSERT_LT(third_value_member_a_member_pointer->total_offset(), third_value_member_b_member_pointer->total_offset());

  ASSERT_PTR_EQ(third_value_member_pointer->base(), *third_pointer);
  ASSERT_PTR_EQ(third_value_member_a_member_pointer->base(), *third_pointer);
  ASSERT_PTR_EQ(third_value_member_b_member_pointer->base(), *third_pointer);

  ASSERT_PTR_EQ(third_value_member_a_member_pointer->up(), *second_next_member_pointer);
  ASSERT_PTR_EQ(third_value_member_a_member_pointer->up().base(), *second_pointer);
  ASSERT_PTR_EQ(third_value_member_a_member_pointer->up().up(), *first_next_member_pointer);
  ASSERT_PTR_EQ(third_value_member_a_member_pointer->up().up().base(), *first_pointer);
  ASSERT_THROW(third_value_member_a_member_pointer->up().up().base().up(), std::logic_error);
}

#undef ASSERT_PTR_EQ

}  // namespace opossum
