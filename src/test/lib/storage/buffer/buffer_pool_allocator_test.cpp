#include <memory>

#include "base_test.hpp"

#include <boost/container/vector.hpp>
#include <filesystem>
#include "storage/buffer/buffer_pool_allocator.hpp"

namespace hyrise {

class BufferPoolAllocatorTest : public BaseTest {};

TEST_F(BufferPoolAllocatorTest, TestAllocateAndDeallocateVector) {
  // BufferPoolResource uses the global Hyrise Buffer Manager
  auto allocator = BufferPoolAllocator<size_t>();

  auto data = std::make_unique<boost::container::vector<int, BufferPoolAllocator<int>>>(5, allocator);
  data->operator[](0) = 1;
  data->operator[](1) = 2;
  data->operator[](2) = 3;
  data->operator[](3) = 4;
  data->operator[](4) = 5;

  auto page = Hyrise::get().buffer_manager.get_page(PageID{0});
  ASSERT_TRUE(page != nullptr);
  auto raw_data = reinterpret_cast<int*>(page->data());
  EXPECT_EQ(raw_data[0], 1);
  EXPECT_EQ(raw_data[1], 2);
  EXPECT_EQ(raw_data[2], 3);
  EXPECT_EQ(raw_data[3], 4);
  EXPECT_EQ(raw_data[4], 5);

  data = nullptr;
  ASSERT_TRUE(Hyrise::get().buffer_manager.get_page(PageID{0}) == nullptr);
}

TEST_F(BufferPoolAllocatorTest, TestPolymorphism) {
  // TODO: resource test
}

TEST_F(BufferPoolAllocatorTest, TestConstructRawPointer) {
  struct Dummy {
    int _value1;
    float _value2;

    Dummy(int value1, float value2) : _value1(value1), _value2(value2) {}

    ~Dummy() {
      _value1 = 2;
      _value2 = 2.0;
    }
  };

  auto allocator = BufferPoolAllocator<size_t>();

  auto dummy = Dummy(0, 0);

  // Test that the constructor is called
  allocator.construct(&dummy, 5, 1.0);
  EXPECT_EQ(dummy._value1, 1);
  EXPECT_EQ(dummy._value2, 1.0);
}

// TODO
// TEST_F(BufferPoolAllocatorTest, TestConstructRawPointer) {
//   auto allocator = BufferPoolAllocator<size_t>();

//   auto dummy = Dummy(0, 0);

//   // Test that the constructor is called
//   allocator.construct(&dummy, 5, 1.0);
//   EXPECT_EQ(dummy._value1, 1);
//   EXPECT_EQ(dummy._value2, 1.0);
// }

//   // Test that the destructor is called
//   allocator.destroy(&dummy);
//   EXPECT_EQ(dummy._value1, 2);
//   EXPECT_EQ(dummy._value2, 2.0);
// }

}  // namespace hyrise