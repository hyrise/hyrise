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

  auto page = Hyrise::get().buffer_manager.get_page(PageID{0}, PageSizeType::KiB8);
  ASSERT_TRUE(page != nullptr);
  auto raw_data = reinterpret_cast<int*>(page);
  EXPECT_EQ(raw_data[0], 1);
  EXPECT_EQ(raw_data[1], 2);
  EXPECT_EQ(raw_data[2], 3);
  EXPECT_EQ(raw_data[3], 4);
  EXPECT_EQ(raw_data[4], 5);

  data = nullptr;
  ASSERT_TRUE(Hyrise::get().buffer_manager.get_page(PageID{0}, PageSizeType::KiB8) == nullptr);
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

// template <typename T>
// class CustomBufferPoolAllocator : public BufferPoolAllocator<T> {
//  public:
//   using pointer = typename BufferPoolAllocator<T>::pointer;
//   using value_type = T;

//   [[nodiscard]] pointer allocate(std::size_t n) override {
//     auto ptr =
//         static_cast<pointer>(BufferManager::get_global_buffer_manager().allocate(sizeof(value_type) * n, alignof(T)));
//     std::cout << "TEst" << std::endl;
//     return ptr;
//   }
// };

// TEST_F(BufferPoolAllocatorTest, TestSubclass) {
//   auto allocator = BufferPoolAllocator<int>{};
//   auto custom = CustomBufferPoolAllocator<int>{};
//   auto new_vector = boost::container::vector<int, BufferPoolAllocator<int>>{100, allocator};

//   new_vector.get_stored_allocator() = custom;
//   new_vector.resize(1000);

//   // TODO: resource test
// }

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