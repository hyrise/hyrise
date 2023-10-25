#include "base_test.hpp"
#include "storage/buffer/page_memory_resource.hpp"

namespace hyrise {

class PageMemoryResourceTest : public BaseTest {
 public:
  void SetUp() override {
    std::filesystem::create_directory(db_path);
    buffer_manager = std::make_unique<BufferManager>(1 << 20, db_path);
  }

  void TearDown() override {
    buffer_manager = nullptr;
  }

  const std::filesystem::path db_path = test_data_path + "buffer_manager_data";
  std::unique_ptr<BufferManager> buffer_manager;
};

TEST_F(PageMemoryResourceTest, TestAllocationAndDeallocation) {
  auto allocator = PageMemoryResource(buffer_manager.get());

  EXPECT_EQ(allocator.allocation_count(), 0);
  EXPECT_EQ(allocator.deallocation_count(), 0);
  EXPECT_EQ(allocator.allocated_bytes(), 0);

  auto large_page_ptr = allocator.allocate(bytes_for_size_type(PageSizeType::KiB256));
  EXPECT_EQ(allocator.allocation_count(), 1);
  EXPECT_EQ(allocator.deallocation_count(), 0);
  EXPECT_EQ(allocator.allocated_bytes(), bytes_for_size_type(PageSizeType::KiB256));

  auto small_page_ptr = allocator.allocate(bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(allocator.allocation_count(), 2);
  EXPECT_EQ(allocator.deallocation_count(), 0);
  EXPECT_EQ(allocator.allocated_bytes(),
            bytes_for_size_type(PageSizeType::KiB256) + bytes_for_size_type(PageSizeType::KiB16));

  auto small_page_ptr2 = allocator.allocate(bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(allocator.allocation_count(), 3);
  EXPECT_EQ(allocator.deallocation_count(), 0);
  EXPECT_EQ(allocator.allocated_bytes(),
            bytes_for_size_type(PageSizeType::KiB256) + 2 * bytes_for_size_type(PageSizeType::KiB16));

  allocator.deallocate(large_page_ptr, bytes_for_size_type(PageSizeType::KiB256));
  EXPECT_EQ(allocator.allocation_count(), 3);
  EXPECT_EQ(allocator.deallocation_count(), 1);
  EXPECT_EQ(allocator.allocated_bytes(), 2 * bytes_for_size_type(PageSizeType::KiB16));

  allocator.deallocate(small_page_ptr, bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(allocator.allocation_count(), 3);
  EXPECT_EQ(allocator.deallocation_count(), 2);
  EXPECT_EQ(allocator.allocated_bytes(), bytes_for_size_type(PageSizeType::KiB16));

  // Check that we reuse the same page id for the same size type if deallocated
  auto small_page_ptr3 = allocator.allocate(bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(allocator.allocation_count(), 4);
  EXPECT_EQ(allocator.deallocation_count(), 2);
  EXPECT_EQ(allocator.allocated_bytes(), 2 * bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(small_page_ptr, small_page_ptr3);
  EXPECT_NE(small_page_ptr, small_page_ptr2);
}

}  // namespace hyrise