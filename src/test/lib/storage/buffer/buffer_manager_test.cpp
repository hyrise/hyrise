#include <future>
#include <memory>
#include <thread>
#include "base_test.hpp"

#include <filesystem>
#include "types.hpp"

namespace hyrise {

class BufferManagerTest : public BaseTest {
 public:
  static constexpr size_t default_seed = 2198738917;

  BufferManager create_buffer_manager(const size_t buffer_pool_size,
                                      const BufferManagerMode mode = BufferManagerMode::DramSSD,
                                      const MigrationPolicy migration_policy = LazyMigrationPolicy(default_seed)) {
    auto config = BufferManager::Config{};
    config.dram_buffer_pool_size = buffer_pool_size;
    config.numa_buffer_pool_size = buffer_pool_size;
    config.ssd_path = db_file;
    config.enable_eviction_purge_worker = false;
    config.mode = mode;
    config.migration_policy = migration_policy;
    return BufferManager(config);
  }

  void evict_frame(BufferManager& buffer_manager, const FramePtr& frame) {
    buffer_manager.evict_frame(frame);
  }

  std::shared_ptr<SSDRegion> get_ssd_region(const BufferManager& buffer_manager) {
    return buffer_manager._ssd_region;
  }

  void TearDown() override {
    std::filesystem::remove(db_file);
  }

  const std::string db_file = test_data_path + "buffer_manager.data";
};

TEST_F(BufferManagerTest, TestPinAndUnpinPageLowMemory) {
  // We create a really small buffer manager with a single frame to test pin and unpin
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
  EXPECT_EQ(buffer_manager.dram_bytes_used(), 0);
  const auto ptr = buffer_manager.allocate(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
  EXPECT_EQ(buffer_manager.dram_bytes_used(), bytes_for_size_type(MAX_PAGE_SIZE_TYPE));

  const auto frame = ptr.get_frame();
  EXPECT_TRUE(frame->is_resident());

  // Pin the page. The next allocation should fail, since there is only a single buffer frame
  // and it has a pinned page
  buffer_manager.pin(frame);
  EXPECT_ANY_THROW(buffer_manager.allocate(512));
  EXPECT_EQ(buffer_manager.dram_bytes_used(), bytes_for_size_type(MAX_PAGE_SIZE_TYPE));

  // Unpin the page. And try again. Now, the allocation works.
  buffer_manager.unpin(frame, false);
  EXPECT_EQ(buffer_manager.dram_bytes_used(), bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
  EXPECT_NO_THROW(buffer_manager.allocate(512));
  EXPECT_EQ(buffer_manager.dram_bytes_used(), bytes_for_size_type(PageSizeType::KiB8));
}

TEST_F(BufferManagerTest, TestPinAndUnpinPageWithDirtyFlag) {
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
  const auto ptr = buffer_manager.allocate(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
  const auto frame = ptr.get_frame();

  EXPECT_EQ(frame->pin_count.load(), 0);
  buffer_manager.pin(frame);
  buffer_manager.pin(frame);
  buffer_manager.pin(frame);
  EXPECT_EQ(frame->pin_count.load(), 3);

  buffer_manager.unpin(frame, false);
  EXPECT_EQ(frame->pin_count.load(), 2);
  EXPECT_FALSE(frame->dirty.load());

  buffer_manager.unpin(frame, true);
  EXPECT_EQ(frame->pin_count.load(), 1);
  EXPECT_TRUE(frame->dirty.load());

  buffer_manager.unpin(frame, false);
  EXPECT_EQ(frame->pin_count.load(), 0);
  EXPECT_TRUE(frame->dirty.load());
}

TEST_F(BufferManagerTest, TestWriteDirtyPageToSSD) {
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
  auto ssd_region = get_ssd_region(buffer_manager);

  const auto ptr = buffer_manager.allocate(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
  const auto frame = ptr.get_frame();

  // Write some data to the page
  buffer_manager.pin(frame);
  std::memset(frame->data, bytes_for_size_type(MAX_PAGE_SIZE_TYPE), 0x05);

  // Unpin the page and mark it as dirty. There should be nothing on the SSD yet.
  buffer_manager.unpin(frame, true);
  alignas(512) std::array<uint8_t, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> read_buffer1;
  auto read_frame =
      make_frame(frame->page_id, frame->size_type, frame->page_type, reinterpret_cast<std::byte*>(read_buffer1.data()));
  ssd_region->read_page(read_frame);

  EXPECT_FALSE(std::memcmp(read_frame->data, frame->data, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)) == 0)
      << "The page should not have been written to SSD";

  // Allocate a new page, which should replace the old one and write it to SSD.
  const auto ptr2 = buffer_manager.allocate(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
  alignas(512) std::array<uint8_t, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> read_buffer2;
  auto read_frame2 =
      make_frame(frame->page_id, frame->size_type, frame->page_type, reinterpret_cast<std::byte*>(read_buffer2.data()));
  ssd_region->read_page(read_frame2);
  EXPECT_EQ(frame->data, nullptr);
  // TODO: We need to compare against some ground truth (setting all bytes to 0x5)
  // EXPECT_TRUE(std::memcmp(read_frame2->data, frame->data, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)) != 0)
  //     << "The page should not have been written to SSD";
}

TEST_F(BufferManagerTest, TestMultipleAllocateAndDeallocate) {
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));

  auto ptr = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256));
  // EXPECT_EQ(ptr, BufferPtr<void>(PageID{0}, 0, PageSizeType::KiB256));

  // TODO: Test Sizes capcaity etc

  // TODO: If the page is deallocated, the pointer should be set to 0
  EXPECT_NE(ptr.operator->(), nullptr);
  buffer_manager.deallocate(ptr, bytes_for_size_type(PageSizeType::KiB256));
  EXPECT_EQ(ptr.operator->(), nullptr);

  auto ptr2 = buffer_manager.allocate(1024);
  // EXPECT_EQ(ptr2, BufferPtr<void>(PageID{1}, 0, PageSizeType::KiB256));

  buffer_manager.deallocate(ptr2, 1024);
  EXPECT_NE(ptr2.operator->(), nullptr);
  buffer_manager.deallocate(ptr2, 1024);
  EXPECT_EQ(ptr2.operator->(), nullptr);
}

TEST_F(BufferManagerTest, TestAllocateDifferentPageSizes) {
  auto buffer_manager = create_buffer_manager(5 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE));

  auto current_bytes = size_t{0};
  auto ptr8 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB8));
  EXPECT_EQ(ptr8.get_frame()->size_type, PageSizeType::KiB8);
  EXPECT_EQ(ptr8.get_frame()->page_type, PageType::Dram);

  current_bytes += bytes_for_size_type(PageSizeType::KiB8);
  EXPECT_EQ(buffer_manager.dram_bytes_used(), current_bytes);

  auto ptr16 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(ptr16.get_frame()->size_type, PageSizeType::KiB16);
  EXPECT_EQ(ptr16.get_frame()->page_type, PageType::Dram);

  current_bytes += bytes_for_size_type(PageSizeType::KiB16);
  EXPECT_EQ(buffer_manager.dram_bytes_used(), current_bytes);

  auto ptr32 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB32));
  EXPECT_EQ(ptr32.get_frame()->size_type, PageSizeType::KiB32);
  EXPECT_EQ(ptr32.get_frame()->page_type, PageType::Dram);
  current_bytes += bytes_for_size_type(PageSizeType::KiB32);
  EXPECT_EQ(buffer_manager.dram_bytes_used(), current_bytes);

  auto ptr64 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB64));
  EXPECT_EQ(ptr64.get_frame()->size_type, PageSizeType::KiB64);
  EXPECT_EQ(ptr64.get_frame()->page_type, PageType::Dram);
  current_bytes += bytes_for_size_type(PageSizeType::KiB64);
  EXPECT_EQ(buffer_manager.dram_bytes_used(), current_bytes);

  auto ptr128 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB128));
  EXPECT_EQ(ptr128.get_frame()->size_type, PageSizeType::KiB128);
  EXPECT_EQ(ptr128.get_frame()->page_type, PageType::Dram);

  current_bytes += bytes_for_size_type(PageSizeType::KiB128);
  EXPECT_EQ(buffer_manager.dram_bytes_used(), current_bytes);

  auto ptr256 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256));
  EXPECT_EQ(ptr256.get_frame()->size_type, PageSizeType::KiB256);
  EXPECT_EQ(ptr256.get_frame()->page_type, PageType::Dram);
  current_bytes += bytes_for_size_type(PageSizeType::KiB256);
  EXPECT_EQ(buffer_manager.dram_bytes_used(), current_bytes);

  auto ptr512 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB512));
  EXPECT_EQ(ptr512.get_frame()->size_type, PageSizeType::KiB512);
  EXPECT_EQ(ptr512.get_frame()->page_type, PageType::Dram);
  current_bytes += bytes_for_size_type(PageSizeType::KiB512);
  EXPECT_EQ(buffer_manager.dram_bytes_used(), current_bytes);

  EXPECT_ANY_THROW(buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB512) + 5));
}

// TEST_F(BufferManagerTest, TestUnswizzle) {
//   auto buffer_manager = create_buffer_manager(bytes_for_size_type(MAX_PAGE_SIZE_TYPE));

//   auto ptr1 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB32));
//   auto [frame1, offset1] = buffer_manager.unswizzle(static_cast<char*>(ptr1.get_pointer()) + 30);
//   EXPECT_EQ(offset1, 30);
//   EXPECT_EQ(frame1->page_id, PageID{0});
//   EXPECT_EQ(frame1->size_type, PageSizeType::KiB32);
//   EXPECT_EQ(frame1->page_type, PageType::Dram);

//   auto ptr2 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB64));
//   auto [frame2, offset2] = buffer_manager.unswizzle(static_cast<char*>(ptr2.get_pointer()) + 1337);
//   EXPECT_EQ(offset2, 1337);
//   EXPECT_EQ(frame2->page_id, PageID{1});
//   EXPECT_EQ(frame2->size_type, PageSizeType::KiB64);
//   EXPECT_EQ(frame2->page_type, PageType::Dram);
// }

TEST_F(BufferManagerTest, TestMakeResidentDramSDDMode) {
  auto buffer_manager = create_buffer_manager(bytes_for_size_type(MAX_PAGE_SIZE_TYPE), BufferManagerMode::DramSSD);
  auto ptr1 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB32));

  auto frame = ptr1.get_frame();
  EXPECT_TRUE(frame->is_resident());
  EXPECT_EQ(frame->page_type, PageType::Dram);
  EXPECT_FALSE(frame->is_pinned());
  EXPECT_NE(frame->data, nullptr);

  auto frame1 = buffer_manager.make_resident(frame, AccessIntent::Read);
  EXPECT_EQ(frame1, frame);
  EXPECT_TRUE(frame->is_resident());
  EXPECT_EQ(frame->page_type, PageType::Dram);
  EXPECT_FALSE(frame->is_pinned());
  EXPECT_TRUE(frame1->is_referenced());

  EXPECT_NE(frame->data, nullptr);

  evict_frame(buffer_manager, frame);
  EXPECT_FALSE(frame->is_resident());

  auto frame2 = buffer_manager.make_resident(frame, AccessIntent::Read);
  EXPECT_EQ(frame2, frame);
  EXPECT_EQ(frame2->page_type, PageType::Dram);
  EXPECT_FALSE(frame->is_pinned());
  EXPECT_TRUE(frame2->is_resident());
  EXPECT_TRUE(frame2->is_referenced());

  EXPECT_NE(frame2->data, nullptr);
}

TEST_F(BufferManagerTest, TestMakeResidentDramNumaEmulationSSDModeWithAllBypassMigrationPolicy) {
  auto buffer_manager = create_buffer_manager(10 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE),
                                              BufferManagerMode::DramNumaEmulationSSD, MigrationPolicy(0, 0, 0, 0));

  // We can use a fixed access intent, since it does not have any effect on the selection, it just controls which bypass function to use.
  constexpr auto access_intent = AccessIntent::Read;
  constexpr auto page_id = PageID{0};

  struct alignas(512) Page {
    std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  };

  auto dram_page = Page{};
  auto numa_page = Page{};

  // Allocate a random to ensure that we have the page id created. We just use manually created frames to test different scenarios
  auto ptr1 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB32));
  EXPECT_EQ(ptr1.get_frame()->page_id, page_id);
  // TODO: Write some data to the page for testin
  // TODO: Mock ssd region

  // If the frame is a DRAM frame and resident, it should return the DRAM frame returned
  {
    auto frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    frame->set_resident();
    auto resident_frame = buffer_manager.make_resident(frame, access_intent);
    EXPECT_EQ(frame, resident_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  //If the frame is a DRAM frame and has a resident NUMA frame assigned, it should return the DRAM frame
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();

    numa_frame->set_resident();
    dram_frame->set_resident();

    auto resident_frame = buffer_manager.make_resident(dram_frame, access_intent);
    EXPECT_EQ(dram_frame, resident_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // If the frame is a NUMA frame and has a resident DRAM frame assigned, it should return the DRAM frame
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();
    numa_frame->data = numa_page.data.data();

    numa_frame->set_resident();
    dram_frame->set_resident();

    auto resident_frame = buffer_manager.make_resident(numa_frame, access_intent);
    EXPECT_EQ(dram_frame, resident_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // If the frame is a NUMA frame and has no resident DRAM frame assigned, it should return NUMA frame, since we do not want to bypass it
  {
    auto numa_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Numa, numa_page.data.data());
    numa_frame->data = numa_page.data.data();
    numa_frame->set_resident();

    auto resident_frame = buffer_manager.make_resident(numa_frame, access_intent);
    EXPECT_EQ(resident_frame, numa_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // Pass a DRAM frame with a Numa frame attached, but both frames are not resident
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();
    dram_frame->set_evicted();
    numa_frame->set_evicted();

    auto resident_frame = buffer_manager.make_resident(dram_frame, access_intent);
    EXPECT_EQ(resident_frame, dram_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // Pass a NUMA frame with a DRAM frame attached, but both frames are not resident
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();
    dram_frame->set_evicted();
    numa_frame->set_evicted();

    auto resident_frame = buffer_manager.make_resident(numa_frame, access_intent);
    EXPECT_EQ(resident_frame, dram_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }
}

TEST_F(BufferManagerTest, TestMakeResidentDramNumaEmulationSSDModeWithNoBypassMigrationPolicy) {
  auto buffer_manager = create_buffer_manager(10 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE),
                                              BufferManagerMode::DramNumaEmulationSSD, MigrationPolicy(1, 1, 1, 1));

  // We can use a fixed access intent, since it does not have any effect on the selection, it just controls which bypass function to use.
  constexpr auto access_intent = AccessIntent::Read;
  constexpr auto page_id = PageID{0};

  struct alignas(512) Page {
    std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  };

  auto dram_page = Page{};
  auto numa_page = Page{};

  // Allocate a random to ensure that we have the page id created. We just use manually created frames to test different scenarios
  auto ptr1 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB32));
  EXPECT_EQ(ptr1.get_frame()->page_id, page_id);
  // TODO: Write some data to the page for testin
  // TODO: Mock ssd region

  // If the frame is a DRAM frame and resident, it should return the DRAM frame returned
  {
    auto frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    frame->set_resident();
    auto resident_frame = buffer_manager.make_resident(frame, access_intent);
    EXPECT_EQ(frame, resident_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  //If the frame is a DRAM frame and has a resident NUMA frame assigned, it should return the DRAM frame
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();

    numa_frame->set_resident();
    dram_frame->set_resident();

    auto resident_frame = buffer_manager.make_resident(dram_frame, access_intent);
    EXPECT_EQ(dram_frame, resident_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // If the frame is a NUMA frame and has a resident DRAM frame assigned, it should return the DRAM frame
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();
    numa_frame->data = numa_page.data.data();

    numa_frame->set_resident();
    dram_frame->set_resident();

    auto resident_frame = buffer_manager.make_resident(numa_frame, access_intent);
    EXPECT_EQ(dram_frame, resident_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // If the frame is a NUMA frame and has no resident DRAM frame assigned, it should return DRAM frame
  {
    auto numa_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Numa, numa_page.data.data());
    numa_frame->data = numa_page.data.data();
    numa_frame->set_resident();

    auto resident_frame = buffer_manager.make_resident(numa_frame, access_intent);
    EXPECT_NE(resident_frame, numa_frame);
    EXPECT_EQ(resident_frame->page_type, PageType::Dram);

    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // Pass a DRAM frame with a Numa frame attached, but both frames are not resident
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();
    dram_frame->set_evicted();
    numa_frame->set_evicted();

    auto resident_frame = buffer_manager.make_resident(dram_frame, access_intent);
    EXPECT_EQ(resident_frame, dram_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // Pass a NUMA frame with a DRAM frame attached, but both frames are not resident
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();
    dram_frame->set_evicted();
    numa_frame->set_evicted();

    auto resident_frame = buffer_manager.make_resident(numa_frame, access_intent);
    EXPECT_EQ(resident_frame, dram_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }
}

TEST_F(BufferManagerTest, TestMakeResidentDramNumaEmulationSSDModeWithNoBypassNumaAndAllBypassDramMigrationPolicy) {
  auto buffer_manager = create_buffer_manager(10 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE),
                                              BufferManagerMode::DramNumaEmulationSSD, MigrationPolicy(0, 0, 1, 1));

  // We can use a fixed access intent, since it does not have any effect on the selection, it just controls which bypass function to use.
  constexpr auto access_intent = AccessIntent::Read;
  constexpr auto page_id = PageID{0};

  struct alignas(512) Page {
    std::array<std::byte, bytes_for_size_type(MAX_PAGE_SIZE_TYPE)> data;
  };

  auto dram_page = Page{};
  auto numa_page = Page{};

  // Allocate a random to ensure that we have the page id created. We just use manually created frames to test different scenarios
  auto ptr1 = buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB32));
  EXPECT_EQ(ptr1.get_frame()->page_id, page_id);
  // TODO: Write some data to the page for testin
  // TODO: Mock ssd region

  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    dram_frame->set_evicted();

    auto resident_frame = buffer_manager.make_resident(dram_frame, access_intent);
    EXPECT_NE(dram_frame, resident_frame);
    EXPECT_EQ(resident_frame->page_type, PageType::Numa);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }

  // If the frame is a DRAM frame and resident, it should return the DRAM frame returned
  {
    auto dram_frame = make_frame(page_id, PageSizeType::KiB32, PageType::Dram, dram_page.data.data());
    auto numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();
    dram_frame->set_evicted();
    numa_frame->set_evicted();

    auto resident_frame = buffer_manager.make_resident(dram_frame, access_intent);
    EXPECT_EQ(numa_frame, resident_frame);
    EXPECT_TRUE(resident_frame->is_resident());
    EXPECT_FALSE(resident_frame->is_pinned());
    EXPECT_TRUE(resident_frame->is_referenced());
  }
}

TEST_F(BufferManagerTest, TestAllocateAndDeallocateWithDramNumaEmulationSSDModeWithMigrationPolicy) {
  constexpr auto iterations = size_t{100};

  {
    // Check that that a around 1 percent (2/100 iterations) number of pages are allocated on dram
    constexpr size_t lazy_seed = 126378163;
    auto lazy_buffer_manager =
        create_buffer_manager(5 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE), BufferManagerMode::DramNumaEmulationSSD,
                              LazyMigrationPolicy(lazy_seed));

    auto lazy_dram_frame_count = size_t{0};
    for (auto i = size_t{0}; i < iterations; ++i) {
      auto ptr = lazy_buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB8));
      auto frame = ptr.get_frame();
      EXPECT_TRUE(frame->is_resident());
      lazy_dram_frame_count += frame->page_type == PageType::Dram;
      lazy_buffer_manager.deallocate(ptr, bytes_for_size_type(PageSizeType::KiB8));
    }

    EXPECT_EQ(lazy_dram_frame_count, 2);
  }
  {
    // Check that that a around 34 percent (32/100 iterations) number of pages are allocated on dram
    constexpr size_t custom_seed = 6999293;

    auto custom_policy_buffer_manager =
        create_buffer_manager(5 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE), BufferManagerMode::DramNumaEmulationSSD,
                              MigrationPolicy(0.34, 0.34, 0.34, 0.34, custom_seed));
    auto custom_policy_dram_frame_count = size_t{0};
    for (auto i = size_t{0}; i < iterations; ++i) {
      auto ptr = custom_policy_buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB8));
      auto frame = ptr.get_frame();
      EXPECT_TRUE(frame->is_resident());
      custom_policy_dram_frame_count += frame->page_type == PageType::Dram;
      custom_policy_buffer_manager.deallocate(ptr, bytes_for_size_type(PageSizeType::KiB8));
    }

    EXPECT_EQ(custom_policy_dram_frame_count, 32);
  }
  {
    // Check that that a around 100 percent (100/100 iterations) number of pages are allocated on dram
    constexpr size_t eager_seed = 19595012;

    auto eager_buffer_manager =
        create_buffer_manager(5 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE), BufferManagerMode::DramNumaEmulationSSD,
                              EagerMigrationPolicy(eager_seed));
    auto eager_dram_frame_count = size_t{0};
    for (auto i = size_t{0}; i < iterations; ++i) {
      auto ptr = eager_buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB8));
      auto frame = ptr.get_frame();
      EXPECT_TRUE(frame->is_resident());
      eager_dram_frame_count += frame->page_type == PageType::Dram;
      eager_buffer_manager.deallocate(ptr, bytes_for_size_type(PageSizeType::KiB8));
    }

    EXPECT_EQ(eager_dram_frame_count, 100);
  }
}

TEST_F(BufferManagerTest, TestAllocateAndDeallocateWithDramSSDMode) {
  GTEST_SKIP();
}

TEST_F(BufferManagerTest, TestFindFrameAndOffset) {
  // TODO: test with numa, too
  auto buffer_manager = create_buffer_manager(5 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE), BufferManagerMode::DramSSD);

  auto ptr1 = static_cast<BufferPtr<int>>(buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB8))) + 50;
  auto [frame1, offset1] = buffer_manager.find_frame_and_offset(ptr1.operator->());
  EXPECT_EQ(frame1, ptr1.get_frame());
  EXPECT_EQ(offset1, 50 * sizeof(int));

  auto ptr2 = static_cast<BufferPtr<int>>(buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB8))) + 1337;
  auto [frame2, offset2] = buffer_manager.find_frame_and_offset(ptr2.operator->());
  EXPECT_EQ(frame2, ptr2.get_frame());
  EXPECT_EQ(offset2, 1337 * sizeof(int));

  auto ptr3 = static_cast<BufferPtr<int>>(buffer_manager.allocate(bytes_for_size_type(PageSizeType::KiB256))) + 0;
  auto [frame3, offset3] = buffer_manager.find_frame_and_offset(ptr3.operator->());
  EXPECT_EQ(frame3, ptr3.get_frame());
  EXPECT_EQ(offset3, 0);

  int test_var = 10;
  auto [outside_frame, outside_offset] = buffer_manager.find_frame_and_offset(std::addressof(test_var));
  EXPECT_EQ(outside_frame, &(buffer_manager.DUMMY_FRAME));
  EXPECT_EQ(outside_offset, 0);
}

TEST_F(BufferManagerTest, TestVectorBeginEnd) {
  // auto allocator = PolymorphicAllocator<size_t>{get_buffer_manager_memory_resource()};
  // // 8192 * 4 == Page32KB
  // auto vector = pmr_vector<int32_t>(8192, allocator);
  // auto begin = vector.begin();
  // auto end = vector.end();

  // auto begin_ptr = begin.get_ptr().operator->();
  // auto end_ptr = end.get_ptr().operator->();

  // auto begin_ptr2 = BufferPtr<int32_t>(begin_ptr);
  // auto end_ptr2 = BufferPtr<int32_t>(end_ptr);

  // std::sort(begin_ptr2, end_ptr2);
  // EXPECT_NE(begin_ptr2, end_ptr2);
}

}  // namespace hyrise