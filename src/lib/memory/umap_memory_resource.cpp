#include "umap_memory_resource.hpp"

#if HYRISE_WITH_UMAP && HYRISE_WITH_JEMALLOC

#include <errno.h>
#include <fcntl.h>
#include <umap/umap.h>

#include <cstdlib>
#include <fstream>
#include <nlohmann/json.hpp>
#include <string>

#include "hyrise.hpp"
#include "utils/format_bytes.hpp"
#include "utils/string_utils.hpp"

namespace opossum {

#ifdef UMAP_JEMALLOC_APPROACH
static void* extent_alloc_hook(extent_hooks_t *extent_hooks, void *new_addr, size_t size,
                               size_t alignment, bool *zero, bool *commit, unsigned _arena_index) {

  // TODO setting *zero to indicate whether the extent is zeroed and *commit to indicate whether the extent is committed.  Zeroing is mandatory if *zero is true upon function entry. Committing is mandatory if *commit is true upon function entry

  // Not actually used
  const auto target_size = 1'099'511'627'776lu;
  static auto offset = size_t{0};

  if (alignment) {
    offset += alignment - (offset % alignment);
  }

  Assert(offset + size < target_size, "exceeded");

  static int fd = 0;
  static void *base_addr;
  if (!fd) {
    // TODO use proper folder
    // TODO move to constructor

    const auto filename = std::string{"/mnt/"} + getenv("TIERING_DEV") + "/tmp";

    std::ofstream ofs(filename, std::ios::binary | std::ios::out);
    ofs.seekp(target_size);
    ofs.write("", 1);
    ofs.close();

    fd = open(filename, O_RDWR | O_LARGEFILE | O_DIRECT | O_CREAT, S_IRUSR | S_IWUSR);
    // posix_fallocate(fd, 0, target_size);
    Assert(fd != -1, std::string{"Opening FD failed with "} + std::to_string(errno));
    // base_addr = mmap(NULL, target_size, PROT_READ|PROT_WRITE, UMAP_PRIVATE, fd, 0);
    base_addr = umap(NULL, target_size, PROT_READ|PROT_WRITE, UMAP_PRIVATE, fd, 0);
    Assert(base_addr != MAP_FAILED, "mapping failed");
  }

  // auto base_addr = malloc(size);
  // auto base_addr = mmap(NULL, size, PROT_READ|PROT_WRITE, UMAP_PRIVATE, fd, 0);
  
  // assert base_addr
  // TODO bugreport umap - needs to remain open
  // close(fd);

  offset += size;

  if (*zero) {
    memset(reinterpret_cast<char*>(base_addr) + offset - size, 0, size);
  }

  Assert(!new_addr, "new_addr is not nullptr");

  return reinterpret_cast<char*>(base_addr) + offset - size; 
}

UmapMemoryResource::UmapMemoryResource() {
  _hooks.alloc = extent_alloc_hook;
  auto hooks_ptr = &_hooks;

  auto sz = sizeof(_arena_index);
  Assert(mallctl("arenas.create", (void*)&_arena_index, &sz, nullptr, 0) == 0, "mallctl failed");

  char cmd[64];
  snprintf(cmd, sizeof(cmd), "arena.%u.extent_hooks", _arena_index);
  if (mallctl(cmd, NULL, NULL, (void *)&hooks_ptr, sizeof(extent_hooks_t *))) assert(false && "mallctl failed");

  _mallocx_flags = MALLOCX_ARENA(_arena_index) | MALLOCX_TCACHE_NONE;

  // TODO Deduplicate with NVMMemoryResource

  std::ifstream config_file("tiering.json");
  if (!config_file.good()) config_file = std::ifstream{"../tiering.json"};
  Assert(config_file.good(), "tiering.json not found");
  const nlohmann::json config = nlohmann::json::parse(config_file);

  const auto umap_pooldir = config["umap_pooldir"].get<std::string>();
  Assert(std::filesystem::is_directory(umap_pooldir), "UMAP pooldir is not a directory");
}


void* UmapMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  auto p = mallocx(bytes, _mallocx_flags);

  return p;
}

#else
UmapMemoryResource::UmapMemoryResource(const std::string& filename) : _filename(std::string{"/mnt/"} + getenv("TIERING_DEV") + "/" + filename) {
  // TODO use proper folder
  unlink(_filename.c_str());  // Somehow, we segfault if the file already exists

  std::ofstream ofs(_filename, std::ios::binary | std::ios::out);
  ofs.seekp(ALLOCATED_SIZE);
  ofs.write("", 1);
  ofs.close();

  auto flags = O_RDWR | O_LARGEFILE | O_CREAT;
  if (std::string{getenv("TIERING_DEV")} != "tmpfs") flags |= O_DIRECT;
  fd = open(_filename.c_str(), flags, S_IRUSR | S_IWUSR);
  Assert(fd != -1, std::string{"Opening FD failed with "} + std::to_string(errno));
  // _base_addr = mmap(NULL, ALLOCATED_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  _base_addr = umap(NULL, ALLOCATED_SIZE, PROT_READ|PROT_WRITE, UMAP_PRIVATE, fd, 0);
  Assert(_base_addr, "mapping failed");

  // close(fd); on destruction
}

void* UmapMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  Assert(_offset + bytes < ALLOCATED_SIZE, "exceeded");

  if (alignment && _offset % alignment) {
    // TODO can be done more efficiently
    _offset += alignment - (_offset % alignment);
  }

  // TODO make this thread-safe. As long as there is only one migration thread, it does not matter
  _offset += bytes;

  return (char*)_base_addr + _offset - bytes;
}
#endif


UmapMemoryResource::~UmapMemoryResource() {
  // TODO uunmap
}


void UmapMemoryResource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  // free(p);

  // TODO track deallocated memory, make sure holes are punched again
}

bool UmapMemoryResource::do_is_equal(const memory_resource& other) const noexcept { return &other == this; }

size_t UmapMemoryResource::memory_usage() const {
  Fail("Not implemented");
  return 0;
}

}  // namespace opossum

#endif
