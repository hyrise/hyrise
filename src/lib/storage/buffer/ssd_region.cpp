#include "ssd_region.hpp"
#include "page.hpp"

namespace hyrise {
SSDRegion::SSDRegion(const std::string& file_name) {
  _backing_file.open(file_name, std::fstream::binary | std::fstream::in | std::fstream::out);  // TODO: O_DIRECT
}

SSDRegion::~SSDRegion() {
  _backing_file.close();
}

void SSDRegion::write_page(const PageID page_id, std::byte* source) {
  const off_t page_pos = page_id * PAGE_SIZE;
  _backing_file.seekg(page_pos);
  // Using reinterpret_cast it neceassay here. Even the C++ StdLib does it in their examples
  _backing_file.write(reinterpret_cast<char*>(source), PAGE_SIZE);
  _backing_file.sync();
}

void SSDRegion::read_page(const PageID page_id, std::byte* destination) {
  const off_t page_pos = page_id * PAGE_SIZE;
  _backing_file.seekg(page_pos);
  // Using reinterpret_cast it neceassay here. Even the C++ StdLib does it in their examples
  _backing_file.read(reinterpret_cast<char*>(destination), PAGE_SIZE);
}

}  // namespace hyrise