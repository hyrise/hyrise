#pragma once

#include <boost/filesystem/fstream.hpp>
#include "types.hpp"

namespace hyrise {

class SSDRegion : private Noncopyable {
 public:
  SSDRegion(const std::string& file_name);
  ~SSDRegion();

  void write_page(const PageID page_id, std::byte* destination);
  void read_page(const PageID page_id, std::byte* source);

 private:
  std::fstream _backing_file;
};
}  // namespace hyrise