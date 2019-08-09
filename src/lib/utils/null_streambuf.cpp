#include "null_streambuf.hpp"

#include <iostream>
#include <streambuf>

namespace opossum {

std::ostream& get_null_streambuf() {
  // Create no-op stream that just swallows everything streamed into it
  // See https://stackoverflow.com/a/11826666
  class NullBuffer : public std::streambuf {
   public:
    int overflow(int c) override { return c; }
  };

  static NullBuffer null_buffer;
  static std::ostream null_stream(&null_buffer);
  return null_stream;
}

}  // namespace opossum
