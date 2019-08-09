#pragma once

#include <iosfwd>

namespace opossum {

// Create no-op stream that just swallows everything streamed into it
// See https://stackoverflow.com/a/11826666
std::ostream& get_null_streambuf();

}  // namespace opossum
