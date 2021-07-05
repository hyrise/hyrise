#pragma once

#include <string>
#include <vector>
#include <iostream>

namespace opossum {

class DictionarySharingTask {
 public:
  void do_segment_sharing(std::optional<std::ofstream> csv_output_stream_opt, const double jaccard_index_threshold = 0.5);

};
}  // namespace opossum
