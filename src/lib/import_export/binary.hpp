#pragma once

namespace opossum {

enum class BinarySegmentType : uint8_t { value_segment = 0, dictionary_segment = 1, run_length_segment = 2 };

using BoolAsByteType = uint8_t;

}  // namespace opossum
