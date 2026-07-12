#include "operators/aggregate_dyod/merge_map.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "utils/assert.hpp"

// The methods below are Fail() stubs so the merge map's tests compile and link; the actual implementations are added
// test-driven in subsequent steps. Fail()-only bodies trip -Wmissing-noreturn; the real implementations will return.
#pragma clang diagnostic ignored "-Wmissing-noreturn"

namespace hyrise {

template <typename KeySchema>
MergeMap<KeySchema>::MergeMap(const KeySchema& key_schema, const uint32_t shift,
                              std::vector<std::unique_ptr<AbstractAccumulatorColumn>> columns)
    : _shift{shift}, _columns{std::move(columns)}, _key_schema{&key_schema} {}

template <typename KeySchema>
void MergeMap<KeySchema>::reserve(const size_t /*distinct_keys*/) {
  Fail("MergeMap::reserve is not implemented yet.");
}

template <typename KeySchema>
void MergeMap<KeySchema>::resolve(std::span<const std::byte> /*key_tile*/, std::vector<uint32_t>& /*slots_out*/) {
  Fail("MergeMap::resolve is not implemented yet.");
}

template <typename KeySchema>
void MergeMap<KeySchema>::fold(const size_t /*aggregate_index*/, std::span<const uint32_t> /*slots*/,
                               std::span<const std::byte> /*value_bytes*/,
                               std::span<const std::byte> /*value_null_bitmap*/) {
  Fail("MergeMap::fold is not implemented yet.");
}

template <typename KeySchema>
size_t MergeMap<KeySchema>::size() const {
  Fail("MergeMap::size is not implemented yet.");
}

template <typename KeySchema>
void MergeMap<KeySchema>::clear() {
  Fail("MergeMap::clear is not implemented yet.");
}

template <typename KeySchema>
void MergeMap<KeySchema>::flush_into(OutputColumns& /*output*/) const {
  Fail("MergeMap::flush_into is not implemented yet.");
}

template <typename KeySchema>
void MergeMap<KeySchema>::grow_index() {
  Fail("MergeMap::grow_index is not implemented yet.");
}

template class MergeMap<NumericShortKeySchema<4>>;
template class MergeMap<NumericShortKeySchema<8>>;
template class MergeMap<NumericShortKeySchema<12>>;
template class MergeMap<NumericShortKeySchema<16>>;
template class MergeMap<NumericArbitraryKeySchema>;
template class MergeMap<MixedKeySchema<4>>;
template class MergeMap<StringOnlyKeySchema<4>>;

}  // namespace hyrise
