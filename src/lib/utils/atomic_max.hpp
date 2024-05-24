#pragma once

#include "storage/mvcc_data.hpp"

namespace hyrise {

// std::max()-like assignment for std::atomic<T>. Taken from https://stackoverflow.com/a/16190791, which is a slightly
// adapted version of https://herbsutter.com/2012/08/31/reader-qa-how-to-write-a-cas-loop-using-stdatomics/.
template <typename T>
inline void set_atomic_max(std::atomic<T>& maximum_value, const T& value) noexcept {
  auto prev_value = maximum_value.load();
  while (prev_value < value && !maximum_value.compare_exchange_weak(prev_value, value)) {}
}

// Specialization for CommitID: Ignore MAX_COMMIT_ID as reserved value and treat it like it was unset.
template <>
inline void set_atomic_max(std::atomic<CommitID>& maximum_value, const CommitID& value) noexcept {
  auto prev_value = maximum_value.load();
  while ((prev_value == MvccData::MAX_COMMIT_ID || prev_value < value) &&
         !maximum_value.compare_exchange_weak(prev_value, value)) {}
}

}  // namespace hyrise
