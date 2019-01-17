#include "chunk_access_counter.hpp"

namespace opossum {

uint64_t ChunkAccessCounter::history_sample(size_t lookback) const {
  if (_history.size() < 2 || lookback == 0) return 0;
  const auto last = _history.back();
  const auto prelast_index =
      std::max(static_cast<int64_t>(0), static_cast<int64_t>(_history.size()) - static_cast<int64_t>(lookback));
  const auto prelast = _history.at(prelast_index);
  return last - prelast;
}

}  // namespace opossum
