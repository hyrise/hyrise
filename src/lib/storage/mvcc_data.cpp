#include "mvcc_data.hpp"

#include <iostream>
#include <sstream>

namespace opossum {

MvccData::MvccData(const size_t size) { grow_by(size, 0); }

size_t MvccData::size() const { return _size; }

void MvccData::shrink() {
  tids.shrink_to_fit();
  begin_cids.shrink_to_fit();
  end_cids.shrink_to_fit();
}

void MvccData::grow_by(size_t delta, CommitID begin_cid) {
  _size += delta;
  tids.grow_to_at_least(_size);
  begin_cids.grow_to_at_least(_size, begin_cid);
  end_cids.grow_to_at_least(_size, MAX_COMMIT_ID);
}

void MvccData::print() const { print(std::cout); }

void MvccData::print(std::ostream& stream) const {
  stream << "TIDs: ";
  for (const auto& tid : tids) stream << tid << ", ";
  stream << std::endl;

  stream << "BeginCIDs: ";
  for (const auto& begin_cid : begin_cids) stream << begin_cid << ", ";
  stream << std::endl;

  stream << "EndCIDs: ";
  for (const auto& end_cid : end_cids) stream << end_cid << ", ";
  stream << std::endl;
}

}  // namespace opossum
