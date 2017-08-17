#pragma once

# include <boost/iterator/iterator_facade.hpp>

namespace opossum {

using ChunkOffsetsIterator = std::vector<std::pair<ChunkOffset, ChunkOffset>>::const_iterator;

template <typename Derived, typename Value>
using BaseIterator = boost::iterator_facade<Derived, Value, boost::forward_traversal_tag, Value>;

template <typename Derived, typename Value>
class BaseReferencedIterator : public BaseIterator<Derived, Value> {
 public:
  explicit BaseReferencedIterator(const ChunkOffsetsIterator& chunk_offsets_it) : _chunk_offsets_it{chunk_offsets_it} {}

 protected:
  /**
   * @return index / chunk offset of referencing column
   */
  const ChunkOffset& index_of_referencing() const { return _chunk_offsets_it->first; }

  /**
   * @return index / chunk offset into referenced column
   */
  const ChunkOffset& index_into_referenced() const { return _chunk_offsets_it->second; }

 private:
  friend class boost::iterator_core_access;

  void increment() { ++_chunk_offsets_it; }
  bool equal(const BaseReferencedIterator & other) const { return (_chunk_offsets_it == other._chunk_offsets_it); }

 private:
  ChunkOffsetsIterator _chunk_offsets_it;
};

}  // namespace opossum
