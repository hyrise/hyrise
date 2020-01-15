#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {
class Table;

template <typename T>
class SegmentAccessCounter {
 public:
  T other;
  T iterator_create;
  T iterator_seq_access;
  T iterator_increasing_access;
  T iterator_random_access;
  T accessor_create;
  T accessor_access;
  T dictionary_access;

  void reset();

  T sum() const;

  std::string to_string() const;

  inline static const std::string HEADERS = "Other,IteratorCreate,IteratorSeqAccess,IteratorIncreasingAccess,"
                                            "IteratorRandomAccess,AccessorCreate,AccessorAccess,"
                                            "DictionaryAccess";
};

class SegmentAccessStatisticsTools {
 public:
  static void reset_all(const std::map<std::string, std::shared_ptr<Table>>& tables);

  // There are six AccessPatterns:
  // 0 (unknown), equivalent to IteratorSeqAccess
  // 1 (sequentially increasing), difference between two neighboring elements is 0 or 1.
  // 2 (increasing randomly)
  // 3 (sequentially decreasing), difference between two neighboring elements is -1 or 0.
  // 4 (decreasing randomly)
  // 5 (random access)
  enum class AccessPattern {Unknown, SeqInc, RndInc, SeqDec, RndDec, Rnd};

  static AccessPattern iterator_access_pattern(const std::shared_ptr<const PosList>& positions);

 private:
  // There are five possible inputs
  enum class Input {Zero, One, Positive, NegativeOne, Negative};

//  constexpr static const std::array<std::array<State, static_cast<uint32_t>(Input::Count)>, static_cast<uint32_t>(State::Count)>
  constexpr static const std::array<std::array<AccessPattern, 5 /*|Input|*/>, 6 /*|AccessPattern|*/>
    _transitions{{{AccessPattern::Unknown, AccessPattern::SeqInc, AccessPattern::SeqInc, AccessPattern::SeqDec, AccessPattern::SeqDec},
                  {AccessPattern::SeqInc, AccessPattern::SeqInc, AccessPattern::RndInc, AccessPattern::Rnd, AccessPattern::Rnd},
                  {AccessPattern::RndInc, AccessPattern::RndInc, AccessPattern::RndInc, AccessPattern::Rnd, AccessPattern::Rnd},
                  {AccessPattern::SeqDec, AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::SeqDec, AccessPattern::RndDec},
                  {AccessPattern::RndDec, AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::RndDec, AccessPattern::RndDec},
                  {AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::Rnd}}};
};

class SegmentAccessStatistics {
 public:
  void on_iterator_create(uint64_t count);

  void on_iterator_create(const std::shared_ptr<const PosList>& positions);

  void on_accessor_create(uint64_t count);

  void on_accessor_access(uint64_t count, ChunkOffset chunk_offset);

  void on_dictionary_access(uint64_t count);

  void on_other_access(uint64_t count);

  void reset();

  SegmentAccessCounter<uint64_t> counter() const;

 private:
  SegmentAccessCounter<std::atomic_uint64_t> _counter;
};

}  // namespace opossum
