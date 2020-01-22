#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {
class Table;

class SegmentAccessCounter {
  friend class SegmentAccessCounterTest;

 public:
  template <typename T>
  class Counter {
   public:
    T other;
    T iterator_create;
    T iterator_seq_access;
    T iterator_increasing_access;
    T iterator_random_access;
    T accessor_create;
    T accessor_access;
    T dictionary_access;

    Counter();

    void reset();

    T sum() const;

    std::string to_string() const;

    inline static const std::string HEADERS =
        "Other,IteratorCreate,IteratorSeqAccess,IteratorIncreasingAccess,"
        "IteratorRandomAccess,AccessorCreate,AccessorAccess,"
        "DictionaryAccess";
  };

  void on_iterator_create(uint64_t count);

  void on_iterator_create(const std::shared_ptr<const PosList>& positions);

  void on_accessor_create(uint64_t count);

  void on_accessor_access(uint64_t count, ChunkOffset chunk_offset);

  void on_dictionary_access(uint64_t count);

  void on_other_access(uint64_t count);

  void reset();

  Counter<uint64_t> counter() const;

  static void reset(const std::map<std::string, std::shared_ptr<Table>>& tables);

 private:
  Counter<std::atomic_uint64_t> _counter;

  // For access pattern analysis: The following enums are used to determine how an iterator iterates over its elements.
  // This is done by analysing the first elements in a given PosList and a state machine, defined below.
  // There are six AccessPatterns:
  // 0 (unknown), equivalent to IteratorSeqAccess
  // 1 (sequentially increasing), difference between two neighboring elements is 0 or 1.
  // 2 (increasing randomly)
  // 3 (sequentially decreasing), difference between two neighboring elements is -1 or 0.
  // 4 (decreasing randomly)
  // 5 (random access)
  enum class AccessPattern { Unknown, SeqInc, RndInc, SeqDec, RndDec, Rnd };
  // There are five possible inputs
  enum class Input { Zero, One, Positive, NegativeOne, Negative };

  constexpr static const std::array<std::array<AccessPattern, 5 /*|Input|*/>, 6 /*|AccessPattern|*/> _transitions{
      {{AccessPattern::Unknown, AccessPattern::SeqInc, AccessPattern::SeqInc, AccessPattern::SeqDec,
        AccessPattern::SeqDec},
       {AccessPattern::SeqInc, AccessPattern::SeqInc, AccessPattern::RndInc, AccessPattern::Rnd, AccessPattern::Rnd},
       {AccessPattern::RndInc, AccessPattern::RndInc, AccessPattern::RndInc, AccessPattern::Rnd, AccessPattern::Rnd},
       {AccessPattern::SeqDec, AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::SeqDec, AccessPattern::RndDec},
       {AccessPattern::RndDec, AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::RndDec, AccessPattern::RndDec},
       {AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::Rnd, AccessPattern::Rnd}}};

  static AccessPattern _iterator_access_pattern(const std::shared_ptr<const PosList>& positions);
};

}  // namespace opossum
