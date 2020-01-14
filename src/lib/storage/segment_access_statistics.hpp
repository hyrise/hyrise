#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "types.hpp"
#include "storage/pos_list.hpp"

namespace opossum {
class Table;
class BaseSegment;
class Chunk;

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
  // There are six AccessPatterns:
  // 0 (unknown), equivalent to IteratorSeqAccess
  // 1 (sequentially increasing), difference between two neighboring elements is 0 or 1.
  // 2 (increasing randomly)
  // 3 (sequentially decreasing), difference between two neighboring elements is -1 or 0.
  // 4 (decreasing randomly)
  // 5 (random access)
  enum class AccessPattern {Unknown, SeqInc, RndInc, SeqDec, RndDec, Rnd};

  static AccessPattern iterator_access_pattern(const std::shared_ptr<const PosList>& positions);

  static SegmentAccessCounter<uint64_t> fetch_counter(const std::shared_ptr<BaseSegment>& segment);
  using ColumnIDAccessStatisticsPair = std::pair<const ColumnID, const SegmentAccessCounter<uint64_t>>;
  static std::vector<ColumnIDAccessStatisticsPair> fetch_counters(const std::shared_ptr<Chunk>& chunk);
  using ChunkIDColumnIDsPair = std::pair<const ChunkID, std::vector<ColumnIDAccessStatisticsPair>>;
  static std::vector<ChunkIDColumnIDsPair> fetch_counters(const std::shared_ptr<Table>& table);

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

  static void save_to_csv(const std::map<std::string, std::shared_ptr<Table>>& tables,
    const std::string& path_to_meta_data, const std::string& path_to_access_statistics);

  /**
   * Resets access statistics of every segment in table
   * @param tables map of tables
   */
  static void reset_all(const std::map<std::string, std::shared_ptr<Table>>& tables);

 private:
  SegmentAccessCounter<std::atomic_uint64_t> _counter;

};

}  // namespace opossum
