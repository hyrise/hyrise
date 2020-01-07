#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#include <types.hpp>
#include <storage/pos_list.hpp>

namespace opossum {
class Table;
// -----------------------------------------------------------------------------------------------------------------
enum class SegmentAccessType {
  Other,
  IteratorCreate,
  IteratorSeqAccess,
  IteratorIncreasingAccess,
  IteratorRandomAccess,
  IteratorDereference,
  AccessorCreate,
  AccessorAccess,
  DictionaryAccess,
  // number of elements in enum
    Count
};

// -----------------------------------------------------------------------------------------------------------------
enum SegmentType {
  Dictionary,
  FrameOfReference,
  LZ4,
  Reference,
  RunLength,
  ValueS
};

// -----------------------------------------------------------------------------------------------------------------

class SegmentAccessTypeTools {
 public:
  inline static const std::string headers = "Other,IteratorCreate,IteratorSeqAccess,IteratorIncreasingAccess,"
                                            "IteratorRandomAccess,IteratorDereference,AccessorCreate,AccessorAccess,"
                                            "DictionaryAccess";

  static SegmentAccessType iterator_access_pattern(const std::shared_ptr<const PosList>& positions);

 private:
  // There are five states:
  // 0 (unknown), equivalent to IteratorSeqAccess
  // 1 (sequentially increasing), difference between two neighboring elements is 0 or 1.
  // 2 (increasing randomly)
  // 3 (sequentially decreasing), difference between two neighboring elements is -1 or 0.
  // 4 (decreasing randomly)
  // 5 (random access)
  enum class State {Unknown, SeqInc, RndInc, SeqDec, RndDec, Rnd, Count};

  // There are five possible inputs
  enum class Input {Zero, One, Positive, NegativeOne, Negative, Count};

  constexpr static const std::array<std::array<State, static_cast<uint32_t>(Input::Count)>, static_cast<uint32_t>(State::Count)>
    _transitions{{{State::Unknown, State::SeqInc, State::SeqInc, State::SeqDec, State::SeqDec},
                  {State::SeqInc, State::SeqInc, State::RndInc, State::Rnd, State::Rnd},
                  {State::RndInc, State::RndInc, State::RndInc, State::Rnd, State::Rnd},
                  {State::SeqDec, State::Rnd, State::Rnd, State::SeqDec, State::RndDec},
                  {State::RndDec, State::Rnd, State::Rnd, State::RndDec, State::RndDec},
                  {State::Rnd, State::Rnd, State::Rnd, State::Rnd, State::Rnd}}};
};
// -----------------------------------------------------------------------------------------------------------------
class AtomicAccessStrategy {
 public:
  uint64_t count(SegmentAccessType type) const;

  void reset();

  void increase(SegmentAccessType type, uint64_t count);

  static std::string header();

  std::vector<std::string> to_string() const;

 private:
  std::array<std::atomic_uint64_t, static_cast<uint32_t>(SegmentAccessType::Count)> _count;
};

// -----------------------------------------------------------------------------------------------------------------
class NoAccessStrategy {
 public:
  uint64_t count(SegmentAccessType type) const;

  void reset();

  void increase(SegmentAccessType type, uint64_t count);

  static std::string header();

  std::vector<std::string> to_string() const;
};

// -----------------------------------------------------------------------------------------------------------------
class AtomicTimedAccessStrategy {
 public:
  explicit AtomicTimedAccessStrategy();

  uint64_t count(SegmentAccessType type) const;

  void reset();

  void increase(SegmentAccessType type, uint64_t count);

  static std::string header();

  std::vector<std::string> to_string() const;

  static std::chrono::time_point<std::chrono::steady_clock> start_time;
  static std::chrono::duration<double, std::milli> interval;

 private:

  struct Counter {
    uint64_t timestamp;
    std::array<std::atomic_uint64_t, static_cast<uint32_t>(SegmentAccessType::Count)> counter;

    Counter(uint64_t timestamp) : timestamp{timestamp}, counter{} {};
    Counter() : Counter(0ul) {}
    Counter(const Counter& other) {
      timestamp = other.timestamp;
      for (auto type = 0ul; type < counter.size(); ++type) {
        counter[type] = other.counter[type].load();
      }
    }
    Counter(Counter&& other) noexcept {
      timestamp = other.timestamp;
      for (auto type = 0ul; type < counter.size(); ++type) {
        counter[type] = other.counter[type].load();
      }
    }
  };

//  struct Counter {
//    uint64_t timestamp;
//    std::array<uint64_t, static_cast<uint32_t>(SegmentAccessType::Count)> counter;
//
//    Counter(uint64_t timestamp) : timestamp{timestamp}, counter{} {};
//    Counter() : Counter(0ul) {}
//  };

  pmr_concurrent_vector<Counter> _counters;
  Counter& _counter();
};

// -----------------------------------------------------------------------------------------------------------------
class NonLockingStrategy {
 public:
  uint64_t count(SegmentAccessType type) const;

  void reset();

  void increase(SegmentAccessType type, uint64_t count);

  static std::string header();

  std::vector<std::string> to_string() const;

 private:
  std::array<uint64_t, static_cast<uint32_t>(SegmentAccessType::Count)> _count;
};

// -----------------------------------------------------------------------------------------------------------------
template<class DataAccessStrategy>
class BulkCountingStrategy {
 public:
  explicit BulkCountingStrategy(DataAccessStrategy& data_access_strategy)
    : _data_access_strategy {data_access_strategy} {}

  void on_iterator_create(uint64_t count) {
     _data_access_strategy.increase(SegmentAccessType::IteratorCreate, 1);
     _data_access_strategy.increase(SegmentAccessType::IteratorSeqAccess, count);
  }

  void on_iterator_create(const std::shared_ptr<const PosList>& positions) {
    // hier muss geprüft werden, um was für einen Zugriff es sich handelt
    _data_access_strategy.increase(SegmentAccessType::IteratorCreate, 1);
    _data_access_strategy.increase(SegmentAccessTypeTools::iterator_access_pattern(positions), positions->size());
  }

  void on_iterator_dereference(uint64_t count) {}

  void on_iterator_dereference(uint64_t count, ChunkOffset chunk_offset) {}

  void on_accessor_create(uint64_t count) {
    _data_access_strategy.increase(SegmentAccessType::AccessorCreate, 1);
  }

  void on_accessor_access(uint64_t count, ChunkOffset chunk_offset) {
    _data_access_strategy.increase(SegmentAccessType::AccessorAccess, count);
  }

  void on_dictionary_access(uint64_t count) {
    _data_access_strategy.increase(SegmentAccessType::DictionaryAccess, count);
  }

  void on_other_access(uint64_t count) {
    _data_access_strategy.increase(SegmentAccessType::Other, count);
  }

 private:
  DataAccessStrategy& _data_access_strategy;
};

// -----------------------------------------------------------------------------------------------------------------
template<class DataAccessStrategy>
class SingleAccessCountingStrategy {
 public:
  explicit SingleAccessCountingStrategy(DataAccessStrategy& data_access_strategy)
    : _data_access_strategy{data_access_strategy} {}

  void on_iterator_create(uint64_t count) {
    _data_access_strategy.increase(SegmentAccessType::IteratorCreate, 1);
  }

  void on_iterator_create(const std::shared_ptr<const PosList>& positions) {
    _data_access_strategy.increase(SegmentAccessType::IteratorCreate, 1);
  }

  void on_iterator_dereference(uint64_t count) {
    _data_access_strategy.increase(SegmentAccessType::IteratorDereference, count);
  }

  void on_iterator_dereference(uint64_t count, ChunkOffset chunk_offset) {
    _data_access_strategy.increase(SegmentAccessType::IteratorDereference, count);
  }

  void on_accessor_create(uint64_t count) {
    _data_access_strategy.increase(SegmentAccessType::AccessorCreate, 1);
  }

  void on_accessor_access(uint64_t count, ChunkOffset chunk_offset) {
    _data_access_strategy.increase(SegmentAccessType::AccessorAccess, count);
  }

  void on_dictionary_access(uint64_t count) {
    _data_access_strategy.increase(SegmentAccessType::DictionaryAccess, count);
  }

  void on_other_access(uint64_t count) {
    _data_access_strategy.increase(SegmentAccessType::Other, count);
  }

 private:
  DataAccessStrategy& _data_access_strategy;
};

// -----------------------------------------------------------------------------------------------------------------
template<class AccessStrategyType, class CountingStrategyType>
class SegmentAccessStatistics {
 public:
  explicit SegmentAccessStatistics()
    : _data_access_strategy{},
      _counting_strategy{_data_access_strategy} {};

  void on_iterator_create(uint64_t count) {
    _counting_strategy.on_iterator_create(count);
  }

  void on_iterator_create(const std::shared_ptr<const PosList>& positions) {
    _counting_strategy.on_iterator_create(positions);
  }

  void on_iterator_dereference(uint64_t count) {
    _counting_strategy.on_iterator_dereference(count);
  }

  void on_iterator_dereference(uint64_t count, ChunkOffset chunk_offset) {
    _counting_strategy.on_iterator_dereference(count, chunk_offset);
  }

  void on_accessor_create(uint64_t count) {
    _counting_strategy.on_accessor_create(count);
  }

  void on_accessor_access(uint64_t count, ChunkOffset chunk_offset) {
    _counting_strategy.on_accessor_access(count, chunk_offset);
  }

  void on_dictionary_access(uint64_t count) {
    _counting_strategy.on_dictionary_access(count);
  }

  void on_other_access(uint64_t count) {
    _counting_strategy.on_other_access(count);
  }

  uint64_t count(SegmentAccessType type) const {
    return _data_access_strategy.count(type);
  }

  void reset() {
    _data_access_strategy.reset();
  }

  std::vector<std::string> to_string() const {
    std::string str;
    str.reserve(static_cast<uint32_t>(SegmentAccessType::Count) * 4);
    str.append(std::to_string(count(static_cast<SegmentAccessType>(0))));

    for (uint8_t type = 1; type < static_cast<uint32_t>(SegmentAccessType::Count); ++type) {
      str.append(",");
      str.append(std::to_string(count(static_cast<SegmentAccessType>(type))));
    }

    return std::vector<std::string>{str};
  }

  static void save_to_csv(const std::map<std::string, std::shared_ptr<Table>>& tables,
    const std::string& path_to_meta_data, const std::string& path_to_access_statistics) {
    // meta-daten mit abspeichern.
    auto entry_id = 0;
    std::ofstream meta_file{path_to_meta_data};
    std::ofstream output_file{path_to_access_statistics};

    meta_file << "entry_id,table_name,column_name,chunk_id,row_count,EstimatedMemoryUsage\n";
    output_file << "entry_id," + AccessStrategyType::header() + "\n";
    // iterate over all tables, chunks and segments
    for (const auto&[table_name, table_ptr] : tables) {
      for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
        const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
        for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
             column_id < count; ++column_id) {
          const auto& column_name = table_ptr->column_name(column_id);
          const auto& segment_ptr = chunk_ptr->get_segment(column_id);
          const auto& access_statistics = segment_ptr->access_statistics();

          meta_file << entry_id << ',' << table_name << ',' << column_name << ',' << chunk_id << ','
                    << segment_ptr->size() << ',' << segment_ptr->estimate_memory_usage() << '\n';

          for (const auto& str : access_statistics._data_access_strategy.to_string()) {
            output_file << entry_id << ',' << str << '\n';
          }

          ++entry_id;
        }
      }
    }

    meta_file.close();
    output_file.close();
  }

  /**
   * Resets access statistics of every segment in table
   * @param tables map of tables
   */
  static void reset_all(const std::map<std::string, std::shared_ptr<Table>>& tables) {
    for (const auto&[table_name, table_ptr] : tables) {
      for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
        const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
        for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
             column_id < count; ++column_id) {
          const auto& segment_ptr = chunk_ptr->get_segment(column_id);
          segment_ptr->access_statistics().reset();
        }
      }
    }
    AtomicTimedAccessStrategy::start_time = std::chrono::steady_clock::now();
  }

 private:
  AccessStrategyType _data_access_strategy;
  CountingStrategyType _counting_strategy;
};

  using SegmentAccessStatistics_T = SegmentAccessStatistics<AtomicTimedAccessStrategy, BulkCountingStrategy<AtomicTimedAccessStrategy>>;
  // using SegmentAccessStatistics_T = SegmentAccessStatistics<NonLockingStrategy, BulkCountingStrategy<NonLockingStrategy>>;
}  // namespace opossum
