#pragma once

#include <array>
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#include <types.hpp>
#include <storage/pos_list.hpp>
//#include <storage/table.hpp>

namespace opossum {
class Table;
// -----------------------------------------------------------------------------------------------------------------
enum SegmentAccessType {
  Other,
  IteratorCreate,
  IteratorAccess,
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
class AtomicAccessStrategy {
 public:
  uint64_t count(SegmentAccessType type) const;

  void reset();

  void increase(SegmentAccessType type, uint64_t count);

 private:
  std::array<std::atomic_uint64_t, SegmentAccessType::Count> _count;
};

// -----------------------------------------------------------------------------------------------------------------
class NonLockingStrategy {
 public:
  uint64_t count(SegmentAccessType type) const;

  void reset();

  void increase(SegmentAccessType type, uint64_t count);

 private:
  std::array<uint64_t, SegmentAccessType::Count> _count;
};

// -----------------------------------------------------------------------------------------------------------------
template<class DataAccessStrategy>
class BulkCountingStrategy {
 public:
  explicit BulkCountingStrategy(DataAccessStrategy& data_access_strategy)
    : _data_access_strategy {data_access_strategy} {}

  void on_iterator_create(uint64_t count) {
    _data_access_strategy.increase(IteratorCreate, 1);
    _data_access_strategy.increase(IteratorAccess, count);
  }

  void on_iterator_create(const std::shared_ptr<const PosList>& positions) {
    _data_access_strategy.increase(IteratorCreate, 1);
    _data_access_strategy.increase(IteratorAccess, positions->size());
  }

  void on_iterator_dereference(uint64_t count) {}

  void on_iterator_dereference(uint64_t count, ChunkOffset chunk_offset) {}

  void on_accessor_create(uint64_t count) {
    _data_access_strategy.increase(AccessorCreate, 1);
  }

  void on_accessor_access(uint64_t count, ChunkOffset chunk_offset) {
    _data_access_strategy.increase(AccessorAccess, count);
  }

  void on_dictionary_access(uint64_t count) {
    _data_access_strategy.increase(DictionaryAccess, count);
  }

  void on_other_access(uint64_t count) {
    _data_access_strategy.increase(Other, count);
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
    _data_access_strategy.increase(IteratorCreate, 1);
  }

  void on_iterator_create(const std::shared_ptr<const PosList>& positions) {
    _data_access_strategy.increase(IteratorCreate, 1);
  }

  void on_iterator_dereference(uint64_t count) {
    _data_access_strategy.increase(IteratorAccess, count);
  }

  void on_iterator_dereference(uint64_t count, ChunkOffset chunk_offset) {
    _data_access_strategy.increase(IteratorAccess, count);
  }

  void on_accessor_create(uint64_t count) {
    _data_access_strategy.increase(AccessorCreate, 1);
  }

  void on_accessor_access(uint64_t count, ChunkOffset chunk_offset) {
    _data_access_strategy.increase(AccessorAccess, count);
  }

  void on_dictionary_access(uint64_t count) {
    _data_access_strategy.increase(DictionaryAccess, count);
  }

  void on_other_access(uint64_t count) {
    _data_access_strategy.increase(Other, count);
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

  std::string to_string() const {
    std::string str;
    str.reserve(SegmentAccessType::Count * 4);
    str.append(std::to_string(count(static_cast<SegmentAccessType>(0))));

    for (uint8_t type = 1; type < Count; ++type) {
      str.append(",");
      str.append(std::to_string(count(static_cast<SegmentAccessType>(type))));
    }

    return str;
  }

  static void save_to_csv(const std::map<std::string, std::shared_ptr<Table>>& tables, const std::string& path) {
    std::ofstream output_file{path};
    output_file << "table_name,column_name,chunk_id,row_count,Other,IteratorCreate,IteratorAccess,AccessorCreate,"
                   "AccessorAccess,DictionaryAccess,EstimatedMemoryUsage\n";
    // iterate over all tables, chunks and segments
    for (const auto&[table_name, table_ptr] : tables) {
      for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
        const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
        for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
             column_id < count; ++column_id) {
          const auto& column_name = table_ptr->column_name(column_id);
          const auto& segment_ptr = chunk_ptr->get_segment(column_id);
          const auto& access_statistics = segment_ptr->access_statistics();
          output_file << table_name << ',' << column_name << ',' << chunk_id << ',' << segment_ptr->size() << ','
                      << access_statistics.to_string() << ',' << segment_ptr->estimate_memory_usage() << '\n';
        }
      }
    }

//  for (auto&[key, value] : _statistics)
//    output_file << "key" << key << ": " << value.to_string() << std::endl;
    output_file.close();
  }

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
  }

 private:
  AccessStrategyType _data_access_strategy;
  CountingStrategyType _counting_strategy;
};

  using SegmentAccessStatistics_T = SegmentAccessStatistics<AtomicAccessStrategy, BulkCountingStrategy<AtomicAccessStrategy>>;
}  // namespace opossum
