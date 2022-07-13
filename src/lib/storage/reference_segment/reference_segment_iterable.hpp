#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <boost/sort/sort.hpp>

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/job_task.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"


#include "operators/print.hpp"

namespace opossum {

template <typename T>
struct Testili {

  explicit Testili(const AnySegmentIterator<T>& init_begin, const AnySegmentIterator<T>& init_end, const ChunkOffset init_chunk_offset)
    : begin{init_begin}, end{init_end}, chunk_offset{init_chunk_offset} {}

  AnySegmentIterator<T> begin;
  AnySegmentIterator<T> end;
  ChunkOffset chunk_offset;
};

template <typename T, EraseReferencedSegmentType erase_reference_segment_type>
class ReferenceSegmentIterable : public SegmentIterable<ReferenceSegmentIterable<T, erase_reference_segment_type>> {
 public:
  using ValueType = T;

  explicit ReferenceSegmentIterable(const ReferenceSegment& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto referenced_table = _segment.referenced_table();
    const auto referenced_column_id = _segment.referenced_column_id();

    const auto& position_filter = _segment.pos_list();

    // If we are guaranteed that the reference segment refers to a single non-NULL chunk, we can do some
    // optimizations. For example, we can use a filtered iterator instead of having to create segments accessors
    // and using virtual method calls.

    if (position_filter->references_single_chunk() && position_filter->size() > 0) {
      // If a single chunk is referenced, we use the PosList as a filter for the referenced segment iterable.
      // This assumes that the PosList itself does not contain any NULL values. As NULL-producing operators
      // (Join, Aggregate, Projection) do not emit a PosList with references_single_chunk, we can assume that the
      // PosList has no NULL values. However, once we have a `has_null_values` flag in a smarter PosList, we should
      // use it here.

      const auto referenced_segment =
          referenced_table->get_chunk(position_filter->common_chunk_id())->get_segment(referenced_column_id);

      bool functor_was_called = false;

      if constexpr (erase_reference_segment_type == EraseReferencedSegmentType::No) {
        resolve_segment_type<T>(*referenced_segment, [&](const auto& typed_segment) {
          using SegmentType = std::decay_t<decltype(typed_segment)>;

          // This is ugly, but it allows us to define segment types that we are not interested in and save a lot of
          // compile time during development. While new segment types should be added here,
#ifdef HYRISE_ERASE_DICTIONARY
          if constexpr (std::is_same_v<SegmentType, DictionarySegment<T>>) {
            return;
          }
#endif

#ifdef HYRISE_ERASE_RUNLENGTH
          if constexpr (std::is_same_v<SegmentType, RunLengthSegment<T>>) {
            return;
          }
#endif

#ifdef HYRISE_ERASE_FIXEDSTRINGDICTIONARY
          if constexpr (std::is_same_v<SegmentType, FixedStringDictionarySegment<T>>) {
            return;
          }
#endif

#ifdef HYRISE_ERASE_FRAMEOFREFERENCE
          if constexpr (std::is_same_v<T, int32_t>) {
            if constexpr (std::is_same_v<SegmentType, FrameOfReferenceSegment<T>>) {
              return;
            }
          }
#endif

          // Always erase LZ4Segment accessors
          if constexpr (std::is_same_v<SegmentType, LZ4Segment<T>>) {
            return;
          }

          if constexpr (!std::is_same_v<SegmentType, ReferenceSegment>) {
            const auto segment_iterable = create_iterable_from_segment<T>(typed_segment);
            segment_iterable.with_iterators(position_filter, functor);

            functor_was_called = true;
          } else {
            Fail("Found ReferenceSegment pointing to ReferenceSegment");
          }
        });

        if (!functor_was_called) {
          PerformanceWarning("ReferenceSegmentIterable for referenced segment type erased by compile-time setting");
        }

      } else {
        PerformanceWarning("Using type-erased accessor as the ReferenceSegmentIterable is type-erased itself");
      }

      if (functor_was_called) {
        return;
      }

      // The functor was not called yet, because we did not instantiate specialized code for the segment type.
      const auto segment_iterable = create_any_segment_iterable<T>(*referenced_segment);
      segment_iterable.with_iterators(position_filter, functor);
    } else {
      using Accessors = std::vector<std::shared_ptr<AbstractSegmentAccessor<T>>>;

      auto accessors = std::make_shared<Accessors>(referenced_table->chunk_count());

      resolve_pos_list_type(position_filter, [&](auto resolved_position_filter) {
        using PosListIteratorType = std::decay_t<decltype(position_begin_it)>;

        const auto position_begin_it = resolved_position_filter->begin();
        const auto position_end_it = resolved_position_filter->end();
        const auto pos_list_size = std::distance(position_begin_it, position_end_it);

        auto begin = MultipleChunkIterator<PosListIteratorType>{referenced_table, referenced_column_id, accessors,
                                                                position_begin_it, position_end_it, position_begin_it,
                                                                pos_list_size, ChunkOffset{0}};
        auto end = MultipleChunkIterator<PosListIteratorType>{referenced_table, referenced_column_id, accessors,
                                                              position_begin_it, position_end_it, position_end_it,
                                                              pos_list_size, static_cast<ChunkOffset>(pos_list_size)};

        functor(begin, end);
      });
    }
  }

  size_t _on_size() const {
    return _segment.size();
  }

 private:
  const ReferenceSegment& _segment;

 private:
  // The iterator for cases where we potentially iterate over multiple referenced chunks
  template <typename PosListIteratorType>
  class MultipleChunkIterator
      : public AbstractSegmentIterator<MultipleChunkIterator<PosListIteratorType>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ReferenceSegmentIterable<T, erase_reference_segment_type>;

   public:
    explicit MultipleChunkIterator(
        const std::shared_ptr<const Table>& referenced_table, const ColumnID referenced_column_id,
        const std::shared_ptr<std::vector<std::shared_ptr<AbstractSegmentAccessor<T>>>>& accessors,
        const PosListIteratorType& begin_pos_list_it, const PosListIteratorType& end_pos_list_it,
        const PosListIteratorType& pos_list_it, const int64_t pos_list_size, const ChunkOffset chunk_offset)
        : _referenced_table{referenced_table},
          _referenced_column_id{referenced_column_id},
          _begin_pos_list_it{begin_pos_list_it},
          _end_pos_list_it{end_pos_list_it},
          _pos_list_it{pos_list_it},
          _pos_list_size{pos_list_size},
          _accessors{accessors},
          _chunk_offset{chunk_offset} {
      // std::cout << "Passed position list (chunk offset is " << _chunk_offset << "): ";
      // auto iter = begin_pos_list_it;
      // while (iter != end_pos_list_it) {
      //   std::cout << *iter << " ";
      //   ++iter;
      // }
      // std::cout << std::endl;
      DebugAssert(_referenced_table->type() == TableType::Data, "Referenced table must be a data table.");

      // The setup can be quite expensive. As it is not required for simple end() iterators that are never moved, we
      // do not initialze all data structures for end() iterators. This comes with the cost, that we now have to check
      // for every iterator movements whether the data structures are already initialized, because someone might
      // decrement end().
      if (_chunk_offset < pos_list_size) {
        _initialize();
      }
    }

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      if (!_is_initialized) {
        _initialize();
      }

      ++_chunk_offset;
    }

    void decrement() {
      if (!_is_initialized) {
        _initialize();
      }

      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      if (!_is_initialized) {
        _initialize();
      }

      _chunk_offset += n;
    }

    bool equal(const MultipleChunkIterator& other) const {
      // std::cout << "Comparing " << _chunk_offset << " and " << other._chunk_offset << std::endl;
      return _chunk_offset == other._chunk_offset;
    }

    std::ptrdiff_t distance_to(const MultipleChunkIterator& other) const {
      return static_cast<std::ptrdiff_t>(other._chunk_offset) - static_cast<std::ptrdiff_t>(_chunk_offset);
    }

    SegmentPosition<T> dereference() const {
      if (_pos_list_chunk_offsets[_chunk_offset].first == INVALID_CHUNK_ID) {
        return SegmentPosition<T>{T{}, true, _chunk_offset};
      }

      DebugAssert(_is_initialized, "Dereferencing uninitialized multi-segment iterator.");
      DebugAssert(_chunk_offset < _pos_list_chunk_offsets.size(), "Invalid access into offsets.");
      const auto& pos_list_chunk_offset = _pos_list_chunk_offsets[_chunk_offset];
      // std::cout << "Trying to obtain iterator of chunk " << pos_list_chunk_offset.first << " with added offset " << pos_list_chunk_offset.second << std::endl;
      const auto iter = *_iterators_by_chunk[pos_list_chunk_offset.first] + pos_list_chunk_offset.second;

      // std::cout << "Returning value " << iter->value() << " and is it NULL? " << iter->is_null() << " and chunk_offset: (iter: " << iter->chunk_offset() << ", anysegiter: " << _chunk_offset << ")" << std::endl;
      // We need to adapt the chunk_offset. It might stem from a temporary pos list.
      return SegmentPosition<T>{iter->value(), iter->is_null(), _chunk_offset};
    }

    void _create_accessor(const ChunkID chunk_id) const {
      auto segment = _referenced_table->get_chunk(chunk_id)->get_segment(_referenced_column_id);
      auto accessor = std::move(create_segment_accessor<T>(segment));
      (*_accessors)[chunk_id] = std::move(accessor);
    }

    /*
     * Simple heuristics to determine if a segment should be materialized. For LZ4 segments, this is always the
     * case as we yield a sequential access pattern.
     */
    bool _is_materialization_beneficial(const std::shared_ptr<AbstractSegment>& segment,
                                        const std::shared_ptr<RowIDPosList>& pos_list) {
      const auto encoding_spec = get_segment_encoding_spec(segment);
      if (encoding_spec.encoding_type == EncodingType::Dictionary || encoding_spec.encoding_type == EncodingType::FixedStringDictionary) {
        return false;
      }

      if (SegmentAccessCounter::access_type(*pos_list) != SegmentAccessCounter::AccessType::Random) {
        return false;
      }

      if (encoding_spec.encoding_type == EncodingType::LZ4) {
        return true;
      }

      // TODO: main purpose is a segment's allocator. Do it!
      return false;
    }

    void materialize_segment(const ChunkID chunk_id) {
      const auto& segment = _referenced_table->get_chunk(chunk_id)->get_segment(_referenced_column_id);
      const auto& pos_list = *(_positions_by_chunk[chunk_id]);
      const auto pos_list_size = pos_list.size();
      auto sortable_pos_list = std::vector<std::pair<RowID, size_t>>{pos_list_size};

      for (auto index = size_t{0}; index < pos_list_size; ++index) {
        sortable_pos_list[index] = {pos_list[index], index};
      }

      // std::cout << "#########" << std::endl;
      // for (const auto& [a, b] : sortable_pos_list) std::cout << "[" << a << " - " << b << "] ";
      // std::cout << "#########" << std::endl;

      boost::sort::pdqsort(sortable_pos_list.begin(), sortable_pos_list.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

      // std::cout << "#########" << std::endl;
      // for (const auto& [a, b] : sortable_pos_list) std::cout << "[" << a << "," << b << "] ";
      // std::cout << "#########" << std::endl;

      // TODO: evaluate if we should introduce a new type of "re-sortable" PosList that uses a pair, but can still be
      // iterated "traditionally".
      _sorted_pos_lists[chunk_id] = std::make_shared<RowIDPosList>(pos_list_size);
      auto& sorted_pos_list_ref = *(_sorted_pos_lists[chunk_id]);
      for (auto index = size_t{0}; index < pos_list_size; ++index) {
        sorted_pos_list_ref[index] = sortable_pos_list[index].first;
      }

      pmr_vector<ValueType> values(pos_list_size);
      pmr_vector<bool> nulls(pos_list_size);

      DebugAssert(_sorted_pos_lists[chunk_id]->size() == pos_list_size,
                  "Size of sorted position list differs from initial position list.");

      sorted_pos_list_ref.guarantee_single_chunk();
      resolve_segment_type<ValueType>(*segment, [&](const auto& typed_segment) {
        const auto iterable = create_any_segment_iterable<T>(typed_segment);
        iterable.with_iterators(_sorted_pos_lists[chunk_id], [&](auto begin, const auto& end) {
          auto index = size_t{0};
          while (begin != end) {
            // This is a scathered write, which can be very cache unfriendly. But since we need to pass values and
            // nulls later on as vectors anyways, a composite data structure with sorting might not be much more
            // efficient either.
            values[sortable_pos_list[index].second] = begin->value();
            nulls[sortable_pos_list[index].second] = begin->is_null();
            ++begin;
            ++index;
          }
        });
      });

      // std::cout << "#########" << std::endl;
      // for (const auto& value : values) std::cout << value << " ";
      // std::cout << "#########" << std::endl;

      const auto value_segment = std::make_shared<ValueSegment<ValueType>>(std::move(values), std::move(nulls));
      _materialized_segments[chunk_id] = std::move(value_segment);
      const auto iterable = create_any_segment_iterable<T>(*(_materialized_segments[chunk_id]));
      iterable.with_iterators([&](auto begin, const auto& end) {
        DebugAssert(std::distance(begin, end) == static_cast<int64_t>(pos_list_size), "TODO");
        DebugAssert(std::distance(begin, end) == _materialized_segments[chunk_id]->size(), "TODO");
        _iterators_by_chunk[chunk_id] = std::move(begin);
      });
    }

    void _initialize() {
      // std::cout << "initializing pos list" << std::endl;
      _pos_list_chunk_offsets.resize(_pos_list_size);

      const auto chunk_count = _referenced_table->chunk_count();
      _positions_by_chunk.resize(chunk_count);
      _iterators_by_chunk.resize(chunk_count);

      const auto estimated_row_count_per_chunk = std::lround(static_cast<float>(_pos_list_size) / static_cast<float>(_pos_list_size));

      // Create positions by chunk.
      // Create any segment iterables for every chunk with their current position.
      // We'll have a pair of <ChunkID, AnySegmentOffset> to know how far we have to advance begin() for each item
      // of the inital position list.
      auto temporary_chunk_counters = std::vector<ChunkOffset>(chunk_count);
      auto insert_index = size_t{0};
      for (auto iter = _begin_pos_list_it; iter != _end_pos_list_it; ++iter, ++insert_index) {
        const auto& pos_list_item = *iter;
        const auto& chunk_id = pos_list_item.chunk_id;

        if (pos_list_item == NULL_ROW_ID) {
          _pos_list_chunk_offsets[insert_index] = {INVALID_CHUNK_ID, ChunkOffset{0}};
          continue;
        }

        if (!_positions_by_chunk[chunk_id]) {
          _positions_by_chunk[chunk_id] = std::make_shared<RowIDPosList>();
          _positions_by_chunk[chunk_id]->reserve(estimated_row_count_per_chunk);
        }

        _positions_by_chunk[chunk_id]->emplace_back(pos_list_item);
        _pos_list_chunk_offsets[insert_index] = {chunk_id, temporary_chunk_counters[chunk_id]};
        ++temporary_chunk_counters[chunk_id];
      }

      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(chunk_count);

      // for (auto& [chunk_id, pos_list] : _positions_by_chunk) {
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        auto& pos_list = _positions_by_chunk[chunk_id];
        if (!pos_list) {
          continue;
        }

        pos_list->guarantee_single_chunk();

        const auto& segment = _referenced_table->get_chunk(chunk_id)->get_segment(_referenced_column_id);
        resolve_segment_type<ValueType>(*segment, [&, chunk_id=chunk_id, &pos_list=pos_list](const auto& typed_segment) {
          if (_is_materialization_beneficial(segment, pos_list)) {
            if (_sorted_pos_lists.capacity() < chunk_count) {
              // Only reserve the full vector (for direct positional access) in case we'll create sorted post lists.
              _sorted_pos_lists.resize(chunk_count);
              _materialized_segments.resize(chunk_count);
            }

            jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id=chunk_id]() {
              materialize_segment(chunk_id);
            }));
            jobs.back()->schedule();
            // Hyrise::get().scheduler()->wait_for_tasks(jobs);
            return;
          }

          const auto iterable = create_any_segment_iterable<T>(typed_segment);
          iterable.with_iterators(pos_list, [&](auto begin, const auto& end) {
            _iterators_by_chunk[chunk_id] = std::move(begin);
          });
        });
      }

      Hyrise::get().scheduler()->wait_for_tasks(jobs);

      // std::cout << "POS_LIST:                ";
      // for (auto iter = _begin_pos_list_it; iter != _end_pos_list_it; ++iter) {
      //   const auto& pos_list_item = *iter;
      //   std::cout << pos_list_item << " ";
      // }
      // std::cout << std::endl;

      // std::cout << "_pos_list_chunk_offsets: ";
      // for (const auto& pos_list_chunk_offset : _pos_list_chunk_offsets) {
      //   std::cout << "[" << pos_list_chunk_offset.first << "," << pos_list_chunk_offset.second << "] ";
      // }
      // std::cout << std::endl;

      // std::cout << "initial pos list size: " << std::distance(_begin_pos_list_it, _end_pos_list_it) << std::endl;
      // auto iter = _begin_pos_list_it;
      // for (auto index = long{0}; index < std::min(long{20}, std::distance(_begin_pos_list_it, _end_pos_list_it)); ++index) {
      //   std::cout << *iter << " ";
      //   ++iter;
      // }
      // std::cout << "\n sub pos lists: ";
      // for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      //   std::cout << "[chunk #" << chunk_id << ": " << _positions_by_chunk[chunk_id]->size() << "] ";
      // }
      // std::cout << std::endl;

      _is_initialized = true;
    }

   private:
    std::shared_ptr<const Table> _referenced_table;
    ColumnID _referenced_column_id;

    std::vector<std::shared_ptr<RowIDPosList>> _positions_by_chunk{};
    // std::vector<AnySegmentIterator<T>> _iterators_by_chunk{};
    // std::optional to allow default initialization of vector via resize().
    std::vector<std::optional<AnySegmentIterator<T>>> _iterators_by_chunk{};

    std::vector<std::shared_ptr<RowIDPosList>> _sorted_pos_lists{};
    std::vector<std::shared_ptr<ValueSegment<ValueType>>> _materialized_segments{};

    // Stores for every initial item in the pos list: which chunk is referenced and at
    // which offset this item is within the chunk-only pos list. This is NOT a RowID.
    std::vector<std::pair<ChunkID, ChunkOffset>> _pos_list_chunk_offsets{};

    PosListIteratorType _begin_pos_list_it;
    PosListIteratorType _end_pos_list_it;
    PosListIteratorType _pos_list_it;

    int64_t _pos_list_size;

    // PointAccessIterators share vector with one Accessor per Chunk
    std::shared_ptr<std::vector<std::shared_ptr<AbstractSegmentAccessor<T>>>> _accessors;

    ChunkOffset _chunk_offset;

    bool _is_initialized{false};
  };
};

template <typename T>
struct is_reference_segment_iterable {
  static constexpr auto value = false;
};

template <template <typename, EraseReferencedSegmentType> typename Iterable, typename T,
          EraseReferencedSegmentType erase_reference_segment_type>
struct is_reference_segment_iterable<Iterable<T, erase_reference_segment_type>> {
  static constexpr auto value = std::is_same_v<ReferenceSegmentIterable<T, erase_reference_segment_type>,
                                               Iterable<T, erase_reference_segment_type>>;
};

template <typename T>
inline constexpr bool is_reference_segment_iterable_v = is_reference_segment_iterable<T>::value;

}  // namespace opossum
