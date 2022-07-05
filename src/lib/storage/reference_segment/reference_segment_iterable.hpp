#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"

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
        const auto position_begin_it = resolved_position_filter->begin();
        const auto position_end_it = resolved_position_filter->end();

        using PosListIteratorType = std::decay_t<decltype(position_begin_it)>;

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
        const PosListIteratorType& pos_list_it, const long pos_list_size, const ChunkOffset chunk_offset)
        : _referenced_table{referenced_table},
          _referenced_column_id{referenced_column_id},
          _begin_pos_list_it{begin_pos_list_it},
          _pos_list_it{pos_list_it},
          _accessors{accessors},
          _chunk_offset{chunk_offset} {
            // TODO: skip the whole initialization until chunk_offset is moved. Otherwise, end() is too
            // expensive and we want to ensure that end can be decremented.
            const auto estimated_row_count_per_chunk = std::lround(static_cast<float>(referenced_table->row_count()) / static_cast<float>(pos_list_size));
            _positions_by_chunk.reserve(referenced_table->chunk_count());

            // Create positions by chunk.
            // Create any segment iterables for every chunk with their current position.
            // We'll have a pair of <ChunkID, AnySegmentOffset> to 
            auto temporary_chunk_counters = std::unordered_map<ChunkID, ChunkOffset>{};
            for (auto iter = begin_pos_list_it; iter != end_pos_list_it; ++iter) {
              const auto& pos_list_item = *iter;

              if (pos_list_item == NULL_ROW_ID) {
                // _pos_list_chunk_offsets.emplace_back(NULL_ROW_ID);
                _pos_list_chunk_offsets.emplace_back(INVALID_CHUNK_ID, ChunkOffset{0});
                continue;
              }

              if (!_positions_by_chunk.contains(pos_list_item.chunk_id)) {
                _positions_by_chunk[pos_list_item.chunk_id] = std::make_shared<RowIDPosList>();
                _positions_by_chunk[pos_list_item.chunk_id]->reserve(estimated_row_count_per_chunk);
                temporary_chunk_counters[pos_list_item.chunk_id] = 0;
              }

              _positions_by_chunk[pos_list_item.chunk_id]->emplace_back(pos_list_item);
              _pos_list_chunk_offsets.emplace_back(pos_list_item.chunk_id, temporary_chunk_counters[pos_list_item.chunk_id]);
              ++temporary_chunk_counters[pos_list_item.chunk_id];
            }

            for (auto& [chunk_id, pos_list] : _positions_by_chunk) {
              pos_list->guarantee_single_chunk();
              // std::cout << "\n" << chunk_id;
              // for (auto item : *pos_list) {
              //   std::cout << "aaa " << item;
              // }

              const auto& segment = referenced_table->get_chunk(chunk_id)->get_segment(referenced_column_id);
              resolve_data_and_segment_type(*segment, [&, chunk_id=chunk_id, pos_list=pos_list](const auto data_type_t, const auto& typed_segment) {
                // using ColumnDataType = typename decltype(data_type_t)::type;

                const auto iterable = create_any_segment_iterable<T>(typed_segment);
                iterable.with_iterators(pos_list, [&](auto begin, const auto& end) {
                  // AnySegmentIterator<T> test = begin;
                  // AnySegmentIterator<T> test2 = end;
                  // auto test3 = std::tuple<AnySegmentIterator<T>, AnySegmentIterator<T>, ChunkOffset>{begin, end, ChunkOffset{0}};
                  // auto testili = Testili<T>(begin, end, ChunkOffset{0});
                  // std::cout << std::get<2>(test3) << std::endl;
                  // _iterators_by_chunk[chunk_id] = std::tuple<AnySegmentIterator<T>, AnySegmentIterator<T>, ChunkOffset>{begin, end, ChunkOffset{0}};
                  // _iterators_by_chunk[chunk_id] = std::make_tuple(begin, end, ChunkOffset{0});
                  // _iterators_by_chunk[chunk_id] = std::move(test3);
                  // _iterators_by_chunk.emplace(chunk_id, testili);
                  // _iterators_by_chunk.emplace(chunk_id, std::make_tuple(begin, end, ChunkOffset{0}));
                  std::cout << "Adding iterator for chunk #" << chunk_id << " with a pos list size of " << pos_list->size() << std::endl;
                  _iterators_by_chunk.emplace(chunk_id, std::make_tuple(begin, end, ChunkOffset{0}));
                  // std::cout << chunk_id << testili.chunk_offset << std::endl;
                });
              });
            }

            std::cout << "POS_LIST:                ";
            for (auto iter = begin_pos_list_it; iter != end_pos_list_it; ++iter) {
              const auto& pos_list_item = *iter;
              std::cout << pos_list_item << " ";
            }
            std::cout << std::endl;

            std::cout << "_pos_list_chunk_offsets: ";
            for (const auto& pos_list_chunk_offset : _pos_list_chunk_offsets) {
              std::cout << "[" << pos_list_chunk_offset.first << "," << pos_list_chunk_offset.second << "] ";
            }
            std::cout << std::endl;
          }

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;

      if (_pos_list_chunk_offsets[_chunk_offset].first == INVALID_CHUNK_ID) {
        // No need to move an iterator.
        return;
      }

      // Get corresponding iterator and increment.
      const auto& pos_list_chunk_offset = _pos_list_chunk_offsets[_chunk_offset];
      ++std::get<0>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
      ++std::get<2>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
    }

    void decrement() {
      --_chunk_offset;

      if (_pos_list_chunk_offsets[_chunk_offset].first == INVALID_CHUNK_ID) {
        // No need to move an iterator.
        return;
      }

      // Get corresponding iterator and increment.
      const auto& pos_list_chunk_offset = _pos_list_chunk_offsets[_chunk_offset];
      --std::get<0>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
      --std::get<2>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
      std::cout << "Decremented" << std::endl;
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;

      if (_pos_list_chunk_offsets[_chunk_offset].first == INVALID_CHUNK_ID) {
        // No need to move an iterator.
        return;
      }

      // Get corresponding iterator and increment.
      const auto& pos_list_chunk_offset = _pos_list_chunk_offsets[_chunk_offset];
      const auto current_iterator_offset = std::get<2>(_iterators_by_chunk.at(pos_list_chunk_offset.first));

      // Move iterator
      for (auto index = size_t{current_iterator_offset}; index < pos_list_chunk_offset.second; ++index) {
        ++std::get<0>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
        ++std::get<2>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
      }
      for (auto index = int64_t{current_iterator_offset}; index > pos_list_chunk_offset.second; --index) {
        --std::get<0>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
        ++std::get<2>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
      }
      std::cout << "Advanced" << std::endl;
    }

    bool equal(const MultipleChunkIterator& other) const {
      return _pos_list_it == other._pos_list_it;
    }

    std::ptrdiff_t distance_to(const MultipleChunkIterator& other) const {
      return other._chunk_offset - _chunk_offset;
    }

    // TODO(anyone): benchmark if using two maps instead doing the dynamic cast every time really is faster.
    SegmentPosition<T> dereference() const {
      if (_pos_list_chunk_offsets[_chunk_offset].first == INVALID_CHUNK_ID) {
        std::cout << "Return: NULL" << std::endl;
        return SegmentPosition<T>{T{}, true, _chunk_offset};
      }

      const auto& pos_list_chunk_offset = _pos_list_chunk_offsets[_chunk_offset];
      std::cout << "Get element at inital_pos_list_pos" << _chunk_offset << " which links chunk #" << pos_list_chunk_offset.first << " and pos " << pos_list_chunk_offset.second << std::endl;
      std::cout << "Return: " << (*std::get<0>(_iterators_by_chunk.at(pos_list_chunk_offset.first))).value() << " (" << (*std::get<0>(_iterators_by_chunk.at(pos_list_chunk_offset.first))).is_null() << ")" << std::endl;
      return *std::get<0>(_iterators_by_chunk.at(pos_list_chunk_offset.first));

      // // Get current offset the iterator is positioned at.
      // const auto pos_list_chunk_offset = _pos_list_chunk_offsets[_chunk_offset];

      // std::cout << "$$1 " << pos_list_chunk_offset.first << std::endl;
      // const auto current_iterator_offset = std::get<2>(_iterators_by_chunk.at(pos_list_chunk_offset.first));
      // std::cout << "$$2 " << current_iterator_offset << std::endl;

      // auto& testili = _iterators_by_chunk.at(pos_list_chunk_offset.first);
      // auto& iterator = std::get<0>(testili);

      // Move iterator
      // for (auto index = size_t{current_iterator_offset}; index < pos_list_chunk_offset.second; ++index) {
      //   ++iterator;
      // }
      // for (auto index = int64_t{current_iterator_offset}; index > pos_list_chunk_offset.second; --index) {
      //   --iterator;
      // }

      // return *iterator;

      // return SegmentPosition<T>{T{}, true, _chunk_offset};
      // if (_pos_list_it->is_null()) {
      //   return SegmentPosition<T>{T{}, true, pos_list_offset};
      // }

      // const auto chunk_id = _pos_list_it->chunk_id;

      // if (!(*_accessors)[chunk_id]) {
      //   _create_accessor(chunk_id);
      // }

      // const auto chunk_offset = _pos_list_it->chunk_offset;

      // const auto typed_value = (*_accessors)[chunk_id]->access(chunk_offset);

      // if (typed_value) {
      //   return SegmentPosition<T>{std::move(*typed_value), false, pos_list_offset};
      // } else {
      //   return SegmentPosition<T>{T{}, true, pos_list_offset};
      // }
    }

    void _create_accessor(const ChunkID chunk_id) const {
      auto segment = _referenced_table->get_chunk(chunk_id)->get_segment(_referenced_column_id);
      auto accessor = std::move(create_segment_accessor<T>(segment));
      (*_accessors)[chunk_id] = std::move(accessor);
    }

   private:
    std::shared_ptr<const Table> _referenced_table;
    ColumnID _referenced_column_id;

    std::unordered_map<ChunkID, std::shared_ptr<RowIDPosList>> _positions_by_chunk{};
    // std::unordered_map<ChunkID, Testili<T>> _iterators_by_chunk{};
    std::unordered_map<ChunkID, std::tuple<AnySegmentIterator<T>, AnySegmentIterator<T>, ChunkOffset>> _iterators_by_chunk{};

    // Stores for every initial item in the pos list: which chunk is referenced and at
    // which offset this item is within the chunk-only pos list. This is NOT a RowID.
    std::vector<std::pair<ChunkID, ChunkOffset>> _pos_list_chunk_offsets{};

    PosListIteratorType _begin_pos_list_it;
    PosListIteratorType _pos_list_it;

    // PointAccessIterators share vector with one Accessor per Chunk
    std::shared_ptr<std::vector<std::shared_ptr<AbstractSegmentAccessor<T>>>> _accessors;

    ChunkOffset _chunk_offset;
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
