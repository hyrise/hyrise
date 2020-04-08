#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

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

template <typename T, EraseReferencedSegmentType erase_reference_segment_type>
class ReferenceSegmentIterable : public SegmentIterable<ReferenceSegmentIterable<T, erase_reference_segment_type>> {
 public:
  using ValueType = T;

  explicit ReferenceSegmentIterable(const ReferenceSegment& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto referenced_table = _segment.referenced_table();
    const auto referenced_column_id = _segment.referenced_column_id();

    const auto& pos_list = _segment.pos_list();

    const auto begin_it = pos_list->cbegin();

    // If we are guaranteed that the reference segment refers to a single non-NULL chunk, we can do some optimizations.
    // For example, we can use a single, non-virtual segment accessor instead of having to keep multiple and using
    // virtual method calls. If begin_it is NULL, chunk_id will be INVALID_CHUNK_ID. Therefore, we skip this case.

    if (pos_list->references_single_chunk() && pos_list->size() > 0 && !begin_it->is_null()) {
      // If a single chunk is referenced, we use the PosList as a filter for the referenced segment iterable.
      // This assumes that the PosList itself does not contain any NULL values. As NULL-producing operators
      // (Join, Aggregate, Projection) do not emit a PosList with references_single_chunk, we can assume that the
      // PosList has no NULL values. However, once we have a `has_null_values` flag in a smarter PosList, we should
      // use it here.

      auto referenced_segment = referenced_table->get_chunk(begin_it->chunk_id)->get_segment(referenced_column_id);

      bool functor_was_called = false;

      if constexpr (erase_reference_segment_type == EraseReferencedSegmentType::No) {
        resolve_segment_type<T>(*referenced_segment, [&](const auto& typed_segment) {
          using SegmentType = std::decay_t<decltype(typed_segment)>;

          // This is ugly, but it allows us to define segment types that we are not interested in and save a lot of
          // compile time during development. While new segment types should be added here,
#ifdef HYRISE_ERASE_DICTIONARY
          if constexpr (std::is_same_v<SegmentType, DictionarySegment<T>>) return;
#endif

#ifdef HYRISE_ERASE_RUNLENGTH
          if constexpr (std::is_same_v<SegmentType, RunLengthSegment<T>>) return;
#endif

#ifdef HYRISE_ERASE_FIXEDSTRINGDICTIONARY
          if constexpr (std::is_same_v<SegmentType, FixedStringDictionarySegment<T>>) return;
#endif

#ifdef HYRISE_ERASE_FRAMEOFREFERENCE
          if constexpr (std::is_same_v<T, int32_t>) {
            if constexpr (std::is_same_v<SegmentType, FrameOfReferenceSegment<T>>) return;
          }
#endif

          // Always erase LZ4Segment accessors
          if constexpr (std::is_same_v<SegmentType, LZ4Segment<T>>) return;

          if constexpr (!std::is_same_v<SegmentType, ReferenceSegment>) {
            const auto segment_iterable = create_iterable_from_segment<T>(typed_segment);

            segment_iterable.with_iterators(pos_list, functor);

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

      if (functor_was_called) return;

      // The functor was not called yet, because we did not instantiate specialized code for the segment type.

      const auto segment_iterable = create_any_segment_iterable<T>(*referenced_segment);
      segment_iterable.with_iterators(pos_list, functor);
    } else {

      std::cout << "Here we go ..." << std::endl;
      // Gather referenced positions for iterables
      std::vector<std::shared_ptr<PosList>> pos_lists_per_segment(8);
      std::vector<SegmentPosition<T>> segment_positions;
      segment_positions.reserve(pos_list->size());

      auto max_chunk_id = ChunkID{0};
      for (const auto& position : *pos_list) {
        const auto chunk_id = position.chunk_id;
        if (chunk_id == INVALID_CHUNK_ID) continue;

        max_chunk_id = std::max(chunk_id, max_chunk_id);
        if (static_cast<size_t>(chunk_id) >= pos_lists_per_segment.size()) {
          std::cout << "we are growing from " << pos_lists_per_segment.capacity() << " to " << chunk_id * 2 << std::endl;
          pos_lists_per_segment.resize(chunk_id * 2);  // grow fast, don't care about a little bit of oversizing here.
        }

        if (!pos_lists_per_segment[chunk_id]) {
          pos_lists_per_segment[chunk_id] = std::make_shared<PosList>();
          pos_lists_per_segment[chunk_id]->reserve(64);
        }
        pos_lists_per_segment[chunk_id]->emplace_back(position);
      }

      std::vector<std::vector<SegmentPosition<T>>> segment_positions_per_segment{max_chunk_id + 1};
      for (auto chunk_id = ChunkID{0}; chunk_id < pos_lists_per_segment.size() - 1; ++chunk_id) {
        const auto& iterable_pos_list = pos_lists_per_segment[chunk_id];
        if (!iterable_pos_list) {
          continue;
        }
        iterable_pos_list->guarantee_single_chunk();

        auto& single_chunk_segment_positions = segment_positions_per_segment[chunk_id];
        single_chunk_segment_positions.reserve(iterable_pos_list->size());

        const auto SEGMENT_TEST = referenced_table->get_chunk(chunk_id)->get_segment(referenced_column_id);
        resolve_segment_type<T>(*SEGMENT_TEST, [&](const auto& typed_segment) {
          const auto iterable = create_iterable_from_segment<T>(typed_segment);
          iterable.with_iterators(iterable_pos_list, [&](auto it, const auto end) {
            while (it != end) {
              single_chunk_segment_positions.emplace_back(SegmentPosition<T>{it->value(), it->is_null(), it->chunk_offset()});
              ++it;
            }
          });
        });
      }

      std::vector<size_t> segment_position_list_offsets(max_chunk_id + 1, 0ul);
      for (const auto& position : *pos_list) {
        std::cout << position << std::endl;
        const auto chunk_id = position.chunk_id;
        if (chunk_id == INVALID_CHUNK_ID) {
          segment_positions.emplace_back(SegmentPosition<T>{T{}, true, position.chunk_offset});
          continue;
        }

        const auto& segment_position = segment_positions_per_segment[chunk_id][segment_position_list_offsets[chunk_id]];
        segment_positions.emplace_back(SegmentPosition<T>{segment_position.value(), segment_position.is_null(), segment_position.chunk_offset()});

        ++segment_position_list_offsets[chunk_id];
      }

      auto begin = SegmentPositionVectorIterator{&segment_positions, ChunkOffset{0}};
      auto end = SegmentPositionVectorIterator{&segment_positions, static_cast<ChunkOffset>(pos_list->size())};

      functor(begin, end);
    }
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const ReferenceSegment& _segment;

 private:
  // The iterator for cases where we iterate over a single referenced chunk
  template <typename Accessor>
  class SingleChunkIterator : public BaseSegmentIterator<SingleChunkIterator<Accessor>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ReferenceSegmentIterable<T, erase_reference_segment_type>;
    using PosListIterator = PosList::const_iterator;

   public:
    explicit SingleChunkIterator(const std::shared_ptr<Accessor>& accessor, const PosListIterator& begin_pos_list_it,
                                 const PosListIterator& pos_list_it)
        : _begin_pos_list_it{begin_pos_list_it}, _pos_list_it{pos_list_it}, _accessor{accessor} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    void decrement() { --_pos_list_it; }

    void advance(std::ptrdiff_t n) { _pos_list_it += n; }

    bool equal(const SingleChunkIterator& other) const { return _pos_list_it == other._pos_list_it; }

    std::ptrdiff_t distance_to(const SingleChunkIterator& other) const { return other._pos_list_it - _pos_list_it; }

    SegmentPosition<T> dereference() const {
      const auto pos_list_offset = static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      if (_pos_list_it->is_null()) return SegmentPosition<T>{T{}, true, pos_list_offset};

      const auto& chunk_offset = _pos_list_it->chunk_offset;

      const auto typed_value = _accessor->access(chunk_offset);

      if (typed_value) {
        return SegmentPosition<T>{std::move(*typed_value), false, pos_list_offset};
      } else {
        return SegmentPosition<T>{T{}, true, pos_list_offset};
      }
    }

   private:
    PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;
    std::shared_ptr<Accessor> _accessor;
  };

  // The iterator for cases where we potentially iterate over multiple referenced chunks
  class MultipleChunkIterator : public BaseSegmentIterator<MultipleChunkIterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ReferenceSegmentIterable<T, erase_reference_segment_type>;
    using PosListIterator = PosList::const_iterator;

   public:
    explicit MultipleChunkIterator(
        const std::shared_ptr<const Table>& referenced_table, const ColumnID referenced_column_id,
        const std::shared_ptr<std::vector<std::shared_ptr<AbstractSegmentAccessor<T>>>>& accessors,
        const PosListIterator& begin_pos_list_it, const PosListIterator& pos_list_it)
        : _referenced_table{referenced_table},
          _referenced_column_id{referenced_column_id},
          _begin_pos_list_it{begin_pos_list_it},
          _pos_list_it{pos_list_it},
          _accessors{accessors} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    void decrement() { --_pos_list_it; }

    void advance(std::ptrdiff_t n) { _pos_list_it += n; }

    bool equal(const MultipleChunkIterator& other) const { return _pos_list_it == other._pos_list_it; }

    std::ptrdiff_t distance_to(const MultipleChunkIterator& other) const { return other._pos_list_it - _pos_list_it; }

    // TODO(anyone): benchmark if using two maps instead doing the dynamic cast every time really is faster.
    SegmentPosition<T> dereference() const {
      const auto pos_list_offset = static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      if (_pos_list_it->is_null()) return SegmentPosition<T>{T{}, true, pos_list_offset};

      const auto chunk_id = _pos_list_it->chunk_id;
      const auto& chunk_offset = _pos_list_it->chunk_offset;

      if (!(*_accessors)[chunk_id]) {
        _create_accessor(chunk_id);
      }
      const auto typed_value = (*_accessors)[chunk_id]->access(chunk_offset);

      if (typed_value) {
        return SegmentPosition<T>{std::move(*typed_value), false, pos_list_offset};
      } else {
        return SegmentPosition<T>{T{}, true, pos_list_offset};
      }
    }

    void _create_accessor(const ChunkID chunk_id) const {
      auto segment = _referenced_table->get_chunk(chunk_id)->get_segment(_referenced_column_id);
      auto accessor = std::move(create_segment_accessor<T>(segment));
      (*_accessors)[chunk_id] = std::move(accessor);
    }

   private:
    std::shared_ptr<const Table> _referenced_table;
    ColumnID _referenced_column_id;

    PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;

    // PointAccessIterators share vector with one Accessor per Chunk
    std::shared_ptr<std::vector<std::shared_ptr<AbstractSegmentAccessor<T>>>> _accessors;
  };

  // The SegmentPositionVectorIterator is basically a wrapper around the std::vector of SegmentPositions. We cannot
  // directly use the vector's iterators as they lack type definitions (e.g., ::ValueType).
  class SegmentPositionVectorIterator : public BaseSegmentIterator<SegmentPositionVectorIterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    // using IterableType = ReferenceSegmentIterable<T, erase_reference_segment_type>;
    using PosListIterator = PosList::const_iterator;

   public:
    explicit SegmentPositionVectorIterator(std::vector<SegmentPosition<T>>* segment_positions, const ChunkOffset chunk_offset)
        : _segment_positions{segment_positions},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }

    void decrement() { --_chunk_offset; }

    void advance(std::ptrdiff_t n) { _chunk_offset += n; }

    bool equal(const SegmentPositionVectorIterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const SegmentPositionVectorIterator& other) const {
      return static_cast<std::ptrdiff_t>(other._chunk_offset) - _chunk_offset;
    }

    SegmentPosition<T> dereference() const {
      return (*_segment_positions)[_chunk_offset];
    }

   private:
    const std::vector<SegmentPosition<T>>* _segment_positions;

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
