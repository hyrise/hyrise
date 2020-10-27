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

      if (functor_was_called) return;

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

        auto begin = MultipleChunkIterator<PosListIteratorType>{referenced_table, referenced_column_id, accessors,
                                                                position_begin_it, position_begin_it};
        auto end = MultipleChunkIterator<PosListIteratorType>{referenced_table, referenced_column_id, accessors,
                                                              position_begin_it, position_end_it};

        functor(begin, end);
      });
    }
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const ReferenceSegment& _segment;

 private:
  // The iterator for cases where we iterate over a single referenced chunk
  template <typename Accessor>
  class SingleChunkIterator : public AbstractSegmentIterator<SingleChunkIterator<Accessor>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ReferenceSegmentIterable<T, erase_reference_segment_type>;
    using PosListIteratorType = AbstractPosList::PosListIterator<>;

   public:
    explicit SingleChunkIterator(const std::shared_ptr<Accessor>& accessor,
                                 const PosListIteratorType& begin_pos_list_it, const PosListIteratorType& pos_list_it)
        : _begin_pos_list_it{begin_pos_list_it}, _pos_list_it{pos_list_it}, _accessor{accessor} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    void decrement() { --_pos_list_it; }

    void advance(std::ptrdiff_t n) { _pos_list_it += n; }

    bool equal(const SingleChunkIterator& other) const { return _pos_list_it == other._pos_list_it; }

    std::ptrdiff_t distance_to(const SingleChunkIterator& other) const { return other._pos_list_it - _pos_list_it; }

    SegmentPosition<T> dereference() const {
      const auto pos_list_offset = static_cast<ChunkOffset>(_pos_list_it - _begin_pos_list_it);

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
    PosListIteratorType _begin_pos_list_it;
    PosListIteratorType _pos_list_it;
    std::shared_ptr<Accessor> _accessor;
  };

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
        const PosListIteratorType& begin_pos_list_it, const PosListIteratorType& pos_list_it)
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
      const auto pos_list_offset = static_cast<ChunkOffset>(_pos_list_it - _begin_pos_list_it);

      if (_pos_list_it->is_null()) return SegmentPosition<T>{T{}, true, pos_list_offset};

      const auto chunk_id = _pos_list_it->chunk_id;

      if (!(*_accessors)[chunk_id]) {
        _create_accessor(chunk_id);
      }

      const auto chunk_offset = _pos_list_it->chunk_offset;

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

    PosListIteratorType _begin_pos_list_it;
    PosListIteratorType _pos_list_it;

    // PointAccessIterators share vector with one Accessor per Chunk
    std::shared_ptr<std::vector<std::shared_ptr<AbstractSegmentAccessor<T>>>> _accessors;
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
