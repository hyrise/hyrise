#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <cppcoro/recursive_generator.hpp>

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







#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"












namespace opossum {

template <typename T, EraseReferencedSegmentType erase_reference_segment_type>
class ReferenceSegmentIterable : public SegmentIterable<ReferenceSegmentIterable<T, erase_reference_segment_type>> {
 public:
  using ValueType = T;

  explicit ReferenceSegmentIterable(const ReferenceSegment& segment) : _segment{segment} {}

  cppcoro::recursive_generator<SegmentPosition<T>> generator(const auto& iterable, const std::shared_ptr<RowIDPosList>& pos_list) const {
    iterable.with_iterators(pos_list, [&](auto it, const auto end) {
      while (it != end) {
        co_yield SegmentPosition(it->value(), it->is_null(), it->chunk_offset());
        ++it;
      }
    });
  }

  auto generator2(auto it, const auto end) const {
    while (it != end) {
      std::cout << "# " << it->value();
      co_yield SegmentPosition(it->value(), it->is_null(), it->chunk_offset());
      ++it;
    }
  }

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

      const auto& referenced_segment =
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
      // Gather referenced positions for iterables
      std::vector<std::shared_ptr<RowIDPosList>> pos_lists_per_segment(8);
      std::vector<std::vector<size_t>> write_offsets_per_segment(8);

      // Preallocate segment positions with NULL positions that might stem from previous operators. Such NULL (in
      // constrast to NULL rows in segments) cannot be passed to iterables. Thus, we intialize the vector with NULL
      // positions and later (potentially) overwrite positions with their actual values.
      std::vector<SegmentPosition<T>> segment_positions(position_filter->size(), {T{}, true, ChunkOffset{0}});
      segment_positions.reserve(position_filter->size());

      // TODO: do we expect an impact of resolving the pos list? Unsure.

      auto max_chunk_id = ChunkID{0};
      auto pos_list_position = size_t{0};
      for (auto iter = position_filter->cbegin(); iter != position_filter->cend(); ++iter) {
        const auto& position = *iter;
        const auto chunk_id = position.chunk_id;
        if (chunk_id == INVALID_CHUNK_ID) {
          ++pos_list_position;
          continue;
        }

        max_chunk_id = std::max(chunk_id, max_chunk_id);
        if (static_cast<size_t>(chunk_id) >= pos_lists_per_segment.size()) {
          pos_lists_per_segment.resize(chunk_id * 2);  // grow fast, don't care about a little bit of oversizing here.
          write_offsets_per_segment.resize(chunk_id * 2);
        }

        if (!pos_lists_per_segment[chunk_id]) {
          // Reserve the expected pos list size when positions are uniformly distributed over all chunks, but always
          // reserve at least 16 elements.
          const auto elements_to_reserve = std::max(16ul, position_filter->size() / (max_chunk_id + 1));

          pos_lists_per_segment[chunk_id] = std::make_shared<RowIDPosList>();
          pos_lists_per_segment[chunk_id]->reserve(elements_to_reserve);
          write_offsets_per_segment.reserve(elements_to_reserve);
        }
        pos_lists_per_segment[chunk_id]->emplace_back(position);
        write_offsets_per_segment[chunk_id].emplace_back(pos_list_position);
        ++pos_list_position;
      }

      // std::vector<std::vector<SegmentPosition<T>>> segment_positions_per_segment{max_chunk_id + 1};
      for (auto chunk_id = ChunkID{0}; chunk_id < pos_lists_per_segment.size(); ++chunk_id) {
        const auto& iterable_pos_list = pos_lists_per_segment[chunk_id];
        const auto& write_offsets = write_offsets_per_segment[chunk_id];
        auto current_write_offset = size_t{0};

        if (!iterable_pos_list) {
          continue;
        }
        iterable_pos_list->guarantee_single_chunk();

        // auto& single_chunk_segment_positions = segment_positions_per_segment[chunk_id];
        // single_chunk_segment_positions.reserve(iterable_pos_list->size());

        const auto& base_segment = referenced_table->get_chunk(chunk_id)->get_segment(referenced_column_id);
        resolve_segment_type<T>(*base_segment, [&](const auto& typed_segment) {
          [[maybe_unused]] const auto write_segment_positions = [&](auto it, const auto end) {
            while (it != end) {
              // Cannot insert *it here since it might hold other SegmentPositions types (NonNullSegmentPosition) and
              // we don't want to use a list of AbstractSegmentPositions.
              // single_chunk_segment_positions.emplace_back(it->value(), it->is_null(), it->chunk_offset());
              segment_positions[write_offsets[current_write_offset]] = SegmentPosition(it->value(), it->is_null(), ChunkOffset{static_cast<uint32_t>(current_write_offset)});

              ++it;
              ++current_write_offset;
            }
          };

          if constexpr (erase_reference_segment_type == EraseReferencedSegmentType::No) {
            const auto iterable = create_iterable_from_segment<T>(typed_segment);
            using IterableType = std::decay_t<decltype(iterable)>;
            if constexpr (!std::is_same_v<IterableType, ReferenceSegmentIterable>) {
              for (const auto& el : iterable.template with_generator<T>(iterable_pos_list)) {
                std::cout << "%" << el.value() << std::endl;
              }
              iterable.with_iterators(iterable_pos_list, write_segment_positions);
            } else {
              Fail("Cannot instantiate an interable with a position list.");
            }
          } else {
            const auto iterable = create_any_segment_iterable<T>(typed_segment);
            for (const auto& el : iterable.template with_generator<T>(iterable_pos_list)) {
              std::cout << "%" << el.value() << std::endl;
            }
            iterable.with_iterators(iterable_pos_list, write_segment_positions);
          }
        });
      }



      DebugAssert(segment_positions.size() == position_filter->size(), "Sizes do not match");

      auto begin = SegmentPositionVectorIterator{&segment_positions, ChunkOffset{0}};
      auto end = SegmentPositionVectorIterator{&segment_positions, static_cast<ChunkOffset>(position_filter->size())};

      functor(begin, end);
    }
  }

  cppcoro::recursive_generator<SegmentPosition<T>> _on_with_generator(const std::shared_ptr<const AbstractPosList>& position_filter) const {
    co_yield SegmentPosition(T{}, false, ChunkOffset{0});
    Fail("That path should simply NOT.");
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
      : public BaseSegmentIterator<MultipleChunkIterator<PosListIteratorType>, SegmentPosition<T>> {
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

  // The SegmentPositionVectorIterator is basically a wrapper around an std::vector<SegmentPosition>. We cannot
  // directly use the vector's iterators as they lack type definitions (e.g., ::ValueType).
  class SegmentPositionVectorIterator : public BaseSegmentIterator<SegmentPositionVectorIterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ReferenceSegmentIterable<T, erase_reference_segment_type>;

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
      DebugAssert((*_segment_positions)[_chunk_offset].chunk_offset() == _chunk_offset, "Unexpected chunk offset.");
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
