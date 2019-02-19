#pragma once

#include <boost/blank.hpp>

#include <cstddef>

#include "types.hpp"

namespace opossum {

/**
 * @brief Return type of segment iterators
 *
 * The class documents the interface that can be expected
 * of an object returned by a segment iterator.
 * The actual returned method will however be a sub-class
 * in order to avoid expensive virtual method calls.
 * For this reason, all methods in sub-classes should be
 * declared using `final`.
 */
template <typename T>
class AbstractSegmentPosition {
 public:
  using Type = T;

 public:
  AbstractSegmentPosition() = default;
  AbstractSegmentPosition(const AbstractSegmentPosition&) = default;
  virtual ~AbstractSegmentPosition() = default;

  virtual const T& value() const = 0;
  virtual bool is_null() const = 0;

  /**
   * @brief Returns the chunk offset of the current value.
   *
   * The chunk offset can point either into a reference segment,
   * if returned by a point-access iterator, or into an actual data segment.
   */
  virtual ChunkOffset chunk_offset() const = 0;
};

/**
 * @brief The most generic segment iterator position
 *
 * Used in most segment iterators.
 */
template <typename T>
class SegmentPosition : public AbstractSegmentPosition<T> {
 public:
  static constexpr bool Nullable = true;

  SegmentPosition(const T& value, const bool null_value, const ChunkOffset& chunk_offset)
      : _value{value}, _null_value{null_value}, _chunk_offset{chunk_offset} {}

  const T& value() const final { return _value; }
  bool is_null() const final { return _null_value; }
  ChunkOffset chunk_offset() const final { return _chunk_offset; }

 private:
  // The alignment improves the suitability of the iterator for (auto-)vectorization
  alignas(8) const T _value;
  alignas(8) const bool _null_value;
  alignas(8) const ChunkOffset _chunk_offset;
};

/**
 * @brief Segment iterator position which is never null.
 *
 * Used when an underlying segment (or data structure) cannot be null.
 */
template <typename T>
class NonNullSegmentPosition : public AbstractSegmentPosition<T> {
 public:
  static constexpr bool Nullable = false;

  NonNullSegmentPosition(const T& value, const ChunkOffset& chunk_offset)
      : _value{value}, _chunk_offset{chunk_offset} {}

  const T& value() const final { return _value; }
  bool is_null() const final { return false; }
  ChunkOffset chunk_offset() const final { return _chunk_offset; }

 private:
  // The alignment improves the suitability of the iterator for (auto-)vectorization
  alignas(8) const T _value;
  alignas(8) const ChunkOffset _chunk_offset;
};

/**
 * @brief Segment iterator position without value information
 *
 * Used for data structures that only store if the entry is null or not.
 *
 * @see NullValueVectorIterable
 */
class IsNullSegmentPosition : public AbstractSegmentPosition<boost::blank> {
 public:
  static constexpr bool Nullable = true;

  IsNullSegmentPosition(const bool null_value, const ChunkOffset& chunk_offset)
      : _null_value{null_value}, _chunk_offset{chunk_offset} {}

  const boost::blank& value() const final { return _blank; }
  bool is_null() const final { return _null_value; }
  ChunkOffset chunk_offset() const final { return _chunk_offset; }

 private:
  // The alignment improves the suitability of the iterator for (auto-)vectorization
  static constexpr auto _blank = boost::blank{};
  alignas(8) const bool _null_value;
  alignas(8) const ChunkOffset _chunk_offset;
};

}  // namespace opossum
