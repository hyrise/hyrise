#pragma once

#include <cstddef>

#include <boost/blank.hpp>

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
  AbstractSegmentPosition(AbstractSegmentPosition&&) = default;
  AbstractSegmentPosition& operator=(const AbstractSegmentPosition&) = default;
  AbstractSegmentPosition& operator=(AbstractSegmentPosition&&) = default;
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
class SegmentPosition final : public AbstractSegmentPosition<T> {
 public:
  static constexpr bool Nullable = true;

  SegmentPosition(T value, const bool null_value, const ChunkOffset& chunk_offset) noexcept
      : _value{std::move(value)}, _null_value{null_value}, _chunk_offset{chunk_offset} {}

  const T& value() const noexcept { return _value; }
  bool is_null() const noexcept { return _null_value; }
  ChunkOffset chunk_offset() const noexcept { return _chunk_offset; }

 private:
  // The alignment improves the suitability of the iterator for (auto-)vectorization
  T _value;
  bool _null_value;
  ChunkOffset _chunk_offset;
};

template <>
class SegmentPosition<pmr_string> final : public AbstractSegmentPosition<std::string_view> {
 public:
  static constexpr bool Nullable = true;

  SegmentPosition(std::string_view value, const bool null_value, const ChunkOffset& chunk_offset) noexcept
      : _value{std::move(value)}, _null_value{null_value}, _chunk_offset{chunk_offset} {}

  // TODO if HYRISE_DEBUG copy string and assert equality on value(), same for NonNullSegmentPosition

  const std::string_view& value() const noexcept { return _value; }
  bool is_null() const noexcept { return _null_value; }
  ChunkOffset chunk_offset() const noexcept { return _chunk_offset; }

 private:
  // The alignment improves the suitability of the iterator for (auto-)vectorization
  std::string_view _value;
  bool _null_value;
  ChunkOffset _chunk_offset;
};

/**
 * @brief Segment iterator position which is never null.
 *
 * Used when an underlying segment (or data structure) cannot be null.
 */
template <typename T>
class NonNullSegmentPosition final : public AbstractSegmentPosition<T> {
 public:
  static constexpr bool Nullable = false;

  NonNullSegmentPosition(T value, const ChunkOffset& chunk_offset) noexcept
      : _value{std::move(value)}, _chunk_offset{chunk_offset} {}

  const T& value() const { return _value; }
  bool is_null() const { return false; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  // The alignment improves the suitability of the iterator for (auto-)vectorization
  T _value;
  ChunkOffset _chunk_offset;
};

template <>
class NonNullSegmentPosition<pmr_string> final : public AbstractSegmentPosition<std::string_view> {
 public:
  static constexpr bool Nullable = false;

  NonNullSegmentPosition(std::string_view value, const ChunkOffset& chunk_offset) noexcept
      : _value{value}, _chunk_offset{chunk_offset} {}

  const std::string_view& value() const { return _value; }
  bool is_null() const { return false; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  // The alignment improves the suitability of the iterator for (auto-)vectorization
  std::string_view _value;
  ChunkOffset _chunk_offset;
};

/**
 * @brief Segment iterator position without value information
 *
 * Used for data structures that only store if the entry is null or not.
 *
 * @see NullValueVectorIterable
 */
class IsNullSegmentPosition final : public AbstractSegmentPosition<boost::blank> {
 public:
  static constexpr bool Nullable = true;

  IsNullSegmentPosition(const bool null_value, const ChunkOffset& chunk_offset)
      : _null_value{null_value}, _chunk_offset{chunk_offset} {}

  const boost::blank& value() const { return _blank; }
  bool is_null() const { return _null_value; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  // The alignment improves the suitability of the iterator for (auto-)vectorization
  static constexpr auto _blank = boost::blank{};
  alignas(8) const bool _null_value;
  alignas(8) const ChunkOffset _chunk_offset;
};

}  // namespace opossum
