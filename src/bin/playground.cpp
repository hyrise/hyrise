#include <iostream>
#include <storage/chunk_encoder.hpp>
#include <storage/value_segment.hpp>
#include <storage/create_iterable_from_segment.hpp>

#include "types.hpp"

using namespace opossum;  // NOLINT

// STL

void print_stl_iterator_type(std::input_iterator_tag) {
  std::cout << "stl input" << std::endl;
}

void print_stl_iterator_type(std::forward_iterator_tag) {
  std::cout << "stl forward" << std::endl;
}

void print_stl_iterator_type(std::bidirectional_iterator_tag) {
  std::cout << "stl bidirectional" << std::endl;
}

void print_stl_iterator_type(std::random_access_iterator_tag) {
  std::cout << "stl random" << std::endl;
}

// Boost

void print_boost_iterator_type(boost::incrementable_traversal_tag) {
  std::cout << "boost incrementable" << std::endl;
}

void print_boost_iterator_type(boost::bidirectional_traversal_tag) {
  std::cout << "boost bidirectional" << std::endl;
}

void print_boost_iterator_type(boost::random_access_traversal_tag) {
  std::cout << "boost random" << std::endl;
}


// STL Input access:
class MyIterator : public boost::iterator_facade<MyIterator, int32_t, boost::random_access_traversal_tag, int32_t> {

// STL Random access:
// class MyIterator : public boost::iterator_facade<MyIterator, int32_t, boost::random_access_traversal_tag>,
// class MyIterator : public boost::iterator_facade<MyIterator, int32_t, boost::random_access_traversal_tag, int32_t&> {
// class MyIterator : public boost::iterator_facade<MyIterator, int32_t, boost::random_access_traversal_tag, const int32_t&>,

                   // public JitBaseSegmentIterator {

 public:
  explicit MyIterator(const std::vector<int32_t> vec)
      : vals{vec} {}

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { ++i; }

  void decrement() { --i; }

  void advance(std::ptrdiff_t n) { i += n; }

  bool equal(const MyIterator& other) const { return other.i == i; }

  std::ptrdiff_t distance_to(const MyIterator& other) const {
    return other.i - i;
  }

  int32_t dereference() const { return vals[i]; }

 private:
  size_t i;
  std::vector<int32_t> vals;
};


int main() {
  {
    const std::vector<int32_t> values{0, 1, 2, 3, 4};
    const auto segment = ValueSegment<int32_t>(values);
    const auto iterable = create_iterable_from_segment<int32_t, false>(segment);
    iterable.with_iterators([](auto begin, auto end) {
      using Iterator = decltype(begin);

      auto bound = std::lower_bound(begin, end, 5, [](const auto &segment_position, const auto &search_value) {
        return segment_position.value() < search_value;
      });
      std::cout << std::distance(begin, bound) << std::endl;

      std::cout << std::endl << "# value segment iterator #" << std::endl << std::endl;
      print_stl_iterator_type(typename std::iterator_traits<Iterator>::iterator_category());
      print_boost_iterator_type(typename boost::iterator_traversal<Iterator>::type());

    });
  }

  std::cout << "LZ4" << std::endl;
  {
    const std::vector<int32_t> values{0, 1, 2, 3, 4};
    const auto segment = std::make_shared<ValueSegment<int32_t>>(values);
    const auto encoded_segment = ChunkEncoder::encode_segment(segment, DataType::Int, SegmentEncodingSpec{EncodingType::LZ4});
    const auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<int32_t>>(encoded_segment);
    const auto iterable = create_iterable_from_segment<int32_t, false>(*lz4_segment);
    iterable.with_iterators([](auto begin, auto end) {
      using Iterator = decltype(begin);

      auto bound = std::lower_bound(begin, end, 5, [](const auto &segment_position, const auto &search_value) {
        return segment_position.value() < search_value;
      });
      std::cout << std::distance(begin, bound) << std::endl;

      std::cout << std::endl << "# value segment iterator #" << std::endl << std::endl;
      print_stl_iterator_type(typename std::iterator_traits<Iterator>::iterator_category());
      print_boost_iterator_type(typename boost::iterator_traversal<Iterator>::type());

    });
  }

  std::cout << "LZ4-any" << std::endl;
  {
    const std::vector<int32_t> values{0, 1, 2, 3, 4};
    const auto segment = std::make_shared<ValueSegment<int32_t>>(values);
    const auto encoded_segment = ChunkEncoder::encode_segment(segment, DataType::Int, SegmentEncodingSpec{EncodingType::LZ4});
    const auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<int32_t>>(encoded_segment);
    const auto iterable = create_iterable_from_segment<int32_t, true>(*lz4_segment);
    iterable.with_iterators([](auto begin, auto end) {
      using Iterator = decltype(begin);

      auto bound = std::lower_bound(begin, end, 5, [](const auto &segment_position, const auto &search_value) {
        return segment_position.value() < search_value;
      });
      std::cout << std::distance(begin, bound) << std::endl;

      std::cout << std::endl << "# value segment iterator #" << std::endl << std::endl;
      print_stl_iterator_type(typename std::iterator_traits<Iterator>::iterator_category());
      print_boost_iterator_type(typename boost::iterator_traversal<Iterator>::type());

    });
  }


  {
    const std::vector<int32_t> values{0, 1, 2, 3, 4};
    const auto segment = std::make_shared<ValueSegment<int32_t>>(values);
    const auto encoded_segment = ChunkEncoder::encode_segment(segment, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
    const auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(encoded_segment);
    const auto iterable = create_iterable_from_segment<int32_t, false>(*dict_segment);
    iterable.with_iterators([](auto begin, auto end) {
      using Iterator = decltype(begin);

      auto bound = std::lower_bound(begin, end, 5, [](const auto &segment_position, const auto &search_value) {
        return segment_position.value() < search_value;
      });
      std::cout << std::distance(begin, bound) << std::endl;

      std::cout << std::endl << "# value segment iterator #" << std::endl << std::endl;
      print_stl_iterator_type(typename std::iterator_traits<Iterator>::iterator_category());
      print_boost_iterator_type(typename boost::iterator_traversal<Iterator>::type());

    });
  }


  std::cout << std::endl << "# custom iterator #" << std::endl << std::endl;
  print_stl_iterator_type(typename std::iterator_traits<MyIterator>::iterator_category());
  print_boost_iterator_type(typename boost::iterator_traversal<MyIterator>::type());


  return 0;
}