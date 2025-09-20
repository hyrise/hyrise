#pragma once

#include <concepts>
#include <ranges>

#include <boost/sort/pdqsort/pdqsort.hpp>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"

namespace hyrise::ips4o {

// Divide left by right and round up to the next larger integer.
auto div_ceil(const auto left, const auto right) {
  return (left + right - 1) / right;
}

// Returns the next multiple. If value is already a multiple, then return value.
auto next_multiple(const auto value, const auto multiple) {
  return div_ceil(value, multiple) * multiple;
}

template <typename Range, typename Value>
concept SortRange = std::ranges::range<Range> && std::ranges::random_access_range<Range> &&
                    std::same_as<std::ranges::range_value_t<Range>, Value>;

// Result of the classification of a stripe.
template <typename T>
struct StripeClassificationResult {
  StripeClassificationResult() = default;

  StripeClassificationResult(size_t num_buckets, size_t block_size)
      : buckets(num_buckets, std::vector<T>()), bucket_sizes(num_buckets, 0) {
    for (auto bucket_index = size_t{0}; bucket_index < num_buckets; ++bucket_index) {
      buckets[bucket_index].reserve(block_size);
    }
  }

  std::vector<std::vector<T>> buckets;
  std::vector<std::size_t> bucket_sizes;
  // Number of blocks written back to the stripe.
  uint64_t num_blocks_written = 0;
};

/**
 * A pair of read and write iterators. This iterator pair can modified with atomic read and write operations.
 */
template <typename T, typename It>
class AtomicBlockIteratorPair {
 public:
  // Used for array initialization. Use init to set the initial values.
  AtomicBlockIteratorPair() = default;

  void init(It begin, int64_t write, int64_t read, size_t block_size) {
    DebugAssert(sizeof(size_t) <= 8, "expects at most 64bit for size types");
    DebugAssert(write % block_size == 0, "Write pointer is not block aligned");
    DebugAssert(read % block_size == 0, "Read pointer is not block aligned");
    _block_size = block_size;
    _begin = begin;
    _offset_pair.store(_combine(write, read), std::memory_order_relaxed);
    _pending_reads.store(0, std::memory_order_relaxed);
  }

  /// Decrement read half by one block and return resulting iterator pair. This will also increment the number of
  /// pending reads. Use complete_read to decrement the read.
  std::pair<std::ranges::subrange<It>, std::ranges::subrange<It>> read() {
    _pending_reads.fetch_add(1, std::memory_order_relaxed);
    while (true) {
      auto initial_offset_pair = _offset_pair.load(std::memory_order_relaxed);
      auto [write_offset, read_offset] = _split_offsets(initial_offset_pair);
      const auto new_read_offset = read_offset - _block_size;
      const auto combined = _combine(write_offset, new_read_offset);
      if (_offset_pair.compare_exchange_weak(initial_offset_pair, combined, std::memory_order_acquire,
                                             std::memory_order_relaxed)) {
        const auto write_iter = std::next(_begin, write_offset);
        const auto write_block = std::ranges::subrange(write_iter, std::next(write_iter, _block_size));
        const auto read_iter = std::next(_begin, read_offset);
        const auto read_block = std::ranges::subrange(read_iter, std::next(read_iter, _block_size));
        return {write_block, read_block};
      }
    }
  }

  /// Increment write offset by one block and return resulting read and write iterator pair.
  std::pair<std::ranges::subrange<It>, std::ranges::subrange<It>> write() {
    while (true) {
      auto initial_offset_pair = _offset_pair.load(std::memory_order_relaxed);
      auto [write_offset, read_offset] = _split_offsets(initial_offset_pair);
      const auto new_write_offset = write_offset + _block_size;
      const auto combined = _combine(new_write_offset, read_offset);
      if (_offset_pair.compare_exchange_weak(initial_offset_pair, combined, std::memory_order_relaxed,
                                             std::memory_order_relaxed)) {
        const auto write_iter = std::next(_begin, write_offset);
        const auto write_block = std::ranges::subrange(write_iter, std::next(write_iter, _block_size));
        const auto read_iter = std::next(_begin, read_offset);
        const auto read_block = std::ranges::subrange(read_iter, std::next(read_iter, _block_size));
        return {write_block, read_block};
      }
    }
  }

  /// Distance of write pointer to start of the sort range.
  int64_t distance_to_start(SortRange<T> auto& range) {
    auto [write_offset, read_offset] = _split_offsets(_offset_pair.load(std::memory_order_relaxed));
    return std::distance(range.begin(), _begin) + write_offset;
  }

  /// Checks if some reads are pending.
  bool pending_reads() {
    return _pending_reads.load(std::memory_order_relaxed) != 0;
  }

  /// Decrement the number of pending reads.
  void complete_read() {
    const auto pending_reads = _pending_reads.fetch_sub(1, std::memory_order_relaxed);
    if constexpr (HYRISE_DEBUG) {
      Assert(pending_reads >= 1, "Pending reads shoul be at least one");
    }
  }

 private:
  // Read the atomic value (relaxed) and split the write and read offset (in order).
  static std::pair<int64_t, int64_t> _split_offsets(boost::uint128_type offset_pairs) {
    const auto read_offset = static_cast<int64_t>(offset_pairs);
    const auto write_offset = static_cast<int64_t>(offset_pairs >> uint64_t{64});
    return {write_offset, read_offset};
  }

  // Combine two offset to a single 128-bit unsigned integer.
  static boost::uint128_type _combine(int64_t write_offset, int64_t read_offset) {
    const auto write_64 = static_cast<uint64_t>(write_offset);
    const auto read_64 = static_cast<uint64_t>(read_offset);
    const auto write_128 = static_cast<boost::uint128_type>(write_64);
    const auto read_128 = static_cast<boost::uint128_type>(read_64);
    return (write_128 << uint32_t{64}) | read_128;
  }

  It _begin = It();
  // Uses 128bit value to encode both read and write offset. The upper bits of the value will be the write and the
  // lower the read offset.
  std::atomic<boost::uint128_type> _offset_pair;
  std::atomic<uint32_t> _pending_reads;
  size_t _block_size = 0;
};

// Split the range at the specified index.
auto split_range_n(std::ranges::range auto& range, size_t index) {
  DebugAssert(index <= std::ranges::size(range), "Split index out of range");
  auto mid = std::next(range.begin(), index);
  return std::pair(std::ranges::subrange(range.begin(), mid), std::ranges::subrange(mid, range.end()));
}

// Removes the first index many elements.
auto cut_range_n(std::ranges::range auto& range, size_t index) {
  DebugAssert(index <= std::ranges::size(range), "Split index out of range");
  auto mid = std::next(range.begin(), index);
  return std::ranges::subrange(mid, range.end());
}

// Return the index of the bucket an element belongs to.
template <typename T>
size_t classify_value(const T& value, const SortRange<T> auto& classifiers,
                      const std::predicate<const T&, const T&> auto& comp) {
  const auto it = std::ranges::lower_bound(classifiers, value, comp);  // NOLINT
  const auto result = std::distance(classifiers.begin(), it);
  return result;
}

// Select the classifiers for the sample sort. At the moment this selects first num classifiers many keys.
template <typename T>
std::vector<T> select_classifiers(const SortRange<T> auto& sort_range, size_t num_classifiers,
                                  size_t samples_per_classifier, const std::predicate<const T&, const T&> auto& comp) {
  const auto size = std::ranges::size(sort_range);
  const auto num_samples = num_classifiers * samples_per_classifier;
  const auto elements_per_sample = size / num_samples;
  const auto offset = elements_per_sample / 2;

  auto samples = std::vector<T>(num_samples);
  for (auto sample = size_t{0}; sample < num_samples; ++sample) {
    const auto index = (sample * elements_per_sample) + offset;
    DebugAssert(index < size, "Index out of range");
    samples[sample] = *std::next(sort_range.begin(), index);
  }
  boost::sort::pdqsort(samples.begin(), samples.end(), comp);

  const auto samples_offset = samples_per_classifier / 2;
  auto classifiers = std::vector<T>(num_classifiers);
  for (auto classifier = size_t{0}; classifier < num_classifiers; ++classifier) {
    const auto sample = (classifier * samples_per_classifier) + samples_offset;
    DebugAssert(sample < num_samples, "Index out of range");
    classifiers[classifier] = samples[sample];
  }

  return classifiers;
}

/*
 * Classify all elements of stripe_range into buckets. Each bucket holds at most block size many elements. A full
 * bucket is written back to the start of the stripe range.
 */
template <typename T>
StripeClassificationResult<T> classify_stripe(SortRange<T> auto& stripe_range, const SortRange<T> auto& classifiers,
                                              const size_t block_size, const std::predicate<T, T> auto& comp) {
  const auto num_buckets = std::ranges::size(classifiers) + 1;
  DebugAssert(num_buckets > 0, "At least one bucket is required");
  DebugAssert(block_size > 0, "Invalid bock size");
  auto result = StripeClassificationResult<T>(num_buckets, block_size);
  auto write_back_begin = std::ranges::begin(stripe_range);
  for (const auto& key : stripe_range) {
    const auto bucket_index = classify_value<T>(key, classifiers, comp);
    DebugAssert(bucket_index < num_buckets, "Bucket index out of range");
    result.buckets[bucket_index].push_back(key);
    ++result.bucket_sizes[bucket_index];
    if (result.buckets[bucket_index].size() == block_size) {
      std::ranges::move(result.buckets[bucket_index], write_back_begin);
      write_back_begin = std::next(write_back_begin, block_size);
      result.buckets[bucket_index].clear();
      ++result.num_blocks_written;
    }
  }
  return result;
}

/**
 * Copy the values of the provide input range to the first and second output range. The first range is filled before
 * the second range and both are updated to only range above non-written values.
 */
void write_to_ranges(std::ranges::range auto& input, std::ranges::range auto& first, std::ranges::range auto& second) {
  const auto input_size = std::ranges::size(input);
  const auto first_size = std::ranges::size(first);
  DebugAssert(input_size <= first_size + std::ranges::size(second),
              "Cannot copy more values than first and second can hold");

  const auto copy_to_first = std::min(input_size, first_size);
  const auto [input_first, input_second] = split_range_n(input, copy_to_first);
  std::ranges::copy(input_first, first.begin());
  first = cut_range_n(first, copy_to_first);

  const auto copy_to_second = input_size - copy_to_first;
  DebugAssert(std::ranges::size(input_second) == copy_to_second, "Unexpected second input size");
  if (copy_to_second > 0) {
    std::ranges::copy(input_second, second.begin());
    second = cut_range_n(second, copy_to_second);
  }
}

/**
 * Move classified blocks into the correct bucket.
 */
template <typename T>
void permute_blocks(const SortRange<T> auto& classifiers, const size_t stripe, auto& block_iterators,
                    const std::predicate<const T&, const T&> auto& comp, size_t num_buckets, size_t block_size,
                    std::vector<T>& overflow_bucket, const auto overflow_bucket_begin) {
  auto target_buffer = std::vector<T>(block_size);
  auto swap_buffer = std::vector<T>(block_size);

  // Cycle through each bucket once.
  for (auto counter = size_t{0}, bucket = stripe % num_buckets; counter < num_buckets;
       ++counter, bucket = (bucket + 1) % num_buckets) {
    while (true) {
      auto [write_block, read_block] = block_iterators[bucket].read();
      if (std::distance(write_block.begin(), read_block.begin()) < 0) {
        block_iterators[bucket].complete_read();
        // Read iterator is before writer iterator: It follows, that no new blocks are left to read. We continue read
        // with the next bucket.
        break;
      }
      std::ranges::move(read_block, target_buffer.begin());
      block_iterators[bucket].complete_read();

      auto target_bucket = classify_value<T>(target_buffer[0], classifiers, comp);
      while (true) {
        auto [write_block, read_block] = block_iterators[target_bucket].write();
        if (std::distance(write_block.begin(), read_block.begin()) < 0) {
          /// Wait until all reads are complete.
          while (block_iterators[target_bucket].pending_reads()) {}

          if (write_block.begin() == overflow_bucket_begin) {
            // Special Case: Let assume we have the following array to sort: [a a b c c]. In addition, we assume that
            // the bucket size is 2. A array with delimiter may look like [a a|b _|c c], but this is array is one
            // element longer than the original array. Because of that we provide an overflow bucket, which can be
            // written to in this case.
            overflow_bucket.resize(block_size);
            std::ranges::move(target_buffer, overflow_bucket.begin());
          } else {
            std::ranges::move(target_buffer, write_block.begin());
          }
          break;
        }

        auto swap_bucket = classify_value<T>(*write_block.begin(), classifiers, comp);
        if (swap_bucket == target_bucket) {
          // Write block is already in the correct block. Skip this block and write to the next one.
          continue;
        }

        std::ranges::move(write_block, swap_buffer.begin());
        std::ranges::move(target_buffer, write_block.begin());
        std::swap(target_buffer, swap_buffer);
        target_bucket = swap_bucket;
      }
    }
  }
}

/**
 * Fill the gaps inside a bucket so that the first blocks are classified and the last blocks are empty. Only buckets crossing chunks boundaries need to be fixed.
 */
template <typename T>
void prepare_block_permutations(SortRange<T> auto& sort_range, size_t stripe_index,
                                const std::ranges::range auto& stripe_ranges,
                                const std::ranges::range auto& stripe_results,
                                const std::ranges::range auto& bucket_delimiter, const size_t block_size) {
  const auto stripe_result = std::next(stripe_results.begin(), stripe_index);
  const auto stripe_range_iter = std::next(stripe_ranges.begin(), stripe_index);
  const auto stripe_begin = stripe_range_iter->begin();
  const auto stripe_begin_index = std::distance(sort_range.begin(), stripe_begin);
  const auto stripe_end = stripe_range_iter->end();
  const auto stripe_end_index = std::distance(sort_range.begin(), stripe_end);

  // Find the first bucket ending after the end of this stripe.
  const auto last_bucket_iter = std::ranges::lower_bound(  // NOLINT
      bucket_delimiter, stripe_end_index, [](const int64_t stripe_end, const auto bucket_end) {
        return stripe_end < static_cast<int64_t>(bucket_end);
      });
  const auto last_bucket_index = std::distance(bucket_delimiter.begin(), last_bucket_iter);
  const auto last_bucket_begin =
      (last_bucket_index != 0) ? static_cast<int64_t>(bucket_delimiter[last_bucket_index - 1]) : int64_t{0};
  const auto last_bucket_end = static_cast<int64_t>(bucket_delimiter[last_bucket_index]);
  // Find the stripe the last bucket starts in.
  const auto last_bucket_first_stripe = std::ranges::lower_bound(  // NOLINT
      stripe_ranges, last_bucket_begin, std::less<int64_t>(), [&](const auto& stripe_range) {
        return std::distance(sort_range.begin(), stripe_range.end());
      });
  const auto last_bucket_first_stripe_index = std::distance(stripe_ranges.begin(), last_bucket_first_stripe);

  // Check if the bucket starts in the next stripe. This can happen if the bucket delimiter aligns with the stripe end.
  if (static_cast<int64_t>(stripe_index) < last_bucket_first_stripe_index) {
    return;
  }

  auto num_already_moved_blocks = int64_t{0};
  auto stripe_result_iter = std::next(stripe_results.begin(), last_bucket_first_stripe_index);
  for (auto stripe_iter = last_bucket_first_stripe; stripe_iter != stripe_range_iter;
       ++stripe_iter, ++stripe_result_iter) {
    const auto stripe_begin = std::distance(sort_range.begin(), stripe_iter->begin());
    const auto stripe_end = std::distance(sort_range.begin(), stripe_iter->end());
    const auto written_end = static_cast<int64_t>(stripe_begin + (stripe_result_iter->num_blocks_written * block_size));
    const auto empty_begin = std::max(written_end, last_bucket_begin);
    const auto num_empty_blocks = (stripe_end - empty_begin) / static_cast<int64_t>(block_size);
    DebugAssert(num_empty_blocks >= 0, "Expects a positive number of empty blocks.");

    // At these empty to blocks to the number of already processed blocks, because they are processed by different
    // threads.
    num_already_moved_blocks += num_empty_blocks;
  }

  // Calculate the number of blocks which must be filled by this stripe.
  const auto written_end = static_cast<int64_t>(stripe_begin_index + (stripe_result->num_blocks_written * block_size));
  const auto empty_begin = std::max(written_end, last_bucket_begin);
  DebugAssert(empty_begin <= stripe_end_index, "Empty region ends after");
  const auto num_empty_blocks = static_cast<int64_t>((stripe_end_index - empty_begin) / block_size);

  auto move_num_blocks = num_empty_blocks;
  const auto stripe_empty_begin_index = stripe_begin_index + (stripe_result->num_blocks_written * block_size);
  const auto bucket_empty_begin = std::max(last_bucket_begin, static_cast<int64_t>(stripe_empty_begin_index));
  auto stripe_empty_begin = std::next(sort_range.begin(), bucket_empty_begin);

  // Find last stripe a bucket is in.
  auto last_stripe = std::ranges::lower_bound(  // NOLINT
      stripe_ranges, last_bucket_end, std::less<int64_t>(), [&](const auto& stripe_range) {
        return std::distance(sort_range.begin(), stripe_range.end());
      });
  if (last_stripe == stripe_ranges.end()) {
    last_stripe = --stripe_ranges.end();
  }
  DebugAssert(last_stripe != stripe_ranges.end(), "Bucket should end in last stripe");
  const auto last_stripe_index = std::distance(stripe_ranges.begin(), last_stripe);
  auto last_stripe_result = std::next(stripe_results.begin(), last_stripe_index);

  // Copy the last blocks in the bucket to the empty blocks of this stripe. Skip num_already_moved_blocks before moving
  // blocks from the end into the empty blocks, because they are already processed by previous stripes.
  for (; last_stripe != stripe_range_iter && move_num_blocks > 0; --last_stripe, --last_stripe_result) {
    const auto begin_index = std::distance(sort_range.begin(), last_stripe->begin());
    DebugAssert(begin_index % block_size == 0, "Wrong alignment");
    const auto stripe_end_index = static_cast<int64_t>(std::distance(sort_range.begin(), last_stripe->end()));
    const auto end_index = std::min(last_bucket_end, stripe_end_index);
    const auto bucket_num_blocks = (end_index - begin_index) / block_size;
    const auto num_blocks_written = last_stripe_result->num_blocks_written;
    auto num_moveable_blocks = static_cast<int64_t>(std::min(bucket_num_blocks, num_blocks_written));

    if (num_already_moved_blocks >= num_moveable_blocks) {
      num_already_moved_blocks -= num_moveable_blocks;
      continue;
    }
    num_moveable_blocks -= num_already_moved_blocks;
    const auto blocks_to_move = std::min(num_moveable_blocks, move_num_blocks);

    auto stripe_full_begin = std::next(last_stripe->begin(), (num_moveable_blocks - 1) * block_size);
    for (auto counter = int64_t{0}; counter < blocks_to_move; ++counter) {
      const auto block_end = std::next(stripe_full_begin, block_size);
      std::move(stripe_full_begin, block_end, stripe_empty_begin);
      stripe_empty_begin = std::next(stripe_empty_begin, block_size);
      stripe_full_begin = std::next(stripe_full_begin, -block_size);
    }
    move_num_blocks -= blocks_to_move;
    num_already_moved_blocks = 0;
  }
}

/**
 * Do a single pass of IPS4o and return an array of bucket delimiter. This past will first classify the input array
 * into blocks. Therefore, the algorithm relies on a sample sort with the provided number of buckets. In a second
 * stage this algorithm will move the block into the correct buckets and finally, it will cleanup the bucket border
 * and add missing elements.
 *
 * @param sort_range             Range of values to sort.
 * @param num_buckets            Number of buckets to classify the elements into. At least to buckets are required.
 * @param samples_per_classifier Number of samples per classifier to select from the array.
 * @param block_size             Number of array elements per block.
 * @param num_stripes            Number of stripes to distribute blocks on. This is equivalent to the maximum amount of
 *								 parallelism.
 * @param comp                   Function for comparing a < b.
 *
 * @return Returns the size of each bucket.
 */
template <typename T>
std::vector<size_t> ips4o_pass(SortRange<T> auto& sort_range, const size_t num_buckets,
                               const size_t samples_per_classifier, const size_t block_size, size_t num_stripes,
                               const std::predicate<const T&, const T&> auto& comp) {
  Assert(num_buckets > 1, "At least two buckets are required");
  Assert(num_stripes > 1, "This function should be called with at least two 2 stripes. Use pdqsort instead");
  Assert(std::ranges::size(sort_range) >= block_size, "Provide at least one block");

  using RangeIterator = decltype(std::ranges::begin(sort_range));

  // The following terms are used in this algorithm:
  // - *block* A number of sequential elements. The sort_range is split up into these blocks.
  // - *stripe* A stripe is a set of sequential blocks. Each stripe is executed in its own task/thread.

  DebugAssert(block_size > 0, "A block must contains at least one element.");
  const auto total_size = std::ranges::size(sort_range);
  const auto num_blocks = div_ceil(total_size, block_size);
  const auto num_classifiers = num_buckets - 1;
  // Calculate the number of blocks assigned to each stripe.
  const auto max_blocks_per_stripe = div_ceil(num_blocks, num_stripes);

  DebugAssert(num_blocks > 0, "At least one block is required.");
  DebugAssert(num_buckets > 0, "At least one bucket is required.");

  // Select the elements for the sample sort.
  const auto classifiers = select_classifiers<T>(sort_range, num_classifiers, samples_per_classifier, comp);

  // Create an array of elements assigned to each stripe. Equally distribute the blocks to each stripe.
  auto num_max_sized_stripes = num_blocks % num_stripes;
  if (num_max_sized_stripes == 0) {
    num_max_sized_stripes = num_stripes;
  }
  auto stripes = std::vector<std::ranges::subrange<RangeIterator>>(num_stripes);
  auto stripe_begin = std::ranges::begin(sort_range);
  for (auto stripe = size_t{0}; stripe < num_stripes - 1; ++stripe) {
    auto stripe_size = (max_blocks_per_stripe - 1) * block_size;
    if (num_max_sized_stripes > 0) {
      stripe_size += block_size;
      --num_max_sized_stripes;
    }
    auto stripe_end = std::next(stripe_begin, stripe_size);
    stripes[stripe] = std::ranges::subrange(stripe_begin, stripe_end);
    stripe_begin = stripe_end;
  }
  stripes.back() = std::ranges::subrange(stripe_begin, std::ranges::end(sort_range));

  // Classify all stripes. After this step each stripe consists of blocks where all elements are classified into the
  // same bucket and followed by empty blocks.
  auto classification_results = std::vector<StripeClassificationResult<T>>(num_stripes);
  auto classification_tasks = std::vector<std::shared_ptr<AbstractTask>>();
  classification_results.reserve(num_stripes);
  for (auto stripe = size_t{0}; stripe < num_stripes; ++stripe) {
    classification_tasks.emplace_back(std::make_shared<JobTask>([&, stripe]() {
      classification_results[stripe] = classify_stripe<T>(stripes[stripe], classifiers, block_size, comp);
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(classification_tasks);

  // Prepare permuting classified blocks into the correct position. This preparation is done in the following:
  // 1. We create the prefix sum of all buckets. As a result, we receive a list of bucket end indices.
  // 2. We align these indices to the next block size.
  // 3. We move all blocks so that the following conditions is met for all buckets: Full blocks are followed by empty
  //    blocks (i.e., first we have the classified blocks followed by non written blocks).

  // Calculate prefix sum of all buckets and align the to the next block.
  auto aggregated_bucket_sizes = std::vector(num_buckets, size_t{0});
  auto aligned_bucket_sizes = std::vector(num_buckets, size_t{0});
  auto buckets_total_size = size_t{0};
  for (auto bucket_index = size_t{0}; bucket_index < num_buckets; ++bucket_index) {
    for (const auto& classification : classification_results) {
      buckets_total_size += classification.bucket_sizes[bucket_index];
    }
    aggregated_bucket_sizes[bucket_index] = buckets_total_size;
    const auto next_bucket_size = ((buckets_total_size + block_size - 1) / block_size) * block_size;
    aligned_bucket_sizes[bucket_index] = next_bucket_size;
  }

  auto prepare_tasks = std::vector<std::shared_ptr<AbstractTask>>();
  prepare_tasks.reserve(num_blocks);
  for (auto stripe = size_t{0}; stripe < num_stripes; ++stripe) {
    prepare_tasks.emplace_back(std::make_shared<JobTask>([&, stripe]() {
      prepare_block_permutations<T>(sort_range, stripe, stripes, classification_results, aligned_bucket_sizes,
                                    block_size);
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(prepare_tasks);

  auto written_blocks_per_stripe = std::vector<size_t>(num_stripes);
  auto empty_blocks_per_stripe = std::vector<size_t>(num_stripes);
  for (auto stripe = size_t{0}; stripe < num_stripes; ++stripe) {
    const auto begin_index = std::distance(sort_range.begin(), stripes[stripe].begin());
    const auto end_index = std::distance(sort_range.begin(), stripes[stripe].end());
    const auto num_blocks = (end_index - begin_index) / block_size;
    const auto written_blocks = classification_results[stripe].num_blocks_written;
    empty_blocks_per_stripe[stripe] = num_blocks - written_blocks;
    written_blocks_per_stripe[stripe] = written_blocks;
  }

  // The write pointer is initialized to the first block of each bucket and the read pointer is initialized to the last
  // non-empty block of each bucket. Because each bucket, contains the classified elements first, all elements between
  // the write and read pointer (inclusive) are classified.
  auto bucket_iterators = std::vector<AtomicBlockIteratorPair<T, RangeIterator>>(num_buckets);
  auto bucket_begin = sort_range.begin();
  for (auto bucket = size_t{0}, stripe = size_t{0}; bucket < num_buckets; ++bucket) {
    const auto delimiter_begin = std::distance(sort_range.begin(), bucket_begin);
    DebugAssert(delimiter_begin % block_size == 0, "Delimiter is misaligned");
    const auto delimiter_end = static_cast<int64_t>(aligned_bucket_sizes[bucket]);
    DebugAssert(delimiter_end % block_size == 0, "Delimiter is misaligned");

    // Sum the number of blocks written to this bucket.
    auto num_blocks = int64_t{0};
    while (stripe < num_stripes) {
      const auto stripe_begin = std::distance(sort_range.begin(), stripes[stripe].begin());
      DebugAssert(stripe_begin % block_size == 0, "Stripe begin is misaligned");
      const auto stripe_end = std::distance(sort_range.begin(), stripes[stripe].end());
      const auto stripe_written_end =
          static_cast<int64_t>(stripe_begin + (block_size * classification_results[stripe].num_blocks_written));
      DebugAssert(stripe_written_end % block_size == 0, "Stripe end is misaligned");

      const auto written_begin = std::max(stripe_begin, delimiter_begin);
      // The delimiter may start after the last block is written. To avoid negative values we set the minimal value to
      // the delimiter_begin.
      const auto written_end =
          std::max(std::min(stripe_written_end, delimiter_end), static_cast<int64_t>(delimiter_begin));

      const auto num_written = (written_end - written_begin);
      DebugAssert(num_written % block_size == 0, "Fence");
      num_blocks += num_written / block_size;

      if (delimiter_end <= stripe_end) {
        break;  // Last stripe of this bucket.
      }
      ++stripe;
    }

    // Set the offset to the first value of the last block.
    const auto read_pointer_offset = (num_blocks - 1) * static_cast<int64_t>(block_size);

    bucket_iterators[bucket].init(bucket_begin, 0, read_pointer_offset, block_size);
    bucket_begin = std::next(sort_range.begin(), delimiter_end);
  }

  const auto overflow_bucket_offset = (std::ranges::size(sort_range) / block_size) * block_size;
  const auto overflow_bucket_begin = std::next(sort_range.begin(), overflow_bucket_offset);
  auto overflow_bucket = std::vector<T>();

  // Move blocks into the correct bucket (based on the aligned bucket delimiter).
  auto permute_blocks_tasks = std::vector<std::shared_ptr<AbstractTask>>();
  permute_blocks_tasks.reserve(num_stripes);
  for (auto stripe = size_t{0}; stripe < num_stripes; ++stripe) {
    permute_blocks_tasks.emplace_back(std::make_shared<JobTask>([&, stripe]() {
      permute_blocks(classifiers, stripe, bucket_iterators, comp, num_buckets, block_size, overflow_bucket,
                     overflow_bucket_begin);
    }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(permute_blocks_tasks);

  // Until this point all buckets are aligned to the block_size and there still some classified elements in the stripe
  // local buffer left. Because of that, we know move elements overflowing the bucket to the start of that bucket and
  // copy the values from the stripe classification result to empty elements at the buffer begin/end.

  // Create a range of empty elements at the start of each bucket. This cannot be done in parallel.
  auto bucket_empty_start = std::vector<std::ranges::subrange<RangeIterator>>(num_buckets);
  for (auto bucket = size_t{0}; bucket < num_buckets; ++bucket) {
    const auto bucket_start_index = (bucket == 0) ? 0 : aggregated_bucket_sizes[bucket - 1];
    const auto bucket_block_start = (bucket == 0) ? 0 : aligned_bucket_sizes[bucket - 1];
    DebugAssert(bucket_block_start - bucket_start_index <= std::ranges::size(sort_range), "Size overflow");

    auto bucket_start_begin = std::next(sort_range.begin(), bucket_start_index);
    const auto bucket_start_end = std::next(sort_range.begin(), bucket_block_start);
    bucket_empty_start[bucket] = std::ranges::subrange(bucket_start_begin, bucket_start_end);
  }

  // Create a range of empty elements at the end of each bucket.
  auto bucket_empty_tail = std::vector<std::ranges::subrange<RangeIterator>>(num_buckets);
  for (auto bucket = size_t{0}; bucket < num_buckets; ++bucket) {
    const auto bucket_end_index = static_cast<int64_t>(aggregated_bucket_sizes[bucket]);
    const auto bucket_block_begin = (bucket > 0) ? static_cast<size_t>(aligned_bucket_sizes[bucket - 1]) : 0;
    auto bucket_written_end_index = bucket_iterators[bucket].distance_to_start(sort_range);

    if (bucket + 1 == num_buckets && !overflow_bucket.empty()) {
      bucket_written_end_index -= block_size;
    }

    const auto bucket_written_end = std::next(sort_range.begin(), bucket_written_end_index);
    if (total_size <= bucket_block_begin) {
      // Last bucket is empty.
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_written_end);
    } else if (bucket_written_end_index > bucket_end_index) {
      DebugAssert(bucket + 1 != num_buckets, "This case should not happen for the last bucket");
      // Bucket overflows actual size. Because of that we move the overflowing elements to the start of that bucket.
      DebugAssert(bucket_written_end_index <= static_cast<int64_t>(aligned_bucket_sizes[bucket]),
                  "More blocks written than bucket should contains");
      DebugAssert(bucket_written_end_index - bucket_end_index <= std::ranges::ssize(bucket_empty_start[bucket]),
                  "Unexpected number of bucket elements");
      const auto overflow_begin = std::next(sort_range.begin(), bucket_end_index);
      const auto overflow_end = std::next(sort_range.begin(), bucket_written_end_index);
      DebugAssert(bucket_written_end_index <= static_cast<int64_t>(total_size), "Overflow stripe size");
      std::move(overflow_begin, overflow_end, bucket_empty_start[bucket].begin());
      bucket_empty_start[bucket] = cut_range_n(bucket_empty_start[bucket], bucket_written_end_index - bucket_end_index);
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_written_end);
    } else {
      const auto bucket_end = std::next(sort_range.begin(), bucket_end_index);
      bucket_empty_tail[bucket] = std::ranges::subrange(bucket_written_end, bucket_end);
    }
  }

  // Copy all partial blocks from the classification result into the output buffer.
  for (auto bucket = size_t{0}; bucket < num_buckets; ++bucket) {
    for (auto stripe = size_t{0}; stripe < num_stripes; ++stripe) {
      auto& bucket_range = classification_results[stripe].buckets[bucket];
      if (std::ranges::empty(bucket_range)) {
        continue;
      }
      write_to_ranges(bucket_range, bucket_empty_start[bucket], bucket_empty_tail[bucket]);
    }
  }
  if (!overflow_bucket.empty()) {
    write_to_ranges(overflow_bucket, bucket_empty_start.back(), bucket_empty_tail.back());
  }

  if constexpr (HYRISE_DEBUG) {
    for (auto bucket = size_t{0}; bucket < num_buckets; ++bucket) {
      DebugAssert(std::ranges::empty(bucket_empty_start[bucket]),
                  std::format("Bucket {} is not fully filled (start)", bucket));
      DebugAssert(std::ranges::empty(bucket_empty_tail[bucket]),
                  std::format("Bucket {} is not fully filled (tail)", bucket));
    }
  }

  return aggregated_bucket_sizes;
}

/**
 * # IPS4o (In-place Parallel Super Scalar Samplesort)
 *
 * This algorithm sorts the specified array of T values in-place. Each of the value is compared using the provided
 * comparator. The comparator's returned value for comparator(a, b), is equivalent to the ordering a < b.
 *
 * ## Naming
 *
 * - `block` A block is fixed number of continuous values aligned to the size of that block. By default this
 *           value is 128.
 * - `stripes` A stripe is continuous sequence of blocks which is processed by a single array. The input array is
 *             divided into multiple stripes.
 *
 * ## Algorithm
 *
 * In its core IPS4o is a parallel samplesort with a few key concepts:
 * 1. Blocks are sorted instead of individual values,
 * 2. Values are classified using samplesort, and
 * 3. Small stripes are processed using insertion sort.
 *
 * Because IPS4o is build arounmd samplesort, multiple rounds are needed to sort the small values. Initially the
 * algorithms starts sorting the hole array into multiple buckets. We call this a single pass of IPS4o. After that, the
 * larger buckets will than sorted again using IPS4o.
 *
 * A single IPS4o pass consits of the followin steps:
 * 1. All values get classified into blocks. We therefore, maintain buckets of block size many elements. If these
 *    buckets are full, the values are written back to the array. The result of this stage is: We know the number of
 *    elements per bucket, that all elements in a block classify to the same value, and we know which blocks are
 *    classified and which are empty (this may happen as some elements remain as left overs in the classification
 *    buckets)
 * 2. Next the classified blocks are moved into the correct places. Therefore, we move the blocks into a swap buffers
 *    and swap them until, we hit an empty block.
 * 3. At last, the bucket borders get cleaned up and all elements sitting in the classification buckets are used to fill
 *    the last elements of each bucket in the array.
 *
 * ## Modifications to IPS4o
 *
 * Normally, IPS4o would assign the buckets to the stripe based on their location in the array. This and the requirement
 * of synchronization between threads make it hard to run with the dynamic scheduling of Hyrise. Therefore, we adjusted
 * two things:
 * 1. Instead of running the tasks fully in separate thread and synchronize them with barriers, we stick to schedule for
 *    each stripe a new task per step.
 * 2. Instead of assigning a fixed number of stripes to each of the larger buckets to divide the workload, we use a
 *    IPS4o pass to divide a range into a smaller one and than sort the small ranges with pdqsort in parallel.
 */
template <typename T>
void sort(SortRange<T> auto range, std::predicate<const T&, const T&> auto comparator, size_t max_parallelism,
          size_t min_blocks_per_stripe, size_t num_classifiers = 31, size_t samples_per_classifier = 16,
          size_t block_size = 128) {
  Assert(num_classifiers > 0, "At least one classifier is required");
  using RangeIterator = decltype(std::ranges::begin(range));

  const auto total_size = std::ranges::size(range);
  const auto total_num_blocks = (total_size + block_size - 1) / block_size;
  if (total_num_blocks < 16 * block_size) {
    // Sort small arrays directly
    boost::sort::pdqsort(std::ranges::begin(range), std::ranges::end(range), comparator);
    return;
  }

  const auto bucket_count = num_classifiers + 1;

  // Do an initial pass of IPS4o.
  const auto num_stripes = std::min(total_num_blocks / min_blocks_per_stripe, max_parallelism);
  auto bucket_lock = std::mutex();
  auto buckets = std::vector<std::ranges::subrange<RangeIterator>>();
  buckets.reserve(bucket_count * bucket_count);
  buckets.emplace_back(std::ranges::begin(range), std::ranges::end(range));

  auto unsplittable_buckets = std::vector<std::ranges::subrange<RangeIterator>>();
  unsplittable_buckets.reserve(bucket_count);

  auto unparsed_buckets = std::ranges::subrange(buckets.begin(), buckets.end());
  const auto small_bucket_max_blocks = div_ceil(total_num_blocks, bucket_count);
  const auto small_bucket_max_size = std::max(small_bucket_max_blocks * block_size, max_parallelism * block_size);
  for (auto round = size_t{0}; round < 4; ++round) {
    auto large_bucket_range = std::ranges::partition(unparsed_buckets, [&](const auto& range) {
      return std::ranges::size(range) <= small_bucket_max_size;
    });
    auto large_buckets = std::vector(large_bucket_range.begin(), large_bucket_range.end());
    buckets.erase(large_bucket_range.begin(), large_bucket_range.end());
    if (large_buckets.size() == 0) {
      break;
    }

    const auto small_offset = buckets.size();

    // Split large buckets into smaller buckets, by rerunning IPS4o
    auto round_tasks = std::vector<std::shared_ptr<AbstractTask>>();
    round_tasks.reserve(large_buckets.size());
    for (auto bucket = size_t{0}; bucket < large_buckets.size(); ++bucket) {
      round_tasks.emplace_back(std::make_shared<JobTask>([&, bucket]() {
        const auto large_range = large_buckets[bucket];
        DebugAssert(std::ranges::size(large_range) >= small_bucket_max_size, "Large bucket");

        const auto delimiter =
            ips4o_pass<T>(large_range, bucket_count, samples_per_classifier, block_size, num_stripes, comparator);

        const auto guard = std::lock_guard(bucket_lock);
        auto bucket_begin_index = size_t{0};
        for (const auto bucket_delimiter : delimiter) {
          const auto begin = std::next(large_range.begin(), bucket_begin_index);
          const auto end = std::next(large_range.begin(), bucket_delimiter);
          const auto bucket_size = bucket_delimiter - bucket_begin_index;
          if (bucket_size == std::ranges::size(large_range)) {
            unsplittable_buckets.emplace_back(begin, end);
          } else if (bucket_size > 0) {
            buckets.emplace_back(begin, end);
          }
          bucket_begin_index = bucket_delimiter;
        }
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(round_tasks);

    const auto small_begin = std::next(buckets.begin(), small_offset);
    unparsed_buckets = std::ranges::subrange(small_begin, buckets.end());
  }

  // Sort buckets by size to first sort large buckets.
  boost::sort::pdqsort(buckets.begin(), buckets.end(), [](const auto& left, const auto& right) {
    return std::ranges::size(left) > std::ranges::size(right);
  });

  auto sort_tasks = std::vector<std::shared_ptr<AbstractTask>>();
  sort_tasks.reserve(unsplittable_buckets.size() + buckets.size());
  for (auto bucket = size_t{0}; bucket < unsplittable_buckets.size(); ++bucket) {
    sort_tasks.emplace_back(std::make_shared<JobTask>([&, bucket]() {
      boost::sort::pdqsort(unsplittable_buckets[bucket].begin(), unsplittable_buckets[bucket].end(), comparator);
    }));
  }
  for (auto bucket = size_t{0}; bucket < buckets.size(); ++bucket) {
    sort_tasks.emplace_back(std::make_shared<JobTask>([&, bucket]() {
      boost::sort::pdqsort(buckets[bucket].begin(), buckets[bucket].end(), comparator);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(sort_tasks);
}

}  // namespace hyrise::ips4o
