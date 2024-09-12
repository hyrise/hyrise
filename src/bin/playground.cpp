  #include <chrono>
  #include <deque>
  #include <iostream>
  #include <queue>

  #include "benchmark_config.hpp"
  #include "hyrise.hpp"
  #include "statistics/statistics_objects/range_filter.hpp"
  #include "storage/dictionary_segment.hpp"
  #include "tpch/tpch_constants.hpp"
  #include "tpch/tpch_table_generator.hpp"
  #include "types.hpp"

  using namespace hyrise;  // NOLINT(build/namespaces)

  constexpr auto MEASUREMENT_COUNT = size_t{5};

  template <typename T>
  std::vector<std::pair<T, T>> queue_based(const pmr_vector<T>& dictionary) {
    constexpr size_t RANGE_COUNT = 64 / sizeof(T) / 2;
    constexpr size_t GAP_COUNT = RANGE_COUNT - 1;

    const auto dictionary_size = dictionary.size();
    auto gap_compare = [](const auto& lhs, const auto& rhs) { return (std::get<0>(lhs) > std::get<0>(rhs)); };

    auto min_heap = std::array<std::tuple<T, T, T>, GAP_COUNT>{};
    for (auto dindex = size_t{0}; dindex < std::min(GAP_COUNT, dictionary_size-1); ++dindex) {
      auto gap_begin = dictionary[dindex];
      auto gap_end = dictionary[dindex + 1];
      auto distance = gap_end - gap_begin;
      min_heap[dindex] = std::make_tuple<T, T, T>(std::move(distance), std::move(gap_begin), std::move(gap_end));
    }

    std::make_heap(min_heap.begin(), min_heap.end(), gap_compare);

    // auto queue = std::priority_queue<std::tuple<T, T, T>, std::vector<std::tuple<T, T, T>>, decltype(gap_compare)>{gap_compare};
    // // We first add the first N slots.
    // for (auto dindex = size_t{0}; dindex < std::min(GAP_COUNT, dictionary_size-1); ++dindex) {
    //   queue.emplace(dictionary[dindex + 1] - dictionary[dindex], dictionary[dindex], dictionary[dindex + 1]);
    // }

    for (auto dindex = GAP_COUNT; dindex < dictionary_size - 1; ++dindex) {
      auto gap_begin = dictionary[dindex];
      auto gap_end = dictionary[dindex + 1];
      auto distance = gap_end - gap_begin;
      const auto& top = min_heap.begin();
      if (std::get<0>(*top) < distance) {
        // std::cout << "prev: "; for (const auto& elem : min_heap) { std::cout << " - " << std::get<0>(elem) << " (" << std::get<1>(elem) << "," << std::get<2>(elem) << ")\t"; }
        std::pop_heap(min_heap.begin(), min_heap.end(), gap_compare);
        // std::cout << "\nI popped" << std::endl;
        // std::cout << "\nnow: "; for (const auto& elem : min_heap) { std::cout << " - " << std::get<0>(elem) << " (" << std::get<1>(elem) << "," << std::get<2>(elem) << ")\t"; } std::cout << "\n";
        min_heap[GAP_COUNT - 1] = std::make_tuple<T, T, T>(std::move(distance), std::move(gap_begin), std::move(gap_end));
        std::push_heap(min_heap.begin(), min_heap.end(), gap_compare);
      }
    }

    // Assert(std::ranges::is_heap(min_heap, gap_compare), "No heap?!?!?");

    // std::cout << "\nBefore sort: "; for (const auto& elem : min_heap) { std::cout << " - " << std::get<0>(elem) << " (" << std::get<1>(elem) << "," << std::get<2>(elem) << ")\t"; } std::cout << "\n";
    // std::ranges::sort_heap(min_heap, [](const auto& lhs, const auto& rhs) {
    //   return std::get<1>(lhs) < std::get<1>(rhs);
    // });
    // std::cout << "\nAfter sort: "; for (const auto& elem : min_heap) { std::cout << " - " << std::get<0>(elem) << " (" << std::get<1>(elem) << "," << std::get<2>(elem) << ")\t"; } std::cout << "\n";
    // 
    if (std::is_integral_v<T> && std::get<0>(min_heap.front()) == 1) {
      return std::vector<std::pair<T, T>>{{dictionary.front(), dictionary.back()}};
    }

    std::ranges::sort(min_heap, [](const auto& lhs, const auto& rhs) {
      return std::get<1>(lhs) < std::get<1>(rhs);
    });
    // std::cout << "\nAfter sort2: "; for (const auto& elem : min_heap) { std::cout << " - " << std::get<0>(elem) << " (" << std::get<1>(elem) << "," << std::get<2>(elem) << ")\t"; } std::cout << "\n";

    // Assert(std::is_sorted(min_heap.begin(), min_heap.end(), [](const auto& lhs, const auto& rhs) {
    //   return std::get<1>(lhs) < std::get<1>(rhs);
    // }), "asdf");

    // std::cout << "MINHEAP\n";
    // for (auto elem : min_heap) {
    //   std::cout << " - " << std::get<0>(elem) << " (" << std::get<1>(elem) << "," << std::get<2>(elem) << ")\t";
    // }
    // std::cout << "\n/MINHEAP\n";

    auto result = std::vector<std::pair<T, T>>{};
    result.reserve(RANGE_COUNT);
    for (auto range_id = size_t{0}; range_id < RANGE_COUNT; ++range_id) {
      if (range_id == 0) {
        result.emplace_back(*dictionary.begin(), std::get<1>(min_heap[range_id]));
      } else if (range_id == RANGE_COUNT - 1) {
        result.emplace_back(std::get<2>(min_heap[range_id - 1]), *(dictionary.end() - 1));
      } else {
        result.emplace_back(std::get<2>(min_heap[range_id - 1]), std::get<1>(min_heap[range_id]));  
      }
    }

    return result;
  }

  int main() {
    const auto benchmark_config = std::make_shared<BenchmarkConfig>();
    TPCHTableGenerator(0.1f, ClusteringConfiguration::None, benchmark_config).generate_and_store();

    auto& sm = Hyrise::get().storage_manager;

    for (const auto& [table_name, table] : sm.tables()) {
      const auto chunk_count = table->chunk_count();
      std::cout << table_name << std::endl;

      const auto& column_definitions = table->column_definitions();

      for (const auto& column_definition : column_definitions) {
        std::cout << "\t" << column_definition.name << std::endl;
        const auto column_id = table->column_id_by_name(column_definition.name);
        if (column_definition.data_type == DataType::String) {
          continue;
        }

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto& chunk = table->get_chunk(chunk_id);
          const auto& segment = chunk->get_segment(column_id);

          const auto encoded_segment = std::static_pointer_cast<AbstractEncodedSegment>(segment);
          Assert(encoded_segment->encoding_type() == EncodingType::Dictionary, "Expected dictionary encoding.");

          resolve_data_type(column_definition.data_type, [&](const auto data_type) {
            using ColumnDataType = typename decltype(data_type)::type;

            if constexpr (std::is_same_v<ColumnDataType, int32_t> || std::is_same_v<ColumnDataType, int64_t> || std::is_same_v<ColumnDataType, float> || std::is_same_v<ColumnDataType, double>) {
              if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment)) {
                const auto& dictionary = dictionary_segment->dictionary();
                
                for (auto run_id = size_t{0}; run_id < MEASUREMENT_COUNT; ++run_id) {
                  const auto start = std::chrono::steady_clock::now();
                  const auto range_filter = RangeFilter<ColumnDataType>::build_filter(*dictionary);
                  const auto end = std::chrono::steady_clock::now();

                  if (run_id == 0) {
                    std::cout << "\tTRADI: " << *range_filter << "\n\tTRADI: ";
                  }
                  std::cout << std::chrono::duration<double>(end - start) * 1000 * 1000 << "\t";
                }
                std::cout << "\n";

                for (auto run_id = size_t{0}; run_id < MEASUREMENT_COUNT; ++run_id) {
                  const auto start = std::chrono::steady_clock::now();
                  const auto range_filter = queue_based(*dictionary);
                  const auto end = std::chrono::steady_clock::now();

                  if (run_id == 0) {
                    auto ss = std::stringstream{};
                    ss << "\tQUEUE: { ";
                    for (const auto& range : range_filter) {
                      // std::cout << " - " << std::get<0>(top) << " (" << std::get<1>(top) << "," << std::get<2>(top) << ")\n";
                      ss << "[" << range.first << ", " << range.second << "], ";
                    }
                    ss << "}\n\tQUEUE: ";
                    std::cout << ss.str();
                  }
                  std::cout << std::chrono::duration<double>(end - start) * 1000 * 1000 << "\t";
                }
                std::cout << "\n";
              } else {
                Fail("Unexpected.");
              }
            }
          });
          
        }
      }
    }
  }
