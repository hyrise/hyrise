#include "base_test.hpp"

#include "storage/value_segment.hpp"
#include "storage/table.hpp"
#include "storage/materialize.hpp"

namespace {

using namespace opossum;  // NOLINT

std::map<std::pair<std::string, ChunkOffset>, std::shared_ptr<Table>> table_cache;

}  // namespace

namespace opossum {

std::shared_ptr<Table> load_table_cached(const std::string& file_name, size_t chunk_size) {
  auto cache_iter = table_cache.find({file_name, chunk_size});

  if (cache_iter == table_cache.end()) {
    cache_iter = table_cache.emplace(std::make_pair(file_name, chunk_size), load_table(file_name, chunk_size)).first;
  }

  const auto cached_table = cache_iter->second;

  const auto result_table = std::make_shared<Table>(cached_table->column_definitions(),
  TableType::Data, cached_table->max_chunk_size(), UseMvcc::Yes);
  
  for (const auto& cached_chunk : cached_table->chunks()) {
    auto segments = Segments{cached_table->column_count()};

    for (auto column_id = ColumnID{0}; column_id < cached_table->column_count(); ++column_id) {
      const auto cached_segment = cached_chunk->get_segment(column_id);
      const auto cached_value_segment = std::dynamic_pointer_cast<BaseValueSegment>(cached_segment);
      Assert(cached_value_segment, "Expected ValueSegment in cache");

      resolve_data_type(cached_table->column_data_type(column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        pmr_concurrent_vector<ColumnDataType> values;
        values.reserve(cached_segment->size());
        materialize_values(*cached_segment, values);

        if (cached_value_segment->is_nullable()) {
          pmr_concurrent_vector<bool> null_values;
          null_values.reserve(cached_segment->size());
          materialize_nulls<ColumnDataType>(*cached_segment, null_values);

          segments[column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
        } else {

          segments[column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
        }
      });

    }

    const auto mvcc_data = std::make_shared<MvccData>(cached_chunk->mvcc_data()->size());
    mvcc_data->tids = cached_chunk->mvcc_data()->tids;
    mvcc_data->begin_cids = cached_chunk->mvcc_data()->begin_cids;
    mvcc_data->end_cids = cached_chunk->mvcc_data()->end_cids;

    const auto chunk = std::make_shared<Chunk>(segments, mvcc_data);
    result_table->append_chunk(chunk);
  }

  return result_table;
}

}  // namespace opossum
