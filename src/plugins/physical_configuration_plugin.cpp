#include "physical_configuration_plugin.hpp"

#include "hyrise.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "physical_configuration/physical_config.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterate.hpp"

namespace hyrise {

std::string PhysicalConfigurationPlugin::description() const {
  return "This is the Hyrise PhysicalConfigurationPlugin";
}

void PhysicalConfigurationPlugin::start() {
  _config_path = std::make_shared<ConfigPath>("PhysicalConfigurationPlugin.ConfigPath");
  _config_path->register_at_settings_manager();
}

void PhysicalConfigurationPlugin::stop() {
  _config_path->unregister_at_settings_manager();
}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
PhysicalConfigurationPlugin::provided_user_executable_functions() {
  return {{"ApplyPhysicalConfiguration", [&]() { this->apply_physical_configuration(); }}};
}

void PhysicalConfigurationPlugin::apply_physical_configuration() {
  std::cout << "apply_physical_configuration" << std::endl;
  auto conf = TableSegmentConfigMapping{};
  conf["nation"]["n_nationkey"].emplace_back(
      ChunkID{0},
      SegmentPhysicalConfig{SegmentEncodingSpec{EncodingType::RunLength}, AbstractSegment::Tier::Memory, false});
  conf["region"]["r_regionkey"].emplace_back(
      ChunkID{0}, SegmentPhysicalConfig{SegmentEncodingSpec{}, AbstractSegment::Tier::Memory, true});
  conf["lineitem"]["l_orderkey"].emplace_back(
      ChunkID{10}, SegmentPhysicalConfig{SegmentEncodingSpec{EncodingType::LZ4}, AbstractSegment::Tier::Memory, true});
  const auto config = std::make_shared<PhysicalConfig>(SegmentPhysicalConfig{}, std::move(conf));
  const auto& storage_manager = Hyrise::get().storage_manager;
  const auto& table_names = storage_manager.table_names();

  const auto sort_definition = SortColumnDefinition(ColumnID{INVALID_COLUMN_ID});
  const auto ascending = sort_definition.sort_mode == SortMode::Ascending;

  for (const auto& table_name : table_names) {
    Assert(storage_manager.has_table(table_name), "Table '" + table_name + "' was deleted unexpectedly");
    const auto table = storage_manager.get_table(table_name);
    const auto& column_names = table->column_names();
    const auto chunk_count = table->chunk_count();
    for (const auto& column_name : column_names) {
      const auto column_id = table->column_id_by_name(column_name);
      const auto& configs_for_column = config->configs_for_column(table_name, column_name);
      auto config_iter = configs_for_column.cbegin();
      for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
        const auto chunk = table->get_chunk(chunk_id);
        if (!chunk) {
          continue;
        }

        const auto custom_config_given = config_iter != configs_for_column.cend() && chunk_id == config_iter->first;
        const auto& segment_config = custom_config_given ? config_iter->second : config->fallback_config;

        // Physical config given, apply it
        auto segment = chunk->get_segment(column_id);
        const auto& old_config = get_segment_encoding_spec(segment);

        if (segment_config.sorted) {
          Hyrise::get().log_manager.add_message(
              "PhysicalConfigurationPlugin",
              "Sort " + table_name + "." + column_name + " chunk " + std::to_string(chunk_id));
          auto dummy_table = Table::create_dummy_table({table->column_definitions().at(column_id)});
          dummy_table->append_chunk({segment});
          auto table_wrapper = std::make_shared<TableWrapper>(dummy_table);
          const auto sort_definition = SortColumnDefinition{ColumnID{0}};
          auto sort = std::make_shared<Sort>(table_wrapper, std::vector<SortColumnDefinition>{sort_definition},
                                             segment->size(), Sort::ForceMaterialization::Yes);
          table_wrapper->execute();
          sort->execute();
          const auto& output_table = sort->get_output();
          Assert(output_table->chunk_count() == ChunkID{1} && output_table->column_count() == ColumnID{1},
                 "Malformed output table");
          const auto old_segment_size = segment->size();
          segment = output_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
          Assert(old_segment_size == segment->size(), "Rows manipulated while sorting");
        }

        if ((old_config.encoding_type != segment_config.encoding_spec.encoding_type) ||
            (segment_config.encoding_spec.vector_compression_type &&
             segment_config.encoding_spec.vector_compression_type != old_config.vector_compression_type)) {
          auto message = std::stringstream{};
          message << "Encode " << table_name << "." << column_name + ":" << chunk_id << " ("
                  << segment_config.encoding_spec << ")";
          Hyrise::get().log_manager.add_message("PhysicalConfigurationPlugin", message.str());
        }
        segment = ChunkEncoder::encode_segment(segment, segment->data_type(), segment_config.encoding_spec);

        // TODO: tiering!!!
        // if (config.tier == AbstractSegment::Tier::SSD) {
        //   do something
        // }

        chunk->replace_segment(column_id, segment);

        // if (segment_config.sorted) {
        // chunk->set_individually_sorted_by(SortColumnDefinition{column_id});
        // }

        if (custom_config_given) {
          ++config_iter;
        }
      }
    }
  }

  // set sort orders
  for (const auto& table_name : table_names) {
    Assert(storage_manager.has_table(table_name), "Table '" + table_name + "' was deleted unexpectedly");
    const auto table = storage_manager.get_table(table_name);
    const auto chunk_count = table->chunk_count();
    const auto column_count = table->column_count();

    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      if (!chunk) {
        continue;
      }

      Assert(chunk->individually_sorted_by().empty(),
             "Sort orders already set for " + table_name + " chunk " + std::to_string(chunk_id));

      auto sorted_by = std::vector<SortColumnDefinition>{};
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto segment = chunk->get_segment(column_id);
        segment_with_iterators(*segment, [&](auto begin, auto end) {
          if (std::is_sorted(begin, end, [&ascending](const auto& left, const auto& right) {
                // is_sorted evaluates the segment by calling the lambda with the SegmentPositions at
                // it+n and it (n being non-negative), which needs to evaluate to false.
                if (right.is_null()) {
                  return false;  // handles right side is NULL and both are NULL
                }
                if (left.is_null()) {
                  return true;
                }
                return ascending ? left.value() < right.value() : left.value() > right.value();
              })) {
            sorted_by.emplace_back(column_id, sort_definition.sort_mode);
          }
        });
      }

      if (!sorted_by.empty()) {
        chunk->set_individually_sorted_by(sorted_by);
      }
    }
  }
  const auto print_flags = static_cast<PrintFlags>(static_cast<uint32_t>(PrintFlags::IgnoreCellWidth) |
                                                   static_cast<uint32_t>(PrintFlags::IgnoreChunkBoundaries));
  Print::print(Hyrise::get().meta_table_manager.generate_table("log"), print_flags);
}

PhysicalConfigurationPlugin::ConfigPath::ConfigPath(const std::string& init_name) : AbstractSetting(init_name) {}

const std::string& PhysicalConfigurationPlugin::ConfigPath::description() const {
  static const auto description = std::string{"Path to JSON file with physical configuration"};
  return description;
}

const std::string& PhysicalConfigurationPlugin::ConfigPath::get() {
  return _value;
}

void PhysicalConfigurationPlugin::ConfigPath::set(const std::string& value) {
  _value = value;
}

EXPORT_PLUGIN(PhysicalConfigurationPlugin)

}  // namespace hyrise
