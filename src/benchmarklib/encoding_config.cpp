#include "encoding_config.hpp"

#include <optional>
#include <string>
#include <utility>

#include "magic_enum/magic_enum.hpp"
#include "nlohmann/json.hpp"

#include "all_type_variant.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "utils/assert.hpp"

namespace hyrise {

EncodingConfig::EncodingConfig(const std::optional<SegmentEncodingSpec>& init_preferred_encoding_spec,
                               DataTypeEncodingMapping init_type_encoding_mapping,
                               TableSegmentEncodingMapping init_encoding_mapping)
    : preferred_encoding_spec{init_preferred_encoding_spec},
      type_encoding_mapping{std::move(init_type_encoding_mapping)},
      custom_encoding_mapping{std::move(init_encoding_mapping)} {}

EncodingConfig EncodingConfig::unencoded() {
  return EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}};
}

std::optional<SegmentEncodingSpec> EncodingConfig::encoding_spec_from_strings(const std::string& encoding_str,
                                                                              const std::string& compression_str) {
  const auto encoding = EncodingConfig::encoding_string_to_type(encoding_str);
  if (!encoding) {
    return {};
  }

  const auto compression = EncodingConfig::compression_string_to_type(compression_str);
  return SegmentEncodingSpec{*encoding, compression};
}

std::optional<EncodingType> EncodingConfig::encoding_string_to_type(const std::string& encoding_str) {
  if (encoding_str == "Automatic") {
    return {};
  }

  const auto type = magic_enum::enum_cast<EncodingType>(encoding_str);
  Assert(type, "Invalid encoding type: '" + encoding_str + "'");
  return type;
}

std::optional<VectorCompressionType> EncodingConfig::compression_string_to_type(const std::string& compression_str) {
  if (compression_str.empty()) {
    return std::nullopt;
  }

  const auto compression = vector_compression_type_to_string.right.find(compression_str);
  Assert(compression != vector_compression_type_to_string.right.end(),
         "Invalid compression type: '" + compression_str + "'");
  return compression->second;
}

nlohmann::json EncodingConfig::to_json() const {
  const auto encoding_spec_to_string_map = [](const SegmentEncodingSpec& spec) {
    auto mapping = nlohmann::json{};
    mapping["encoding"] = std::string{magic_enum::enum_name(spec.encoding_type)};
    if (spec.vector_compression_type) {
      mapping["compression"] = vector_compression_type_to_string.left.at(*spec.vector_compression_type);
    }
    return mapping;
  };

  auto json = nlohmann::json{};
  if (preferred_encoding_spec) {
    json["preferred"] = encoding_spec_to_string_map(*preferred_encoding_spec);
  }

  auto type_mapping = nlohmann::json{};
  for (const auto& [type, spec] : type_encoding_mapping) {
    const auto& type_str = data_type_to_string.left.at(type);
    type_mapping[type_str] = encoding_spec_to_string_map(spec);
  }

  if (!type_mapping.empty()) {
    json["type"] = type_mapping;
  }

  auto table_mapping = nlohmann::json{};
  for (const auto& [table, column_config] : custom_encoding_mapping) {
    auto column_mapping = nlohmann::json{};
    for (const auto& [column, spec] : column_config) {
      column_mapping[column] = encoding_spec_to_string_map(spec);
    }
    table_mapping[table] = column_mapping;
  }

  if (!table_mapping.empty()) {
    json["custom"] = table_mapping;
  }

  json["default"] = "Automatic";

  return json;
}

// This is intentionally limited to 80 chars per line, as cxxopts does this too and it looks bad otherwise.
const char* const EncodingConfig::description = R"(
======================
Encoding Configuration
======================
The encoding config represents the segment encodings specified for a benchmark.
All segments of a given share column the same encoding.
If encoding (and vector compression) were specified via command line args,
all segments are compressed using the default encoding.
If a JSON config was provided, a column- and/or type-specific
encoding/compression can be chosen (same in each chunk). The JSON config must
look like this:

All encoding/compression types can be viewed with the `help` command or seen
in constant_mappings.cpp.
The encoding is always required, the compression is optional.

{
  "preferred": {
    "encoding": <ENCODING_TYPE_STRING>,               // required
    "compression": <VECTOR_COMPRESSION_TYPE_STRING>,  // optional
  },

  "type": {
    <DATA_TYPE>: {
      "encoding": <ENCODING_TYPE_STRING>,
      "compression": <VECTOR_COMPRESSION_TYPE_STRING>
    },
    <DATA_TYPE>: {
      "encoding": <ENCODING_TYPE_STRING>
    }
  },

  "custom": {
    <TABLE_NAME>: {
      <column_name>: {
        "encoding": <ENCODING_TYPE_STRING>,
        "compression": <VECTOR_COMPRESSION_TYPE_STRING>
      },
      <column_name>: {
        "encoding": <ENCODING_TYPE_STRING>
      }
    },
    <TABLE_NAME>: {
      <column_name>: {
        "encoding": <ENCODING_TYPE_STRING>,
        "compression": <VECTOR_COMPRESSION_TYPE_STRING>
      }
    }
  }
})";

}  // namespace hyrise
