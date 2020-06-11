#include "calibration_query_generator_predicate.hpp"

#include <random>

#include "constant_mappings.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "storage/storage_manager.hpp"
#include "synthetic_table_generator.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

const std::vector<CalibrationQueryGeneratorPredicateConfiguration>
CalibrationQueryGeneratorPredicate::generate_predicate_permutations(
    const std::vector<std::pair<std::string, size_t>>& tables, const CalibrationConfiguration& configuration) {
  std::vector<CalibrationQueryGeneratorPredicateConfiguration> output;

  //
  // Simple column scans
  //
  for (const auto& column : configuration.columns) {
    for (const auto& selectivity : configuration.selectivities) {
      for (const auto& [table_name, table_size] : tables) {
        // With and without ReferenceSegments
        output.push_back({table_name, column.data_type, column.encoding, std::nullopt, std::nullopt, selectivity, false,
                          table_size});
        output.push_back(
            {table_name, column.data_type, column.encoding, std::nullopt, std::nullopt, selectivity, true, table_size});
      }
    }
  }

  // Wrap all encodings in std::optional and add std::nullopt. Used for second and third column encodings
  std::vector<std::optional<SegmentEncodingSpec>> all_encodings;
  for (const auto& encoding : configuration.encodings) {
    all_encodings.emplace_back(encoding);
  }
  all_encodings.push_back({});

  /**
   * Column To Column Scans:
   * We generate not all, but most column to column scans since they are important for multi-predicate joins, where non-first
   * predicates are executed as column to column scans after the actual join.
   * We neglect vector compression types and only calibrate combinations of encodings and data types.
   */
  // Generating all combinations
  for (const auto& data_type : configuration.data_types) {
    for (const auto& first_encoding : configuration.encodings) {
      // Illegal data type - encoding combinations
      if (data_type != DataType::String && first_encoding == EncodingType::FixedStringDictionary) continue;
      if (data_type != DataType::Int && data_type != DataType::Long &&
          first_encoding == EncodingType::FrameOfReference) {
        continue;
      }
      for (const auto& second_encoding : all_encodings) {
        // Illegal data type - encoding combination
        if (second_encoding && data_type != DataType::String &&
            second_encoding == EncodingType::FixedStringDictionary) {
          continue;
        }
        // Illegal data type - encoding combination
        if (second_encoding && data_type != DataType::Int && data_type != DataType::Long &&
            second_encoding == EncodingType::FrameOfReference) {
          continue;
        }

        // for (const auto& third_encoding : all_encodings) {
        //     // Cannot generate queries without second_encoding but with third_encoding
        //     if (!second_encoding && third_encoding) {
        //         continue;
        //     }
        //     // Illegal data type - encoding combination
        //   if (third_encoding && data_type != DataType::String &&
        //       third_encoding == EncodingType::FixedStringDictionary) {
        //     continue;
        //   }
        //     // Illegal data type - encoding combination
        //   if (third_encoding && data_type != DataType::Int && data_type != DataType::Long &&
        //       third_encoding == EncodingType::FrameOfReference) {
        //     continue;
        //   }

        //   // If we have a column-to-column scan, we don't need the selectivity..
        //   // effectively reducing the number of generated queries
        //   if (second_encoding) {
        //     for (const auto& table : tables) {
        //       // With and without ReferenceSegments
        //       output.push_back({table.first, data_type, first_encoding, second_encoding, third_encoding, 0.5,
        //                         false, table.second});
        //       output.push_back({table.first, data_type, first_encoding, second_encoding, third_encoding, 0.5,
        //                         true, table.second});
        //     }
        //   } else {
        //     for (const auto& selectivity : configuration.selectivities) {
        //       for (const auto& table : tables) {
        //         // With and without ReferenceSegments
        //         output.push_back({table.first, data_type, first_encoding, second_encoding, third_encoding, selectivity,
        //                           false, table.second});
        //         output.push_back({table.first, data_type, first_encoding, second_encoding, third_encoding, selectivity,
        //                           true, table.second});
        //       }
        //     }
        //   }
        // }
      }
    }
  }

  std::stringstream ss;
  ss << "WARNING: Scans in the form `WHERE a = b` on columns a and b are currently not being calibrated.\n";
  ss << "WARNING: Scans in the form `WHERE a between b and c` on columns a, b, c are currently not being";
  ss << " calibrated as we consider them too rare to be of relevance here. In case they are of interest,";
  ss << " check out previous versions of calibration_query_generator_predicate.cpp.";
  std::cout << ss.str() << std::endl;

  std::cout << "Generated " << output.size() << " permutations for Predicates" << std::endl;
  return output;
}

/**
 * Generates a list of predicates that are generated by the predicate_generator,
 * e.g., an IndexScan-based and a TableScan-based variant,
 * but also permutations for different secondary columns in cases with column-to-columns scans.
*/
const std::vector<std::shared_ptr<PredicateNode>> CalibrationQueryGeneratorPredicate::generate_predicates(
    const PredicateGeneratorFunctor& predicate_generator,
    const std::vector<CalibrationColumnSpecification>& column_definitions,
    const std::shared_ptr<StoredTableNode>& table, const CalibrationQueryGeneratorPredicateConfiguration& configuration,
    const bool generate_index_scan) {
  auto predicate = predicate_generator({table, column_definitions, configuration});

  // TODO(Sven): add test for this case
  if (!predicate) {
    std::cout << "missing predicate for configuration " << configuration << std::endl;
    return {};
  }

  std::vector<ScanType> scan_types = {ScanType::TableScan, ScanType ::IndexScan};
  std::vector<std::shared_ptr<PredicateNode>> permutated_predicate_nodes{};

  for (const auto& scan_type : scan_types) {
    // IndexScans support less options than TableScans.. let's filter here
    if (scan_type == ScanType::IndexScan) {
      if (!generate_index_scan) continue;

      auto column_expression_count = 0;
      visit_expression(predicate, [&](const auto& sub_expression) {
        if (sub_expression->type == ExpressionType::LQPColumn) {
          column_expression_count += 1;
        }

        return ExpressionVisitation::VisitArguments;
      });

      // IndexScan neither works on ReferenceSegments nor handles multiple columns in B-Tree
      if (configuration.reference_column || column_expression_count > 1) continue;

      //      if (predicate->type != ExpressionType::Predicate) {
      //        std::cout << "Is this really supported? IndexScan with ExpressionType::"
      //                  << expression_type_to_string.at(predicate->type) << std::endl;
      //      }

      if (predicate->type == ExpressionType::Predicate) {
        const auto abstract_predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate);
        // IndexScans do not support Like or NotLike predicates
        const auto predicate_condition = abstract_predicate_expression->predicate_condition;
        if (predicate_condition == PredicateCondition::Like || predicate_condition == PredicateCondition::NotLike) {
          continue;
        }
      }
    }
    const auto predicate_node = PredicateNode::make(predicate);
    predicate_node->scan_type = scan_type;
    permutated_predicate_nodes.push_back(predicate_node);
  }

  return permutated_predicate_nodes;
}

const std::shared_ptr<PredicateNode> CalibrationQueryGeneratorPredicate::generate_concreate_scan_predicate(
    const std::shared_ptr<AbstractExpression>& predicate, const ScanType scan_type) {
  const auto predicate_node = PredicateNode::make(predicate);
  predicate_node->scan_type = scan_type;

  // Additional checks if IndexScan is applicable
  if (scan_type == ScanType::IndexScan) {
    auto column_expression_count = 0;
    visit_expression(predicate, [&](const auto& sub_expression) {
      if (sub_expression->type == ExpressionType::LQPColumn) {
        column_expression_count += 1;
      }

      return ExpressionVisitation::VisitArguments;
    });

    // IndexScan do not handle multiple columns
    if (column_expression_count > 1) {
      Fail("Index scans on multiple columns are not supported.");
    }

    if (predicate->type == ExpressionType::Predicate) {
      const auto abstract_predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate);
      // IndexScans do not support Like or NotLike predicates
      const auto predicate_condition = abstract_predicate_expression->predicate_condition;
      if (predicate_condition == PredicateCondition::Like || predicate_condition == PredicateCondition::NotLike) {
        Fail("Index scans with like predicates are not supported.");
      }
    }
  }

  return predicate_node;
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value(
    const PredicateGeneratorFunctorConfiguration& generator_configuration) {
  const auto calibration_config = generator_configuration.configuration;
  const auto table = generator_configuration.table;

  const auto scan_column_configuration =
      _find_column_for_configuration(generator_configuration.column_definitions, calibration_config.data_type,
                                     calibration_config.first_encoding.encoding_type);

  if (!scan_column_configuration) {
    std::cout << "BetweenValueValue: Did not find query for configuration " << calibration_config << std::endl;
    return {};
  }

  const auto scan_column = _generate_column_expression(table, *scan_column_configuration);
  const auto first_value = _generate_value_expression(calibration_config.data_type, calibration_config.selectivity);
  const auto second_value = _generate_value_expression(calibration_config.data_type, 0.0f);

  if (first_value->value < second_value->value) {
    return between_inclusive_(scan_column, first_value, second_value);
  }
  return between_inclusive_(scan_column, second_value, first_value);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column(
    const PredicateGeneratorFunctorConfiguration& generator_configuration) {
  const auto calibration_config = generator_configuration.configuration;

  Assert(calibration_config.second_encoding, "BetweenColumnColumn needs second encoding");
  const auto second_encoding = *calibration_config.second_encoding;
  Assert(calibration_config.third_encoding, "BetweenColumnColumn needs third encoding");
  const auto third_encoding = *calibration_config.third_encoding;

  const auto table = generator_configuration.table;
  auto remaining_columns = generator_configuration.column_definitions;

  const auto scan_column_configuration = _find_column_for_configuration(
      remaining_columns, calibration_config.data_type, calibration_config.first_encoding.encoding_type);

  DebugAssert(scan_column_configuration, "BetweenColumnColumn::Did not find scan_column_configuration");

  remaining_columns.erase(std::remove(remaining_columns.begin(), remaining_columns.end(), scan_column_configuration),
                          remaining_columns.end());

  const auto second_column_configuration =
      _find_column_for_configuration(remaining_columns, calibration_config.data_type, second_encoding.encoding_type);

  DebugAssert(second_column_configuration, "BetweenColumnColumn::Did not find first_column_configuration");

  remaining_columns.erase(std::remove(remaining_columns.begin(), remaining_columns.end(), second_column_configuration),
                          remaining_columns.end());

  const auto third_column_configuration =
      _find_column_for_configuration(remaining_columns, calibration_config.data_type, third_encoding.encoding_type);

  DebugAssert(third_column_configuration, "BetweenColumnColumn::Did not find second_column_configuration");

  if (!scan_column_configuration || !second_column_configuration || !third_column_configuration) {
    std::cout << "BetweenColumnColumn: Did not find query for configuration " << calibration_config << std::endl;
    return {};
  }

  const auto scan_column = _generate_column_expression(table, *scan_column_configuration);
  const auto second_value = _generate_column_expression(table, *second_column_configuration);
  const auto third_value = _generate_column_expression(table, *third_column_configuration);

  return between_inclusive_(scan_column, second_value, third_value);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_column_value(
    const PredicateGeneratorFunctorConfiguration& generator_configuration) {
  const auto calibration_config = generator_configuration.configuration;

  const auto table = generator_configuration.table;
  const auto filter_column_configuration =
      _find_column_for_configuration(generator_configuration.column_definitions, calibration_config.data_type,
                                     calibration_config.first_encoding.encoding_type);

  if (!filter_column_configuration) {
    std::cout << "ColumnValue: Did not find query for configuration " << calibration_config << std::endl;
    return {};
  }

  const auto filter_column = _generate_column_expression(table, *filter_column_configuration);
  const auto value = _generate_value_expression(calibration_config.data_type, calibration_config.selectivity);
  return less_than_equals_(filter_column, value);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_concrete_predicate_column_value(
    const std::shared_ptr<StoredTableNode> table_node, const CalibrationColumnSpecification column_specification,
    const float selectivity, const StringPredicateType string_predicate_type) {
  const auto lqp_column_reference = table_node->get_column(column_specification.column_name);

  const auto value = _generate_value_expression(column_specification, selectivity, string_predicate_type);

  if (string_predicate_type != StringPredicateType::Equality)
    return like_(lqp_column_reference, value);

  return less_than_equals_(lqp_column_reference, value);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_column_column(
    const PredicateGeneratorFunctorConfiguration& generator_configuration) {
  const auto calibration_config = generator_configuration.configuration;

  Assert(calibration_config.second_encoding, "BetweenColumnColumn needs second encoding");
  const auto second_encoding = *calibration_config.second_encoding;

  auto remaining_columns = generator_configuration.column_definitions;

  const auto table = generator_configuration.table;
  const auto first_column_configuration = _find_column_for_configuration(
      remaining_columns, calibration_config.data_type, calibration_config.first_encoding.encoding_type);

  remaining_columns.erase(std::remove(remaining_columns.begin(), remaining_columns.end(), first_column_configuration),
                          remaining_columns.end());

  const auto second_column_configuration =
      _find_column_for_configuration(remaining_columns, calibration_config.data_type, second_encoding.encoding_type);

  if (!first_column_configuration || !second_column_configuration) {
    std::cout << "ColumnColumn: Did not find query for configuration " << calibration_config << std::endl;
    return {};
  }

  const auto first_column = _generate_column_expression(table, *first_column_configuration);
  const auto second_column = _generate_column_expression(table, *second_column_configuration);

  return less_than_equals_(first_column, second_column);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_like(
    const PredicateGeneratorFunctorConfiguration& generator_configuration) {
  const auto calibration_config = generator_configuration.configuration;

  if (calibration_config.data_type != DataType::String) {
    std::cout << "Like: trying to generate for non-string column" << std::endl;
    return {};
  }

  const auto table = generator_configuration.table;
  const auto filter_column_configuration =
      _find_column_for_configuration(generator_configuration.column_definitions, calibration_config.data_type,
                                     calibration_config.first_encoding.encoding_type);

  if (!filter_column_configuration) {
    std::cout << "Like: Did not find query for configuration " << calibration_config << std::endl;
    return {};
  }

  const auto filter_column = _generate_column_expression(table, *filter_column_configuration);
  const auto value = _generate_value_expression(calibration_config.data_type, calibration_config.selectivity, 0, true);

  return like_(filter_column, value);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings(
    const PredicateGeneratorFunctorConfiguration& generator_configuration) {
  const auto calibration_config = generator_configuration.configuration;

  if (calibration_config.data_type != DataType::String) {
    std::cout << "EquiString: trying to generate for non-string column" << std::endl;
    return {};
  }

  const auto table = generator_configuration.table;
  const auto filter_column_configuration =
      _find_column_for_configuration(generator_configuration.column_definitions, calibration_config.data_type,
                                     calibration_config.first_encoding.encoding_type);

  if (!filter_column_configuration) {
    std::cout << "EquiString: Did not find query for configuration " << calibration_config << std::endl;
    return {};
  }

  const auto filter_column = _generate_column_expression(table, *filter_column_configuration);
  const auto column_id = filter_column->original_column_id;

  // Get an existing value from column and filter by that
  static std::mt19937 engine((std::random_device()()));
  const auto stored_table = Hyrise::get().storage_manager.get_table(table->table_name);
  std::uniform_int_distribution<uint64_t> row_id_dist(0, stored_table->row_count() - 1);
  const auto row_id = row_id_dist(engine);
  const auto value = stored_table->get_value<pmr_string>(column_id, row_id);

  return equals_(filter_column, *value);
}

const std::shared_ptr<AbstractExpression> CalibrationQueryGeneratorPredicate::generate_predicate_or(
    const PredicateGeneratorFunctorConfiguration& generator_configuration) {
  const auto calibration_config = generator_configuration.configuration;

  const CalibrationQueryGeneratorPredicateConfiguration second_configuration{
      calibration_config.table_name,       calibration_config.data_type,      calibration_config.first_encoding,
      calibration_config.second_encoding,  calibration_config.third_encoding, 0.5f,
      calibration_config.reference_column, calibration_config.row_count};

  const auto lhs = generate_predicate_column_value(
      {generator_configuration.table, generator_configuration.column_definitions, calibration_config});
  const auto rhs = generate_predicate_column_value(
      {generator_configuration.table, generator_configuration.column_definitions, second_configuration});

  if (!lhs || !rhs) {
    std::cout << "Or: Did not find query for configuration " << calibration_config << std::endl;
    return {};
  }

  return or_(lhs, rhs);
}

/**
 *
 * @param column_definitions
 * @param data_type
 * @param encoding_type
 * @return
 */
const std::optional<CalibrationColumnSpecification> CalibrationQueryGeneratorPredicate::_find_column_for_configuration(
    const std::vector<CalibrationColumnSpecification>& column_definitions, const DataType& data_type,
    const EncodingType& encoding_type) {
  for (const auto& definition : column_definitions) {
    if (definition.data_type == data_type && definition.encoding.encoding_type == encoding_type) {
      return definition;
    }
  }

  return {};
}

const std::shared_ptr<LQPColumnExpression> CalibrationQueryGeneratorPredicate::_generate_column_expression(
    const std::shared_ptr<StoredTableNode>& table, const CalibrationColumnSpecification& filter_column) {
  const auto column_name = filter_column.column_name;
  return table->get_column(column_name);
}

const std::shared_ptr<ValueExpression> CalibrationQueryGeneratorPredicate::_generate_value_expression(
    const DataType& data_type, const float selectivity, const size_t int_value_upper_limit, const bool trailing_like) {
  const auto int_value = static_cast<int32_t>(static_cast<float>(int_value_upper_limit) * selectivity);
  const auto long_value = static_cast<int64_t>(int_value);
  const auto float_value = selectivity;
  const auto double_value = static_cast<double>(selectivity);
  const auto string_value = static_cast<int>(25 * selectivity);

  switch (data_type) {
    case DataType::Int:
      return value_(int_value);
    case DataType::Long:
      return value_(long_value);
    case DataType::String: {
      auto search_string = pmr_string(4, ' ');  // strings are generated with 4 starting spaces
      search_string += static_cast<char>('A' + string_value);
      if (trailing_like) {
        return value_(search_string + '%');
      }
      return value_(search_string);
    }
    case DataType::Float:
      return value_(float_value);
    case DataType::Double:
      return value_(double_value);
    case DataType::Null:
    default:
      Fail("Unsupported data type in CalibrationQueryGeneratorPredicates: " + data_type_to_string.left.at(data_type));
  }
}

const std::shared_ptr<ValueExpression> CalibrationQueryGeneratorPredicate::_generate_value_expression(
    const CalibrationColumnSpecification& column_specification, const float selectivity,
    const StringPredicateType string_predicate_type) {
  const auto distinct_value_count = column_specification.distinct_value_count;

  const auto int_value = static_cast<int32_t>(selectivity * static_cast<float>(distinct_value_count));
  const auto long_value = static_cast<int64_t>(selectivity * static_cast<float>(distinct_value_count));
  const auto float_value = selectivity * static_cast<float>(distinct_value_count);
  const auto double_value = static_cast<double>(selectivity);

  switch (column_specification.data_type) {
    case DataType::Int:
      return value_(int_value);
    case DataType::Long:
      return value_(long_value);
    case DataType::String: {
      auto search_string = SyntheticTableGenerator::generate_value<pmr_string>(int_value);

      // TODO: like-predicates do not care about the given selectivity, yet.
      if (string_predicate_type == StringPredicateType::TrailingLike) {
        search_string[search_string.size() - 1] = '%';
        return value_(search_string);
      } else if (string_predicate_type == StringPredicateType::PrecedingLike) {
        const auto first_non_space = search_string.find_first_not_of(' ');
        if (first_non_space == std::string::npos) {
          // empty string
          return value_(pmr_string{'%'});
        }
        return value_('%' + search_string.substr(first_non_space));
      }
      return value_(search_string);
    }
    case DataType::Float:
      return value_(float_value);
    case DataType::Double:
      return value_(double_value);
    case DataType::Null:
    default:
      Fail("Unsupported data type in CalibrationQueryGeneratorPredicates: " +
           data_type_to_string.left.at(column_specification.data_type));
  }
}

}  // namespace opossum
