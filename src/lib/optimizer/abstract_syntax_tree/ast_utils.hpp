#pragma once

#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/ast_types.hpp"
#include "types.hpp"

/**
 * This file contains functions that operate on AbstractSyntaxTrees, but, in order to keep the interface of
 * AbstractASTNode clean, are not part of the class.
 * This is intended for functionality that is *read-only* and operates on the interface of AbstractASTNode without
 * needing to know of derived node types.
 */

namespace opossum {

class AbstractASTNode;

/**
 * Given two shuffled sets of ColumnOrigins, determine the mapping of the indices from one (column_origins_a) into the
 * other (column_origins_b)
 */
ColumnIDMapping ast_generate_column_id_mapping(const ColumnOrigins& column_origins_a,
                                               const ColumnOrigins& column_origins_b);

}  // namespace opossum