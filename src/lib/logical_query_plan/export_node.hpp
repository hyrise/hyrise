#include <string>

#include "abstract_lqp_node.hpp"
#include "enable_make_for_lqp_node.hpp"
#include "import_export/file_type.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

/**
 * This node type represents the COPY TO management command.
 */
class ExportNode : public EnableMakeForLQPNode<ExportNode>, public AbstractLQPNode {
 public:
  ExportNode(const std::string& init_tablename, const std::string& init_filename, const FileType init_filetype, const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string tablename;
  const std::string filename;
  const FileType filetype;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
