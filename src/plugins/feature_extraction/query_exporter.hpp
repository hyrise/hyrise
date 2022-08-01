#pragma once

#include "feature_types.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace opossum {

class QueryExporter {
 public:
  static std::string query_hash(const std::string& query);

  QueryExporter();

  ~QueryExporter();

  void add_query(const std::shared_ptr<Query>& query);

  void export_queries(const std::string& file_path);

 protected:
 protected:
  class FileName : public AbstractSetting {
   public:
    static constexpr const char* DEFAULT_FILE_NAME = "queries.csv";
    explicit FileName(const std::string& init_name);

    const std::string& description() const final;

    const std::string& get() final;

    void set(const std::string& value) final;

   private:
    std::string _value = DEFAULT_FILE_NAME;
  };
  std::shared_ptr<FileName> _file_name;

  std::vector<std::shared_ptr<Query>> _queries;
};

}  // namespace opossum
