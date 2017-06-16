#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/config.hpp>
#include <boost/program_options/environment_iterator.hpp>
#include <boost/program_options/eof_iterator.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/version.hpp>

#include <atomic>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "network/server.hpp"
#include "operators/import_csv.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"

namespace po = boost::program_options;

namespace {
opossum::Server server;
volatile std::sig_atomic_t count_sigint{0};

void sighandler(int signum) {
  switch (signum) {
    case SIGINT:
      if (count_sigint++ == 0) {
        server.stop();
      } else {
        exit(1);
      }
      break;
  }
}

void import_dummy_data(const std::string& directory, const std::string& filename, const std::string& table_name) {
  auto importer = std::make_shared<opossum::ImportCsv>(directory, filename);
  importer->execute();

  // Cannot use importer->get_output() to store the table directly, because it is const. Need to copy values.
  const auto t = importer->get_output();
  auto table = std::make_shared<opossum::Table>();
  for (opossum::ColumnID i{0}; i < t->col_count(); ++i) {
    table->add_column(t->column_name(i), t->column_type(i));
  }
  for (auto i = opossum::ChunkID{0}; i < t->chunk_count(); ++i) {
    auto& chunk = t->get_chunk(i);
    for (size_t row_id = 0; row_id < chunk.size(); ++row_id) {
      auto row = std::vector<opossum::AllTypeVariant>();
      for (opossum::ColumnID col_id{0}; col_id < t->col_count(); ++col_id) {
        row.push_back((*chunk.get_column(col_id))[row_id]);
      }
      table->append(row);
    }
  }
  opossum::StorageManager::get().add_table(table_name, table);
}
}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, sighandler);

  opossum::ServerConfiguration config;

  po::options_description desc("Allowed options");
  auto options = desc.add_options();
  options("help", "print help message");
  options("address", po::value<std::string>()->default_value("0.0.0.0:50051"), "IP:PORT");
  options("listener_threads", po::value<size_t>()->default_value(1), "Number of threads listening for client requests");
  options("skip_scheduler",
          "Skip scheduling and add response to sender queue immediately - useful for grpc no-op benchmarking");
  options("numa_max_cores", po::value<uint32_t>()->default_value(0),
          "Number of max cores used - zero indicates no limit");
  options("fake_numa", "Use a fake numa topology");
  options("fake_numa_max_workers", po::value<uint32_t>()->default_value(0),
          "Number of max workers used - zero indicates no limit");
  options("fake_numa_workers_per_node", po::value<uint32_t>()->default_value(1), "Number of max workers per node");
  if (IS_DEBUG) {
    options("csv_import_dir", po::value<std::string>()->default_value("src/test/csv"), "CSV import folder");
    options("csv_import_filename", po::value<std::string>()->default_value("float_int"),
            "Filename (without .csv) - file is imported once at server start");
    options("csv_import_table_name", po::value<std::string>()->default_value("table1"), "Import target table name");
  }

  po::variables_map variables;
  po::store(po::parse_command_line(argc, argv, desc), variables);
  po::notify(variables);

  if (variables.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  config.address = variables["address"].as<std::string>();
  config.num_listener_threads = variables["listener_threads"].as<size_t>();
  config.skip_scheduler = variables.count("skip_scheduler") > 0;

  if (variables.count("fake_numa")) {
    config.topology = opossum::Topology::create_fake_numa_topology(
        variables["fake_numa_max_workers"].as<uint32_t>(), variables["fake_numa_workers_per_node"].as<uint32_t>());
  } else {
    config.topology = opossum::Topology::create_numa_topology(variables["numa_max_cores"].as<uint32_t>());
  }

  if (IS_DEBUG) {
    try {
      // Provide some dummy data during development - can be removed when persistance is implemented
      import_dummy_data(variables["csv_import_dir"].as<std::string>(),
                        variables["csv_import_filename"].as<std::string>(),
                        variables["csv_import_table_name"].as<std::string>());
    } catch (const std::ios_base::failure e) {
      std::cerr << "Unable to import CSV: " << variables["csv_import_dir"].as<std::string>() << "/"
                << variables["csv_import_filename"].as<std::string>() << std::endl;
      std::cerr << "Error: " << e.what() << std::endl;
      std::cerr << "Dummy data import skipped" << std::endl;
    }
  }

  server.start(config);

  return 0;
}
