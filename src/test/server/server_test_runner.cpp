#include <pqxx/pqxx>

#include <thread>

#include "base_test.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"

#include "server/server.hpp"

namespace opossum {

class ServerTestRunner : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().reset();

    _table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", _table_a);

    // Set scheduler so that the server can execute the tasks on separate threads.
    opossum::CurrentScheduler::set(
        std::make_shared<opossum::NodeQueueScheduler>(opossum::Topology::create_numa_topology()));

    auto server_runner = [](boost::asio::io_service& io_service, const uint16_t port) {
      // The server registers itself to the boost io_service. The io_service is the main IO control unit here and it
      // lives until the server doesn't request any IO any more, i.e. is has terminated. The server requests IO in its
      // constructor and then runs forever.
      opossum::Server server{io_service, port};

      io_service.run();
    };

    _server_thread = std::make_unique<std::thread>(server_runner, std::ref(_io_service), _server_port);
    _connection_string = "hostaddr=127.0.0.1 port=" + std::to_string(_server_port);
  }

  void TearDown() override {
    _io_service.stop();
    _server_thread->join();
  }

  uint16_t _server_port = 54321;
  boost::asio::io_service _io_service;
  std::unique_ptr<std::thread> _server_thread;
  std::string _connection_string;

  std::shared_ptr<Table> _table_a;
};

TEST_F(ServerTestRunner, TestSimpleSelect) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), _table_a->row_count()) << "Expected " << _table_a->row_count() << " rows, but got "
                                                  << result.size();
}

TEST_F(ServerTestRunner, TestSimpleInsertSelect) {
  pqxx::connection connection{_connection_string};
  pqxx::nontransaction transaction{connection};

  const auto num_rows = _table_a->row_count();
  transaction.exec("INSERT INTO table_a VALUES (1, 1.0);");
  const auto result = transaction.exec("SELECT * FROM table_a;");
  EXPECT_EQ(result.size(), num_rows + 1) << "Expected " << num_rows + 1 << " rows, but got " << result.size();
}

}  // namespace opossum
