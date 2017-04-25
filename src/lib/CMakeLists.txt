# Configure protobuf and grpc
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/network/generated
    COMMAND [ -d ${CMAKE_CURRENT_SOURCE_DIR}/network/generated ] || mkdir ${CMAKE_CURRENT_SOURCE_DIR}/network/generated
)

add_custom_command(
    OUTPUT
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.grpc.pb.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.grpc.pb.h
    COMMAND
        ${PROJECT_SOURCE_DIR}/third_party/grpc/bins/opt/protobuf/protoc
            --grpc_out=./network/generated
            --plugin=protoc-gen-grpc=${PROJECT_SOURCE_DIR}/third_party/grpc/bins/opt/grpc_cpp_plugin
            -I=\"./network/protos/\"
            ./network/protos/opossum.proto
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated
        ./network/protos/opossum.proto
    WORKING_DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_command(
    OUTPUT
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.pb.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.pb.h
    COMMAND
        ${PROJECT_SOURCE_DIR}/third_party/grpc/bins/opt/protobuf/protoc
            --cpp_out=./network/generated
            -I=\"./network/protos/\"
            ./network/protos/opossum.proto
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated
        ./network/protos/opossum.proto
    WORKING_DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# https://cmake.org/cmake/help/v3.0/command/add_custom_command.html:
#   Do not list the output in more than one independent target that may build in parallel or the two instances of the
#   rule may conflict (instead use add_custom_target to drive the command and make the other targets depend on that one).
# This implements the solution proposed by
#    http://cmake.3232098.n2.nabble.com/How-to-have-generated-source-compiled-multiple-ways-td7585202.html
add_custom_target(
    serialize_protobuf_and_grpc_generation
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.grpc.pb.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.grpc.pb.h
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.pb.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/network/generated/opossum.pb.h
)

# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
    operators/validate.cpp
    operators/abstract_join_operator.cpp
    operators/projection.hpp
    operators/export_binary.hpp
    operators/commit_records.hpp
    operators/aggregate.hpp
    operators/csv_writer.hpp
    operators/import_binary.cpp
    operators/print.cpp
    operators/union_all.hpp
    operators/join_nested_loop_b.hpp
    operators/index_column_scan.hpp
    operators/csv.hpp
    operators/delete.hpp
    operators/insert.hpp
    operators/import_binary.hpp
    operators/abstract_read_only_operator.hpp
    operators/update.hpp
    operators/aggregate.cpp
    operators/table_wrapper.hpp
    operators/validate.hpp
    operators/join_hash.hpp
    operators/export_csv.cpp
    operators/index_column_scan.cpp
    operators/commit_records.cpp
    operators/export_binary.cpp
    operators/get_table.hpp
    operators/abstract_operator.hpp
    operators/difference.hpp
    operators/rollback_records.hpp
    operators/join_hash.cpp
    operators/abstract_operator.cpp
    operators/term.hpp
    operators/difference.cpp
    operators/product.hpp
    operators/delete.cpp
    operators/projection.cpp
    operators/sort.cpp
    operators/termfactory.hpp
    operators/get_table.cpp
    operators/sort.hpp
    operators/insert.cpp
    operators/table_scan.cpp
    operators/join_nested_loop_b.cpp
    operators/product.cpp
    operators/rollback_records.cpp
    operators/table_scan.hpp
    operators/import_csv.cpp
    operators/join_nested_loop_a.cpp
    operators/abstract_read_write_operator.hpp
    operators/join_nested_loop_a.hpp
    operators/abstract_join_operator.hpp
    operators/table_wrapper.cpp
    operators/print.hpp
    operators/import_csv.hpp
    operators/union_all.cpp
    operators/update.cpp
    operators/export_csv.hpp
    operators/csv_writer.cpp
    types.cpp
    scheduler/abstract_scheduler.hpp
    scheduler/worker.hpp
    scheduler/current_scheduler.hpp
    scheduler/abstract_task.cpp
    scheduler/worker.cpp
    scheduler/abstract_scheduler.cpp
    scheduler/current_scheduler.cpp
    scheduler/abstract_task.hpp
    scheduler/node_queue_scheduler.hpp
    scheduler/job_task.cpp
    scheduler/processing_unit.hpp
    scheduler/topology.hpp
    scheduler/node_queue_scheduler.cpp
    scheduler/operator_task.cpp
    scheduler/task_queue.cpp
    scheduler/processing_unit.cpp
    scheduler/job_task.hpp
    scheduler/topology.cpp
    scheduler/task_queue.hpp
    scheduler/operator_task.hpp
    import_export/binary.hpp
    types.hpp
    network/server_configuration.hpp
    network/server.hpp
    network/response_builder.hpp
    network/server.cpp
    network/request_handler.cpp
    network/request_handler.hpp
    network/operator_translator.hpp
    network/operator_translator.cpp
    network/generated/opossum.pb.cc
    network/generated/opossum.grpc.pb.cc
    common.hpp
    uid_allocator.hpp
    concurrency/transaction_context.cpp
    concurrency/commit_context.hpp
    concurrency/transaction_context.hpp
    concurrency/transaction_manager.hpp
    concurrency/transaction_manager.cpp
    concurrency/commit_context.cpp
    storage/storage_manager.cpp
    storage/chunk.hpp
    storage/dictionary_column.hpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.cpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_index.cpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.hpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp
    storage/index/group_key/variable_length_key_proxy.cpp
    storage/index/group_key/variable_length_key.cpp
    storage/index/group_key/variable_length_key.hpp
    storage/index/group_key/variable_length_key_store.hpp
    storage/index/group_key/variable_length_key_proxy.hpp
    storage/index/group_key/variable_length_key_base.cpp
    storage/index/group_key/composite_group_key_index.cpp
    storage/index/group_key/composite_group_key_index.hpp
    storage/index/group_key/variable_length_key_base.hpp
    storage/index/group_key/variable_length_key_store.cpp
    storage/index/group_key/group_key_index.hpp
    storage/index/base_index.hpp
    storage/chunk.cpp
    storage/untyped_dictionary_column.hpp
    storage/reference_column.hpp
    storage/table.hpp
    storage/base_attribute_vector.hpp
    storage/column_visitable.hpp
    storage/base_column.hpp
    storage/table.cpp
    storage/reference_column.cpp
    storage/value_column.hpp
    storage/storage_manager.hpp
    storage/fitted_attribute_vector.hpp
    utils/murmur_hash.cpp
    utils/murmur_hash.hpp
    utils/cuckoo_hashtable.hpp
    type_comparison.hpp
)

set(
    LIBRARIES
    protobuf
    grpc++
    grpc
    z
    tbb
)


# Configure the regular opossum library used for tests/server/playground...
add_library(opossum STATIC ${SOURCES})
target_link_libraries(opossum ${LIBRARIES})
add_dependencies(opossum serialize_protobuf_and_grpc_generation)

# Configure the lib used for asan
add_library(opossumAsan STATIC ${SOURCES})
target_link_libraries(
    opossumAsan
    ${LIBRARIES}
    -lgcov
    -fsanitize=address
)
set_target_properties(opossumAsan PROPERTIES COMPILE_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
add_dependencies(opossumAsan serialize_protobuf_and_grpc_generation)

# Configure the lib used for coverage
add_library(opossumCoverage STATIC ${SOURCES})
target_link_libraries(
    opossumCoverage
    ${LIBRARIES}
    -lgcov
    --coverage
)
set_target_properties(opossumCoverage PROPERTIES COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
add_dependencies(opossumCoverage serialize_protobuf_and_grpc_generation)
