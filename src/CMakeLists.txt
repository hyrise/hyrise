# Global flags and include directories
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -pthread -Wno-error=unused-parameter -Wall -Werror")

include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/benchmark/include
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
    ${PROJECT_SOURCE_DIR}/third_party/grpc/include
    ${PROJECT_SOURCE_DIR}/third_party/grpc/third_party/protobuf/src

    ${PROJECT_SOURCE_DIR}/src/lib/
)

link_directories(
    ${PROJECT_SOURCE_DIR}/third_party/grpc/libs/opt/
    ${PROJECT_SOURCE_DIR}/third_party/grpc/libs/opt/protobuf
)

add_subdirectory(benchmark)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(test)
