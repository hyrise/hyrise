set(DATABASE ORACLE)
set(MACHINE LINUX)
set(WORKLOAD TPCH)

add_library(tpch_dbgen
    STATIC
    build.c
    config.h
    driver.c
    dss.h
    dsstypes.h
    tpch_dbgen.c
    bm_utils.c
    release.h
    rnd.c
    load_stub.c
    speed_seed.c
    text.c
    permute.c
    print.c
    rng64.c
    shared.h
)

target_include_directories(tpch_dbgen
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_definitions(
    tpch_dbgen
    PUBLIC
    -DDBNAME="dss"
    -D${MACHINE}
    -D${DATABASE}
    -D${WORKLOAD}
    #-DRNG_TEST
    -D_FILE_OFFSET_BITS=64
)