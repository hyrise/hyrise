#!/usr/bin/env python3

import os
import sys
import pexpect


def initialize():
    if len(sys.argv) == 1:
        print("Usage: ./scripts/test/hyriseConsole_test.py <build_dir>")
        sys.exit(1)

    if not os.path.isdir("resources/test_data/tbl"):
        print(
            "Cannot find resources/test_data/tbl. Are you running the test suite from the main folder of the Hyrise"
            "repository?"
        )
        sys.exit(1)

    build_dir = sys.argv[1]
    console = pexpect.spawn(build_dir + "/hyriseConsole", timeout=120, dimensions=(200, 64))
    return console


def close_console(console):
    console.expect(pexpect.EOF)
    console.close()


def check_exit_status(console):
    if console.exitstatus is None:
        sys.exit(console.signalstatus)


def main():
    # Disable Pagination
    if "TERM" in os.environ:
        del os.environ["TERM"]

    console = initialize()
    console.logfile = sys.stdout.buffer

    build_dir = sys.argv[1]
    lib_suffix = ""

    if sys.platform.startswith("linux"):
        lib_suffix = ".so"
    elif sys.platform.startswith("darwin"):
        lib_suffix = ".dylib"

    # Test error handling of print command
    console.sendline("print test")
    console.expect("Table does not exist in StorageManager")

    # Test load command
    console.sendline("load resources/test_data/tbl/10_ints.tbl test")
    console.expect('Loading .*tbl/10_ints.tbl into table "test"')
    console.expect('Encoding "test" using Unencoded')

    console.sendline("load resources/test_data/bin/float.bin test_bin")
    console.expect('Loading .*bin/float.bin into table "test_bin"')
    console.expect('Encoding "test_bin" using Unencoded')

    # Reload table with a specified encoding and check meta tables for applied encoding
    console.sendline("load resources/test_data/bin/float.bin test_bin RunLength")
    console.expect('Loading .*bin/float.bin into table "test_bin"')
    console.expect('Table "test_bin" already existed. Replacing it.')
    console.expect('Encoding "test_bin" using RunLength')
    console.sendline(
        "select encoding_type from meta_segments where table_name='test_bin' and chunk_id=0 and column_id=0"
    )
    console.expect("RunLength")

    # Test SQL statement
    console.sendline("select sum(a) from test")
    console.expect("786")
    console.expect("Execution info:")

    # Test transactions
    console.sendline("insert into test (a) values (17);")
    console.sendline("begin")
    console.sendline("insert into test (a) values (18);")
    console.sendline("commit; insert into test (a) values (19);")
    console.sendline("begin; insert into test (a) values (20);")
    console.sendline("select sum(a) from test")
    console.expect("860")
    console.sendline("txinfo")
    console.expect("Active transaction")
    console.sendline("insert into test (a) values (21); rollback;")
    console.sendline("select sum(a) from test")
    console.expect("840")
    console.sendline("txinfo")
    console.expect("auto-commit mode")

    # Test invalid transaction handling
    console.sendline("begin")
    console.sendline("insert into test (a) values (18);")
    console.sendline("begin")
    console.expect("Cannot begin transaction inside an active transaction.")
    console.sendline("txinfo")
    console.expect("Active transaction")
    console.sendline("rollback")
    console.expect("0 rows total")
    console.sendline("txinfo")
    console.expect("auto-commit mode")
    console.sendline("begin; delete from test; begin; insert into test (a) values (20);")
    console.sendline("txinfo")
    console.expect("Active transaction")
    console.sendline("select sum(a) from test")
    console.expect("0")
    console.sendline("rollback")
    console.sendline("select sum(a) from test")
    console.expect("840")

    # Test TPCH generation
    console.sendline("generate_tpch     0.01   7")
    console.expect("Generating tables done")

    # Test TPCH tables
    console.sendline("select * from nation")
    console.expect("25 rows total")

    # Test correct chunk size (25 nations, independent of scale factor, with a max chunk size of 7 results in 4 chunks)
    console.sendline("print nation")
    console.expect("=== Chunk 3 ===")

    # Test meta table modification
    console.sendline("insert into meta_settings values ('foo', 'bar', 'baz')")
    console.expect("Invalid input error: Cannot insert into meta_settings")
    console.sendline("select * from meta_plugins")
    console.expect("0 rows total")
    console.sendline("insert into meta_plugins values ('" + build_dir + "/lib/libhyriseTestPlugin" + lib_suffix + "')")
    console.sendline("select * from meta_plugins")
    console.expect("hyriseTestPlugin")

    # Create a transaction that is still open when the console exists. It will be rolled back.
    console.sendline("begin")

    # Test exit command
    console.sendline("exit")
    console.expect("rolled back")
    console.expect("Bye.")

    close_console(console)
    check_exit_status(console)


if __name__ == "__main__":
    main()
