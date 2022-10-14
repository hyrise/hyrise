#!/usr/bin/env python3

import os
import pexpect
import re

from hyriseBenchmarkCore import initialize


def main():
    build_dir = initialize()

    server = pexpect.spawn(f"{build_dir}/hyriseServer --benchmark_data=tpc-h:0.01 -p 0", timeout=10)

    server.expect_exact("Loading/Generating tables", timeout=120)
    server.expect_exact("Encoding 'lineitem'", timeout=120)
    search_regex = r"Server started at 0.0.0.0 and port (\d+)"
    server.expect(search_regex, timeout=120)

    server_port = int(re.search(search_regex, str(server.after)).group(1))
    # Recent Postgres/psql versions changed the authentication behavior. Disabling an environment variable solves the
    # issue. Since hyriseServer does not implement authentication at all, this is no problem.
    # See https://github.com/psycopg/psycopg2/issues/1084#issuecomment-656778107
    environment_variables = os.environ.copy()
    environment_variables.update({"PGGSSENCMODE": "disable"})
    client = pexpect.spawn(f"psql -h localhost -p {server_port}", timeout=20, env=environment_variables)

    client.sendline("select count(*) from region;")
    client.expect_exact("COUNT(*)")
    client.expect_exact("5")
    client.expect_exact("(1 row)")

    # Not using close_benchmark() here, as a server is started and a timeout of None would wait forever.
    client.close()
    server.close()


if __name__ == "__main__":
    main()
