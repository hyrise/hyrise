#!/usr/bin/env python3

import pexpect
import re

from hyriseBenchmarkCore import initialize


def main():
    build_dir = initialize()

    arguments = {}
    arguments["--benchmark_data"] = "tpc-h:0.01"

    benchmark = pexpect.spawn(f"{build_dir}/hyriseServer --benchmark_data=tpc-h:0.01 -p 0", timeout=10)

    benchmark.expect_exact("Loading/Generating tables", timeout=120)
    benchmark.expect_exact("Encoding 'lineitem'", timeout=120)
    search_regex = r"Server started at 0.0.0.0 and port (\d+)"
    benchmark.expect(search_regex, timeout=120)

    server_port = int(re.search(search_regex, str(benchmark.after)).group(1))
    client = pexpect.spawn(f"psql -h localhost -p {server_port}", timeout=10)

    client.sendline("select count(*) from region;")
    client.expect_exact("COUNT(*)")
    client.expect_exact("5")
    client.expect_exact("(1 row)")

    # Not using close_benchmark() here, as a server is started and a timeout of None would wait forever
    client.close()
    benchmark.close()


if __name__ == "__main__":
    main()
