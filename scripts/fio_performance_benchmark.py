import os
import time
from datetime import date
import subprocess

# we use MiB instead of MB to easier calculate block-size aligned file offsets
# this is needed for DIRECT_IO (e.g. for io_uring or libaio)
MiB = pow(2,20)

thread_range = [1, 2, 4, 8, 16, 32, 64]
io_types = ["randread"]
filesizes = ["100M", "1000M"]

async_io_io_depth = 16
ioengine_configs = [
    # ('io_engine', 'additional parameters')
    ("sync", ""),
    ("psync", ""),
    ("mmap", ""),
    ("io_uring", f"--iodepth={async_io_io_depth}"),
    ("libaio", f"--direct=1 --iodepth={async_io_io_depth}"), #libaio needs direct_io
    ("posixaio", f"--iodepth={async_io_io_depth}"),
]

num_repetitions = 10

# fio --minimal hardcoded positions
fio_total_io_pos = 5
fio_bandwidth = 6
fio_runtime_under_test = 8
fio_bandwidth_mean = 44

kernel_version = os.uname()[2]
today = date.today()
columns = (
    "name,iterations,real_time,cpu_time,time_unit,bytes_per_second,items_per_second,label,error_occurred,"
    "error_message"
)
f = open(f"""fio_benchmark_{kernel_version}_{today.strftime("%y-%m-%d")}_{time.strftime("%H-%M-%S")}_fio.csv""", "w+")
f.write(columns + "\n")


def run_and_write_command(run, command, fio_type_offset, fio_size, numjobs, io_engine):
    os.system("sleep 2")  # Give time to finish inflight IOs
    output = subprocess.check_output(command, shell=True)
    if "write" in run:
        fio_type_offset = 41
    # fio is called with --group_reporting. This means that all
    # statistics are group for different jobs.
    split_output = output.split(b";")
    total_io = float(split_output[fio_type_offset + fio_total_io_pos].decode("utf-8"))
    bandwidth = float(split_output[fio_type_offset + fio_bandwidth].decode("utf-8"))
    runtime = float(split_output[fio_type_offset + fio_runtime_under_test].decode("utf-8"))
    bandwidth_mean = float(split_output[fio_type_offset + fio_bandwidth_mean].decode("utf-8"))
    result = (
        f'"FileIOMicroBenchmarkFixture/FIO_{io_engine}_{run}/{fio_size[:-1]}/{numjobs}/",{num_repetitions},{str(runtime * 1000)},{str(runtime * 1000)},ns,{str(bandwidth * 1000)},,,,\n'
        ""
    )
    f.write(result)
    f.flush()


for fio_size in filesizes:
    filesize_mib = int(fio_size[:-1]) * MiB
    for io_type in io_types:
        for io_engine_config in ioengine_configs:
            for numjobs in thread_range:
                batch_size = int(filesize_mib / numjobs)
                if numjobs == 1:
                    command = f"""sudo fio -minimal -name=fio-bandwidth --bs=4k --size={fio_size} --rw={io_type} --ioengine={io_engine_config[0]} {io_engine_config[1]} --filename=file.txt --group_reporting --refill_buffers -loops={num_repetitions}"""
                else:
                    command = f"""sudo fio -minimal -name=fio-bandwidth --bs=4k --size={fio_size} --io_size={batch_size} --rw={io_type} --ioengine={io_engine_config[0]} {io_engine_config[1]} --filename=file.txt --group_reporting --refill_buffers --numjobs={numjobs} --thread -loops={num_repetitions} --offset_increment={batch_size}"""

                fio_type_offset = 0
                print(command)
                run_and_write_command(io_type, command, fio_type_offset, fio_size, numjobs, io_engine_config[0])

f.closed
