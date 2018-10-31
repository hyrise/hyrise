#!/usr/bin/env python3

import os

from datetime import datetime

import requests
import urllib

def bot_sendtext(bot_message):
    encoded_message = urllib.parse.quote_plus(bot_message)
    bot_token = '606667730:AAHrpRwxIYfhhV8GrbYHGgKvGqSBVjHMZMw'
    bot_chatID = '5505853'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + encoded_message

    requests.get(send_text)

CWD = os.getcwd()
OUTPUT_FILE = 'current_run.out'
ERROR_FILE = 'current_run.err'

timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

static_parameters = [
    '--verbose',
    '--queries 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20,21,22',
    # '--cores 224 168 112 56 28 14 7 2 1 0',
    '--cores 0 1 2 7 14 28 56 84 112 140 168 196 224',
    '--runs 10000',
    '--time 60',
    '--result-dir ma-results',
]

chunk_size_50k = '50000'
chunk_size_100k = '100000'
chunk_size_max = '4294967294'

chunk_size_to_str = {
    chunk_size_50k : '50k',
    chunk_size_100k : '100k',
    chunk_size_max : 'Max',
}

variable_parameters = [
    ['--scale 1', '--clients 10', '--chunk_size ' + chunk_size_100k],

    # ['--scale 1', '--clients 10', '--chunk_size ' + chunk_size_100k],
    # ['--scale 1', '--clients 100', '--chunk_size ' + chunk_size_100k],

    # ['--scale 1', '--clients 100', '--chunk_size ' + chunk_size_max],
    # ['--scale 1', '--clients 200', '--chunk_size ' + chunk_size_max],
    # ['--scale 1', '--clients 10', '--chunk_size ' + chunk_size_50k],
    # ['--scale 1', '--clients 100', '--chunk_size ' + chunk_size_50k],
    # ['--scale 1', '--clients 200', '--chunk_size ' + chunk_size_50k],
    # ['--scale 10', '--clients 10', '--chunk_size ' + chunk_size_100k],
    # ['--scale 10', '--clients 100', '--chunk_size ' + chunk_size_100k],
    # ['--scale 10', '--clients 200', '--chunk_size ' + chunk_size_100k],

    # ['--scale 1', '--clients 10', '--chunk_size ' + chunk_size_100k],
    # ['--scale 1', '--clients 100', '--chunk_size ' + chunk_size_100k],
    # ['--scale 1', '--clients 200', '--chunk_size ' + chunk_size_100k],



    # ['--scale 10', '--clients 10', '--chunk_size ' + chunk_size_max],
    # ['--scale 10', '--clients 100', '--chunk_size ' + chunk_size_max],
]

commands = []
for vp in variable_parameters:
    params = ''.join(' '.join(vp).split('--')).split(' ')
    chunksize_str = chunk_size_to_str[params[-1]]
    params_str = params[0] + params[1] + '_' + params[2] + params[3] + '_chunksize' + chunksize_str
    resultname = 'tpch_' + params_str + '_' + timestamp
    cmd = './scripts/benchmark_multithreaded.py -e build-release/hyriseBenchmarkTPCH ' + ' '.join(static_parameters) + ' ' + ' '.join(vp) + ' ' + '--result-name ' + resultname
    commands.append((cmd, resultname))

for f in (OUTPUT_FILE, ERROR_FILE):
    if os.path.exists(f): os.remove(f)

bot_sendtext('Starting benchmarks...')

counter = 0
with open(OUTPUT_FILE, 'w') as output, open(ERROR_FILE, 'w') as error:
    for command in commands:
        cmd = command[0]
        print(cmd)
        # run(cmd, cwd=CWD, stdout=output, stderr=error, shell=True)
        os.system(cmd)
        counter += 1
        http_friendly_str = '-'.join(command[1].split('_'))
        bot_sendtext('Benchmark ' + http_friendly_str + '(' + str(counter) + ' of ' + str(len(commands)) + ')' + ' complete!')

bot_sendtext('Benchmarks complete!')
