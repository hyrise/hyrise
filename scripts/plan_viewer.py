#!/usr/local/bin/python3

import glob
import http.server
import re
import socket
import socketserver

class Handler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self.send_response(200)

        if self.path == '/':
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            output = ''
            output += '<html><head><title>svg viewer</title></head>'
            output += '<body style="height: 100%">'
            output += '<div style="width: 10%; float: left">'
            for svg in sorted(glob.glob('*.svg')):
                output += '<a href="#" onclick="document.getElementsByTagName(\'img\')[0].src=\'./' + svg + '\';">' + svg + '</a><br />'
            output += '</div>'
            output += '<img src="#" style="max-width: 90%; max-height: 100%; border: solid 10px #000" />'
            output += '</body></html>'

            self.wfile.write(output.encode())

        elif re.match(r'\/[A-Za-z0-9\-_.]+\.svg', self.path):
            self.send_header('Content-type', 'image/svg+xml')
            self.end_headers()

            file = open('.' + self.path, 'rb')
            self.wfile.write(file.read())

        elif self.path == '/favicon.ico':
            pass

        else:
            print('Unhandled: %s' , self.path)


with socketserver.TCPServer(('', 0), Handler) as httpd:
    hostname = socket.getfqdn().replace('.de.de', '.de')  # HPI configuration is weird and adds .de twice
    print('Server started, visit http://' + hostname + ':' + str(httpd.server_address[1]))
    httpd.serve_forever()