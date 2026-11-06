"""Run the live discovery test through a recording, non-mocking HTTP proxy.
Credentials stay in the environment. Only tool schemas and provider usage are saved.
"""
import argparse
import http.server
import json
import os
import subprocess
import threading
import urllib.error
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument('--model', required=True)
parser.add_argument('--upstream', required=True)
parser.add_argument('--report', required=True)
parser.add_argument('--baseline', action='store_true')
args = parser.parse_args()
requests = []

class Proxy(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        payload = json.loads(body)
        tools = payload.get('tools', [])
        record = {'model': payload.get('model'), 'tools': tools, 'instructions': payload.get('instructions'), 'input': payload.get('input', payload.get('messages'))}
        requests.append(record)
        headers = {key: value for key, value in self.headers.items()
                   if key.lower() not in ('host', 'content-length', 'connection', 'accept-encoding')}
        request = urllib.request.Request(args.upstream, data=body, headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=180)
        except urllib.error.HTTPError as error:
            response = error
        content = response.read()
        try:
            record['usage'] = json.loads(content).get('usage')
        except ValueError:
            pass
        record['status'] = response.status
        self.send_response(response.status)
        self.send_header('Content-Type', response.headers.get('Content-Type', 'application/json'))
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)

server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Proxy)
threading.Thread(target=server.serve_forever, daemon=True).start()
env = os.environ.copy()
env['CHEVALIER_DISCOVERY_MODEL'] = args.model + '@server_url=http://127.0.0.1:' + str(server.server_port)
try:
    result = subprocess.run([os.path.expanduser('~/.cargo/bin/cargo'), '+1.96.0', 'test',
                             '--test', 'tool_discovery_live', 'eager_catalog_baseline' if args.baseline else 'discovers_executes', '--', '--ignored', '--nocapture'], env=env)
finally:
    server.shutdown()
    with open(args.report, 'w') as output:
        json.dump(requests, output, indent=2)
def loaded_from(value):
    if isinstance(value, dict):
        if isinstance(value.get('loaded_tool_names'), list):
            yield value['loaded_tool_names']
        for child in value.values():
            yield from loaded_from(child)
    elif isinstance(value, list):
        for child in value:
            yield from loaded_from(child)
    elif isinstance(value, str) and value.startswith('{'):
        try:
            decoded = json.loads(value)
        except ValueError:
            return
        yield from loaded_from(decoded)

expected = {'tool_search'}
for index, record in enumerate(requests):
    names = [tool.get('name', tool.get('function', {}).get('name')) for tool in record['tools']]
    if args.baseline:
        assert set(names) == {'read_appointment_record', 'read_invoice_record'}
        continue
    if index == 0:
        assert names == ['tool_search'], f'Initial request leaked schemas: {names}'
    for loaded in loaded_from(record.get('input')):
        expected.update(loaded)
    assert set(names) <= expected, f'Undiscovered schemas sent: {set(names) - expected}'
    assert 'tool_search' in names, 'Discovery vanished from a subsequent request'
    record['schema_utf8_bytes'] = len(json.dumps(record['tools'], separators=(',', ':')).encode())
for record in requests:
    print(json.dumps({'status': record['status'], 'tools': [tool.get('name', tool.get('function', {}).get('name')) for tool in record['tools']], 'usage': record.get('usage')}))
raise SystemExit(result.returncode)
