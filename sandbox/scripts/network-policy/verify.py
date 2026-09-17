import json, socket, struct, subprocess, threading, time, pathlib, os

def run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout
clients = json.loads(pathlib.Path('/fixture/clients.json').read_text())
for ip in [*clients.values(), '10.42.0.10']:
    run('ip', 'addr', 'add', ip + '/32', 'dev', 'lo')
pathlib.Path('/tmp/threat-hosts').write_text('0.0.0.0 malware.test\n')

def dns_server(ip, answer):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip, 53))
    while True:
        data, peer = sock.recvfrom(4096)
        reply = data[:2] + b'\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00' + data[12:] + b'\xc0\x0c\x00\x01\x00\x01\x00\x00\x00<\x00\x04' + socket.inet_aton(answer)
        sock.sendto(reply, peer)
for upstream, answer in [('127.0.0.2', '10.42.0.11'), ('127.0.0.3', '10.42.0.12')]:
    threading.Thread(target=dns_server, args=(upstream, answer), daemon=True).start()
run('envoy', '--mode', 'validate', '-c', '/fixture/envoy.json')
logs = open('/tmp/network-process.log', 'w+')
dns = subprocess.Popen(['coredns', '-conf', '/fixture/Corefile'], stdout=logs, stderr=logs)
envoy = subprocess.Popen(['envoy', '-c', '/fixture/envoy.json', '--concurrency', '1'], stdout=logs, stderr=logs)
try:
    time.sleep(2)
    assert dns.poll() is None and envoy.poll() is None
    query = b'\x124\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x08hardware\x04test\x00\x00\x01\x00\x01'
    for _ in range(2):
        for client, expected in [('allowed', '10.42.0.11'), ('blocked', '10.42.0.12')]:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(3)
            s.bind((clients[client], 0))
            s.sendto(query, ('127.0.0.1', 15053))
            answer = s.recv(4096)
            s.close()
            assert socket.inet_ntoa(answer[-4:]) == expected, (client, answer)
    print('PASS: per-VM DNS responses and caches are isolated', flush=True)

    def serve():
        s = socket.socket()
        s.bind(('10.42.0.10', 18080))
        s.listen()
        while True:
            c, _ = s.accept()
            c.recv(4096)
            c.sendall(b'HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nOK')
            c.close()
    threading.Thread(target=serve, daemon=True).start()
    for client, port in [('allowed', '15001'), ('blocked', '15002')]:
        run('iptables', '-t', 'nat', '-A', 'OUTPUT', '-s', clients[client], '-d', '10.42.0.10', '-p', 'tcp', '--dport', '18080', '-j', 'REDIRECT', '--to-ports', port)
    # UID matching substitutes for the production service-cgroup match in this isolated namespace.
    run('iptables', '-A', 'OUTPUT', '-m', 'conntrack', '--ctstate', 'ESTABLISHED,RELATED', '-j', 'ACCEPT')
    run('iptables', '-A', 'OUTPUT', '-d', '10.42.0.10', '-m', 'owner', '--uid-owner', '0', '-m', 'mark', '--mark', '16384', '-j', 'ACCEPT')
    run('iptables', '-A', 'OUTPUT', '-d', '10.42.0.10', '-m', 'owner', '--uid-owner', '0', '-j', 'REJECT')
    for client in ['allowed', 'blocked', 'allowed']:
        code = "import socket; s=socket.socket(); s.settimeout(3); s.bind((%r,0)); s.connect(('10.42.0.10',18080)); s.sendall(b'GET / HTTP/1.0\\r\\n\\r\\n'); print(s.recv(4096))" % clients[client]
        result = subprocess.run(['python3', '-c', code], capture_output=True, text=True, user=1001)
        assert ('200 OK' in result.stdout) == (client == 'allowed'), (client, result.stdout, result.stderr)
    print('PASS: LAN opt-in works through marked Envoy sockets; neighboring VM remains blocked', flush=True)
finally:
    dns.terminate()
    envoy.terminate()
    dns.wait()
    envoy.wait()
    logs.seek(0)
    print(logs.read()[-4000:])
