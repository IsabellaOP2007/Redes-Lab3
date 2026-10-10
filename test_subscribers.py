"""Pruebas preliminares de recepción; no simula el protocolo de suscripción."""
import pathlib
import socket
import subprocess
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parent
BIN = ROOT / 'build'

def tcp_case(parts, expected, code):
    with socket.socket() as server:
        server.bind(('127.0.0.1', 0))
        server.listen()
        def send():
            with server.accept()[0] as peer:
                for part in parts:
                    peer.sendall(part)
                    time.sleep(0.03)
        worker = threading.Thread(target=send)
        worker.start()
        result = subprocess.run([str(BIN / 'subscriber_tcp'), '127.0.0.1', str(server.getsockname()[1])], capture_output=True, timeout=5)
        worker.join()
        assert result.stdout == expected, result.stdout
        assert result.returncode == code, result.stderr

tcp_case([b'Go', b'l uno\nGol dos\nGo', b'l tres\n'], b'Gol uno\nGol dos\nGol tres\n', 0)
tcp_case([b'incompleto'], b'', 1)
tcp_case([b'x' * 8193], b'', 1)
for name in ('subscriber_tcp', 'subscriber_udp'):
    args = ['127.0.0.1', '80abc'] if name.endswith('tcp') else ['80abc']
    assert subprocess.run([str(BIN / name), *args], capture_output=True).returncode == 1
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
    # Puerto efímero reservado temporalmente; el receptor debe tomarlo después.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reserve:
        reserve.bind(('127.0.0.1', 0))
        port = reserve.getsockname()[1]
    proc = subprocess.Popen([str(BIN / 'subscriber_udp'), str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert b'Escuchando' in proc.stderr.readline()
        for data in (b'Gol uno\n', b'Gol dos', b''):
            sender.sendto(data, ('127.0.0.1', port))
        time.sleep(0.1)
        proc.terminate()
        out, _ = proc.communicate(timeout=3)
        assert out == b'Gol uno\nGol dos\n\n', out
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
print('OK: TCP fragmentado y agrupado, EOF incompleto, límite, puertos inválidos y 3 datagramas UDP.')
