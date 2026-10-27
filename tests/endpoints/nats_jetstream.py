"""Create a JetStream stream and durable pull consumer:
`nats_jetstream.py <port> <stream> <subject> <consumer>`.

Speaks the NATS text protocol directly, since the image has no `nats` CLI.
"""

import json
import socket
import sys


def request(sock: socket.socket, subject: str, body: dict) -> dict:
    payload = json.dumps(body).encode()
    sock.sendall(f"PUB {subject} _INBOX.setup {len(payload)}\r\n".encode() + payload + b"\r\nPING\r\n")
    buffer = b""
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("connection closed")
        buffer += chunk
        head, sep, rest = buffer.partition(b"\r\n")
        while sep:
            if head.startswith(b"MSG "):
                size = int(head.split()[-1])
                if len(rest) < size + 2:
                    break
                reply = json.loads(rest[:size])
                if "error" in reply:
                    raise RuntimeError(f"{subject}: {reply['error']}")
                return reply
            if head.startswith(b"-ERR"):
                raise RuntimeError(head.decode())
            buffer = rest
            head, sep, rest = buffer.partition(b"\r\n")


def main(port: str, stream: str, subject: str, consumer: str) -> None:
    with socket.create_connection(("127.0.0.1", int(port)), timeout=10) as sock:
        sock.recv(4096)
        sock.sendall(b'CONNECT {"verbose":false}\r\nSUB _INBOX.setup 1\r\n')
        request(sock, f"$JS.API.STREAM.CREATE.{stream}", {"name": stream, "subjects": [subject]})
        config = {"durable_name": consumer, "ack_policy": "explicit", "deliver_policy": "all"}
        request(sock, f"$JS.API.CONSUMER.DURABLE.CREATE.{stream}.{consumer}", {"stream_name": stream, "config": config})


if __name__ == "__main__":
    main(*sys.argv[1:5])
