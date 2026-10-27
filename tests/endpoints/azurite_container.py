"""Create a blob container on Azurite: `azurite_container.py <port> <name>`.

Azurite refuses anonymous container creation, so this signs the request with
the emulator's well-known account key. Retries while the emulator starts.
"""

import base64
import email.utils
import hashlib
import hmac
import http.client
import sys
import time

ACCOUNT = "devstoreaccount1"
KEY = base64.b64decode(
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


def create(port: str, name: str) -> None:
    date = email.utils.formatdate(usegmt=True)
    signed = f"PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:2021-08-06\n/{ACCOUNT}/{ACCOUNT}/{name}\nrestype:container"
    signature = base64.b64encode(hmac.new(KEY, signed.encode(), hashlib.sha256).digest()).decode()
    connection = http.client.HTTPConnection("127.0.0.1", int(port), timeout=5)
    headers = {"x-ms-date": date, "x-ms-version": "2021-08-06", "Authorization": f"SharedKey {ACCOUNT}:{signature}"}
    connection.request("PUT", f"/{ACCOUNT}/{name}?restype=container", body=b"", headers=headers)
    response = connection.getresponse()
    if response.status != 201:
        raise RuntimeError(f"HTTP {response.status}: {response.read().decode()[:300]}")


if __name__ == "__main__":
    for attempt in range(30):
        try:
            create(*sys.argv[1:3])
            break
        except ConnectionError:
            if attempt == 29:
                raise
            time.sleep(1)
