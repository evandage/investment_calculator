from __future__ import annotations

import os
import pathlib
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: serve_frontend_dist.py PORT")

    port = int(sys.argv[1])
    root = pathlib.Path(__file__).resolve().parents[1] / "frontend" / "dist"
    if not root.exists():
        raise SystemExit(f"missing dist directory: {root}")

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(root), **kwargs)

        def do_GET(self):
            path = self.translate_path(self.path)
            if os.path.isdir(path):
                path = os.path.join(path, "index.html")
            if not os.path.exists(path):
                self.path = "/index.html"
            return super().do_GET()

        def log_message(self, format, *args):
            sys.stdout.write("%s\n" % (format % args))

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Serving {root} on port {port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
