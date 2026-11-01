#!/usr/bin/env python3
"""Set PYTHONPATH to the accepted exact broker source; see docs/telegram-nightly-eval-v1.md."""
from broker.nightly_client import main

if __name__ == '__main__':
    raise SystemExit(main())
