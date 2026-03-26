#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from etf_factor.cli import main

if __name__ == '__main__':
    sys.argv = ['init_factor_db.py', 'init-db', *sys.argv[1:]]
    main()
