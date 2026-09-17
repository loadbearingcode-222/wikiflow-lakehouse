"""Make the CI scripts under .github/scripts importable from the unit tests."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / ".github" / "scripts"))
