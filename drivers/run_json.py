# import pytest
# print("\n=== ✅ Running JSON-Config-Based Feedback Tests ===\n")
# pytest.main(["-v", "tests/test_from_config.py"])
import os
import pytest

# Remove previous log
log_path = "tests/test_report.log"
if os.path.exists(log_path):
    os.remove(log_path)

# Run without verbose reporting (no `::test_case` lines)
pytest.main([
    "tests/test_from_config.py",
    "-v",             # quiet mode, no test names
    "--tb=short",     # minimal traceback (won't show since we suppress)
    "--disable-warnings"
])

