import pytest
import ast
import student_submission.solution as sol
from pyspark.sql import SparkSession
from datetime import datetime
import os

LOG_PATH = "student_submission/test_report.log"

def log_result(func_name, status):
    try:
        with open(LOG_PATH, "a") as f:
            f.write(f"[{status.upper()}] {func_name}\n")
    except Exception as e:
        print(f"Logging failed: {e}")

def load_config(path="tests/test_config.json"):
    import json
    with open(path) as f:
        return json.load(f)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("MultiLLDTest").getOrCreate()

@pytest.fixture(scope="module")
def df(spark):
    return sol.load_transactions_data(spark, "data/input.csv")

def check_no_pass(func_name):
    tree = ast.parse(open("student_submission/solution.py").read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            for sub in ast.walk(node):
                if isinstance(sub, ast.Pass):
                    raise AssertionError(f"Anti-cheat violation: 'pass' in {func_name}()")

def pytest_generate_tests(metafunc):
    if "test_case" in metafunc.fixturenames:
        config = load_config()
        metafunc.parametrize("test_case", config)

def test_from_config(test_case, spark, df):
    func_name = test_case["function"]
    status = "pass"
    try:
        check_no_pass(func_name)
        inputs = {"df": df, "spark": spark}
        args = [inputs.get(arg, arg) for arg in test_case["args"]]
        result = getattr(sol, func_name)(*args)

        if test_case["expected"] == "DataFrame":
            assert result.count() >= 0
        elif test_case["expected"] == "list":
            assert isinstance(result, list)
        elif test_case["expected"] == "dict":
            assert isinstance(result, dict)
        elif test_case["expected"] == "int":
            assert isinstance(result, int)
    except Exception:
        status = "fail"
        raise
    finally:
        log_result(func_name, status)
        print(f"[{status.upper()}] {func_name}")
