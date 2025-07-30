## How to Run Unit Tests in sparkMeasure

This guide explains how to run both the **Scala unit tests** (via `sbt`) and the **Python integration tests** for the `sparkMeasure` project.

---

### 1. Run Scala Unit Tests

To execute the built-in unit tests for the core sparkMeasure Scala codebase:

```bash
sbt test
````

This will compile the project and run all tests defined under `src/test/scala/`.

---

### 2. Run Python Integration Tests

#### a. Create and Activate a Python Virtual Environment

From the root of the repository:

```bash
python3 -m venv ~/venv/sparkmeasure
source ~/venv/sparkmeasure/bin/activate
```

#### b. Install Python Dependencies

```bash
pip install -r python/requirements.txt
```

#### c. Build the sparkMeasure JAR

Python tests require the JAR built from the Scala code:

```bash
sbt package
```

This generates the JAR in `target/scala-2.12/` or `target/scala-2.13/`.

#### d. Run the Python Tests

```bash
pytest python/sparkmeasure -vvv -s
```

---

### Notes

* Ensure the JAR is up-to-date and present in the expected `target/scala-*/` directory.
* Python tests require a working Spark installation (`SPARK_HOME` may need to be set).
* Scala and Python tests are independent; run both for full validation.
* Test sources:

  * Scala: `src/test/scala/`
  * Python: `python/sparkmeasure/test_*.py`