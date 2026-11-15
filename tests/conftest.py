import os
import sys

# Use the Spark that pip installed, and the same Python for the workers.
# A SPARK_HOME left over from another install would otherwise break the session.
if not os.path.isdir(os.environ.get("SPARK_HOME", "")):
    os.environ.pop("SPARK_HOME", None)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
