import pytest
import json
from pydeequ.checks import Check, CheckLevel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pydeequ.verification import VerificationResult
from pyspark.sql import Row


from pydeequHelper.DQ_create_scenario import DQ_create_scenario





