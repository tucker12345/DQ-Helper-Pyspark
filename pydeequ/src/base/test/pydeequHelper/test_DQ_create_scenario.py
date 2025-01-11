import os # Set the SPARK_VERSION environment variable 
os.environ["SPARK_VERSION"] = "3.3.2"

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), r'C:\Users\tkoik\OneDrive\Documents\git\DQ-Helper-Pyspark\DQ-Helper-Pyspark\pydeequ\src\base\main')))

import pytest
import json
from pydeequ.checks import Check, CheckLevel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pydeequ.verification import VerificationResult
from pyspark.sql import Row

from pyspark.sql import SparkSession


# spark = SparkSession.builder.master("local").appName("PyDeequTest").config("spark.hadoop.home.dir", "C:/hadoop").config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow").getOrCreate()
from pydeequHelper.DQ_create_scenario import DQ_create_scenario

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("PyDeequTest")
        .config("spark.hadoop.home.dir", "C:/hadoop")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )
    return spark
    # yield spark
    # spark.stop()


# @pytest.mark.parametrize("json",                             
#     [
#     *[{"constraint_suggestions": [
#             {
#                 "column_name": "test_column",
#                 "suggesting_rule": x,
#                 "code_for_constraint": "areComplete(['test_column'])",
#                 "description": "'test_column' is not null"
#             }
#         ]} for x in range("RetainCompletenessRule", "CategoricalRangeRule", "CompleteIfCompleteRule")],
#     ]                         
# )     
def test_create_checks_from_json(spark):
    
    
    # spark = SparkSession.builder.master("local").appName("PyDeequTest").config("spark.hadoop.home.dir", "C:/hadoop").config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow").getOrCreate()
    
    # mock_json = {
    #     "constraint_suggestions": [
    #         {
    #             "column_name": "test_column",
    #             "suggesting_rule": "CompleteIfCompleteRule",
    #             "code_for_constraint": "areComplete(['test_column'])",
    #             "description": "'test_column' is not null"
    #         }
    #     ]
    # }
    
                   
    
    mock_json = [
        {
            "constraint_suggestions": [
                {
                    "column_name": "test_column",
                    "suggesting_rule": x,
                    "code_for_constraint": "areComplete(['test_column'])",
                    "description": "'test_column' is not null"
                }
            ]
        } for x in ["RetainCompletenessRule", "CategoricalRangeRule", "CompleteIfCompleteRule"]
    ]
    
    for json_data in mock_json:
        checks = DQ_create_scenario().create_checks_from_json(json_data,spark)
        assert len(checks) ==  0  


# test_create_checks_from_json()
    
    # Verify that report is updated
    # updated_count = DQ_create_scenario.report.count()
    # assert updated_count > initial_count
    # assert DQ_create_scenario.report.filter(DQ_create_scenario.report["column_name"] == "test_column").count() > 0