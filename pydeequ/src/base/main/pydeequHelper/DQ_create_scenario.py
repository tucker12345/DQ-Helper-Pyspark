import json
from pydeequ.checks import Check, CheckLevel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pydeequ.verification import VerificationResult
from pyspark.sql import Row


class DQ_create_scenario():

    schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("suggesting_rule", StringType(), True),
        StructField("code_for_constraint", StringType(), True),
        StructField("description", StringType(), True),
        StructField("completed", IntegerType(), True),
        StructField("how", StringType(), True)
    ])

    report = spark.createDataFrame([], schema)

    def __append_data(_constraint,_completed):
        return [
            Row(column_name=_constraint["column_name"], suggesting_rule=_constraint["suggesting_rule"],code_for_constraint=_constraint['code_for_constraint'],description=_constraint['description'],completed=_completed,how="a")
        ]

    @classmethod
    def create_checks_from_json(cls,_json_constraints):
        checks = []
        # global report 
        # __report = spark.createDataFrame([], schema)
        cls.report = spark.createDataFrame([], cls.schema)
        constraints = _json_constraints['constraint_suggestions']

        for constraint in constraints:
            check = Check(spark, CheckLevel.Warning, f"{constraint['suggesting_rule']}. Check on {constraint['column_name']}. {constraint['description']}")

            method_name = None
            if constraint["suggesting_rule"].find("RetainCompletenessRule") != -1:
                cls.report = cls.report.union(spark.createDataFrame(cls._append_data(constraint,0), cls.schema))
                continue
            elif constraint["suggesting_rule"].find("CategoricalRangeRule") != -1:
                cls.report = cls.report.union(spark.createDataFrame(cls._append_data(constraint,0), cls.schema))
                continue
            elif constraint["suggesting_rule"].find("CompleteIfCompleteRule") != -1:
                cls.report = cls.report.union(spark.createDataFrame(cls._append_data(constraint,0), cls.schema))
                continue
            else:
                method_name = constraint["code_for_constraint"]
                cls.report = cls.report.union(spark.createDataFrame(cls._append_data(constraint,1), cls.schema))

            if method_name:
                code_for_constraint =  f"check{method_name}"
                check = eval(code_for_constraint)
                checks.append(check)
            else:
                print(f"Constraint type {constraint['suggesting_rule']} is not currently supported")
    
        return checks
    