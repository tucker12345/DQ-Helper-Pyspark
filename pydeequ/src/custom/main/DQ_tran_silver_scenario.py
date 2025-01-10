class DQ_tran_silver_scenario(DQ_create_scenario):
    
    
    
    def __append_data(_column_name,_suggesting_rule,_code_for_constraint,_description,_completed):
        return [
            Row(column_name=_column_name, suggesting_rule=_suggesting_rule,code_for_constraint=_code_for_constraint,description=_description,completed=_completed,how="c")
    ]

    @classmethod
    def create_checks_from_json(cls,_json_constraints):
 
        checks = super().create_checks_from_json(_json_constraints)

        # fix completeness
        check = Check(spark, CheckLevel.Warning, f"CompleteIfCompleteRule(). Check on quantity. 'quantity' is not null")
        check.areComplete(["quantity"])
        cls.report = cls.report.union(spark.createDataFrame(cls.__append_data("quantity","CompleteRule()","areComplete(['quantity'])","'quantity' is not null",1), cls.schema))
        checks.append(check)

        check = Check(spark, CheckLevel.Error, f"CompleteIfCompleteRule(). Check on d_date. 'd_date' is not null")
        check.areComplete(["d_date"])
        cls.report = cls.report.union(spark.createDataFrame(cls.__append_data("d_date","CompleteRule()","areComplete(['d_date'])","'d_date' is not null",1), cls.schema))
        checks.append(check)

        check = Check(spark, CheckLevel.Warning, f"CompleteIfCompleteRule(). Check on date. 'date' is not null")
        check.areComplete(["date"])
        cls.report = cls.report.union(spark.createDataFrame(cls.__append_data("date","CompleteRule()","areComplete(['date'])","'date' is not null",1), cls.schema))
        checks.append(check)

        check = Check(spark, CheckLevel.Warning, f"CompleteIfCompleteRule(). Check on item_sk. 'item_sk' is not null")
        check.areComplete(["item_sk"])
        cls.report = cls.report.union(spark.createDataFrame(cls.__append_data("item_sk","CompleteRule()","areComplete(['item_sk'])","'item_sk' is not null",1), cls.schema))
        checks.append(check)

        # Add custom rules
        check = Check(spark, CheckLevel.Error, f"UniqueRule(). Check on multiple keys. 'd_date,`time`,item_sk,customer_sk,cdemo_sk,hdemo_sk,addr_sk,promo_sk' is unique")
        check.hasUniqueness(["d_date","time","item_sk","customer_sk","cdemo_sk","hdemo_sk","addr_sk","promo_sk"],lambda x: x == 1)
        cls.report = cls.report.union(spark.createDataFrame(cls.__append_data("multiple keys","UniqueRule()","hasUniqueness(['d_date,time','item_sk','customer_sk','cdemo_sk','hdemo_sk','addr_sk','promo_sk'])","multiple keys is unique",1), cls.schema))
        checks.append(check)

        check = Check(spark, CheckLevel.Warning, f"CategoricalRangeRule(). Check on web_name. 'web_name' has value range ")
        check.isContainedIn("web_name", ["site_0", "site_1", "site_5", "site_4", "site_3", "site_2"])
        cls.report = cls.report.union(spark.createDataFrame(cls.__append_data("web_name","ComplianceConstraint()","isContainedIn(['site_0', 'site_1', 'site_5', 'site_4', 'site_3', 'site_2'])","'web_name' has range of 6 values",1), cls.schema))
        checks.append(check)

        return checks