class DQ_run():
    @staticmethod
    def Run(_DQ_scenario,_DQScenarioJson,_df):
        try:
            # checks =  DQ_tran_silver_scenario.create_checks_from_json(_DQScenarioJson)
            checks = _DQ_scenario.create_checks_from_json(_DQScenarioJson)

            verification_suite = VerificationSuite(spark).onData(_df)
            for check in checks:
                verification_suite = verification_suite.addCheck(check)  # Add checks one by one
                
            verificationResult = verification_suite.run()

            global checkResult_df 
            checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
            
            if verificationResult.status != "Success":
                print("Data quality check failed!")
                print(verificationResult.checkResults)
            else:
                print("Data quality check success!")
            
            return verificationResult.status 
        
            pass
        except Exception as e:
            # Code that runs if the exception is caught
            print(f"An error occurred: {e}")