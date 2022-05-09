WITH balances_data AS 
(
	SELECT "Account Number" "ACCOUNT_NUMBER", "Account Description" "ACCOUNT_DESCRIPTION", "Source" "SOURCE", "date-month" "DATE_MONTH"
			, "Amount - Trial Balances"  AMOUNT_TRIAL_BALANCES
	FROM {{source('SAPHINEIA','SAPHINEIA_ASENZYA_SAP_BALANCES')}}--"PUBLIC".SAPHINEIA_ASENZYA_SAP_BALANCES
		
)
SELECT * FROM balances_data