 WITH orders_data AS 
(
	SELECT "item number" "ITEM_NUMBER", "start time" START_TIME, "end_time" END_TIME, "part_type" PART_TYPE, "source" "SOURCE"
			, "machine number" MACHINE_NUMBER, "site" SITE, UOM UOM, "comp_date" COMP_DATE, "work order number" WORK_ORDER_NUMBER
			, "part_desc" PART_DESC, "run_date" RUN_DATE 
			, "quantity" QUANTITY, "volume sales (lbs)" VOLUME_SALES_LBS, "run time, minutes" RUN_TIME_MINUTES, "part weight" PART_WEIGHT
			, "total time" TOTAL_TIME 
	FROM {{source('SAPHINEIA','SAPHINEIA_ASENZYA_SAP_WORK_ORDERS')}}--"PUBLIC".SAPHINEIA_ASENZYA_SAP_WORK_ORDERS
)
SELECT * FROM orders_data