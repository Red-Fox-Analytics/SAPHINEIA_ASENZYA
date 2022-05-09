WITH invoices_data AS
(
	SELECT "Ownership" "OWNERSHIP" , "Category" CATEGORY, "year" "YEAR", "Inv No" "INV_NO" , "Day of Week" "DAY_OF_WEEK"
			, "item_wgt" "ITEM_WEIGHT", "Table Names-1"  "TABLE_NAMES_1", "Item Description" "ITEM_DESCRIPTION"
			, "Source" "SOURCE", "Segment" "SEGMENT", "Table Names" "TABLE_NAMES", "Ownership ID" "OWNERSHIP_ID"
			, "date-month" "DATE_MONTH", "state" "STATE", "Cust No" "CUST_NO", "Recipe ID" "RECIPE_ID" 
			, "Customer Name" "CUSTOMER_NAME", "Part Type"  "PART_TYPE", "File Paths" "FILE_PATHS", "Item_wgt class" "ITEM_WEIGHT_CLASS"
			, "Ship Qty" "SHIP QTY", "Date" "DATE", UOM "UOM", "Item No" "ITEM_NO", "month" "MONTH", "location" "LOCATION"
			
			, "Volume Sales (LBs)"  "VOLUME_SALES_LB", "No of Orders" "NO_OF_ORDERS", "Dollar Sales"  "DOLLAR_SALES"
			, "Price/Unit" "PRICE_UNIT", "Cost/LB" "COST_LB", "Price/LB" "PRICE_LB", "Cost/Unit" "COST_UNIT"
			
	FROM {{source('SAPHINEIA','SAPHINEIA_ASENZYA_SAP_INVOICES')}}--"PUBLIC".SAPHINEIA_ASENZYA_SAP_INVOICES
)
SELECT * FROM invoices_data