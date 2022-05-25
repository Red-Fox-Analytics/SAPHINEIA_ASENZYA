with invoices_count as 
(
	select count(*) as NO_OF_ORDERS FROM {{source('SAPHINEIA','SAPHINEIA_ASENZYA_SAP_INVOICES')}}

),
invoices_data AS
(
	SELECT case when "Ownership" is null then 'Not Coded' else "Ownership" end "CUSTOMER_(PARENT)" 
			, case when "Category" is null then 'Not Coded' else "Category" end CATEGORY
			, "year" "YEAR", "Inv No" "INV_NO" , "Day of Week" "DAY_OF_WEEK"
			, "item_wgt" "ITEM_WEIGHT", "Table Names-1"  "TABLE_NAMES_1"
			, case when "Item Description" is null then 'Not Coded' else "Item Description" end "ITEM_DESCRIPTION"
			, "Source" "SOURCE"
			, case when "Segment" is null then 'Not Coded' else "Segment"  end "SEGMENT"
			, "Table Names" "TABLE_NAMES", "Ownership ID" "CUSTOMER_ID(PARENT)"
			, "date-month" "DATE_MONTH", "state" "STATE"	, "Cust No" "CUSTOMER_(CHILD)_ID", "Recipe ID" "RECIPE_ID" 
			, "Customer Name" "CUSTOMER_(CHILD)", "Part Type"  "PART_TYPE", "File Paths" "FILE_PATHS", "Item_wgt class" "ITEM_WEIGHT_CLASS"
			, "Ship Qty" "SHIP QTY", "Date" "DATE", UOM "UOM"
			, case when cast("Item No" as varchar) is null then 'Not Coded' else cast("Item No" as varchar) end  "ITEM_NO"
			, "month" "MONTH"
			, "location" "LOCATION"
			, (Select NO_OF_ORDERS from invoices_count) NO_OF_ORDERS
			
			, case when "Volume Sales (LBs)" is null then 0 else "Volume Sales (LBs)" end "VOLUME_SALES_LB"
			, case when  "Dollar Sales" is null then 0 else "Dollar Sales" end  "DOLLAR_SALES"
			, case when "Price/Unit" is null then 0 else "Price/Unit" end "PRICE_UNIT"
			, case when "Cost/LB" is null then 0 else "Cost/LB" end "COST_LB"
			, case when "Price/LB" is null then 0 else "Price/LB" end "PRICE_LB"
			, case when "Cost/Unit" is null then 0 else "Cost/Unit" end "COST_UNIT"
			
	FROM {{source('SAPHINEIA','SAPHINEIA_ASENZYA_SAP_INVOICES')}} --"PUBLIC".SAPHINEIA_ASENZYA_SAP_INVOICES
)
SELECT * FROM invoices_data
	
	