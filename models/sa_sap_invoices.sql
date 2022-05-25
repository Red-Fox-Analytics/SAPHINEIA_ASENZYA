with invoices_count as 
(
	select count(*) as NO_OF_ORDERS FROM {{source('SAPHINEIA','SAPHINEIA_ASENZYA_SAP_INVOICES')}}

),
invoices_data AS
(
	SELECT case when "Ownership" is null then 'Not Coded' else "Ownership" end "CUSTOMER_(PARENT)" 
			, case when "Category" is null then 'Not Coded' else "Category" end CATEGORY
			, case when cast("year" as varchar) is null then 'Not Coded' else cast("year" as varchar) end "YEAR"
			, case when cast("Inv No" as varchar) is null then 'Not Coded' else cast("Inv No" as varchar) end "INV_NO" , "Day of Week" "DAY_OF_WEEK"
			, case when cast("item_wgt" as varchar) is null then 'Not Coded' else cast("item_wgt" as varchar) end "ITEM_WEIGHT", "Table Names-1"  "TABLE_NAMES_1"
			, case when "Item Description" is null then 'Not Coded' else "Item Description" end "ITEM_DESCRIPTION"
			, "Source" "SOURCE"
			, case when "Segment" is null then 'Not Coded' else "Segment"  end "SEGMENT"
			, "Table Names" "TABLE_NAMES", "Ownership ID" "CUSTOMER_ID(PARENT)"
			, "date-month" "DATE_MONTH", "state" "STATE"	
			, case when cast("Cust No" as varchar) is null then 'Not Coded' else cast("Cust No" as varchar) end "CUSTOMER_(CHILD)_ID", "Recipe ID" "RECIPE_ID" 
			, case when "Customer Name" is null then 'Not Coded' else "Customer Name" end "CUSTOMER_(CHILD)", "Part Type"  "PART_TYPE", "File Paths" "FILE_PATHS", "Item_wgt class" "ITEM_WEIGHT_CLASS"
			, "Ship Qty" "SHIP QTY", "Date" "DATE", UOM "UOM"
			, case when cast("Item No" as varchar) is null then 'Not Coded' else cast("Item No" as varchar) end  "ITEM_NO"
			, "month" "MONTH"
			, case when "location" is null then 'Not Coded' else "location" end "LOCATION"
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
	
	