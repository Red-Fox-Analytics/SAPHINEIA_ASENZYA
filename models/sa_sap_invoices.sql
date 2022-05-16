with invoices_data AS
(
	SELECT "Date" "DATE","Item No" "ITEM_NO","Item Description" "ITEM_DESCRIPTION", "Recipe ID" "RECIPE_ID" ,"Part Type"  "PART_TYPE" 
			, "Customer Name" "CUSTOMER_NAME","Cust No" "CUST_NO", "Category" CATEGORY,"Ownership ID" "OWNERSHIP_ID"
			, "Ownership" "OWNERSHIP" , "Ship Qty" "SHIP QTY"
			,  "item_wgt" "ITEMS_WEIGHT", "state" "STATE", "Segment" "SEGMENT", "location" "LOCATION"
			, "Item_wgt class" "ITEM_WEIGHT_CLASS"
			--,"Inv No" "INV_NO"
			,"Price/Unit" "PRICE_UNIT", "Cost/LB" "COST_LB", "Price/LB" "PRICE_LB", "Cost/Unit" "COST_UNIT"
			
			
			, sum("Volume Sales (LBs)")  "VOLUME_SALES_LB", sum("No of Orders") "NO_OF_ORDERS", sum("Dollar Sales")  "DOLLAR_SALES"
	FROM {{source('SAPHINEIA','SAPHINEIA_ASENZYA_SAP_INVOICES')}}
	GROUP BY "Date" ,"Item No" ,"Item Description", "Recipe ID"  ,"Part Type" 
			, "Customer Name" ,"Cust No" , "Category" ,"Ownership ID" 
			, "Ownership" , "Ship Qty" 
			,  "item_wgt" ,"state", "Segment" , "location" 
			,"Item_wgt class" 
			--,"Inv No" 
		   , "Price/Unit" , "Cost/LB" , "Price/LB" , "Cost/Unit"	
	ORDER BY "Date" 
)
SELECT id.* , id1."VOLUME_SALES_LB" "VOLUME_SALES_LB_YA",id1."NO_OF_ORDERS" "NO_OF_ORDERS_YA", id1."DOLLAR_SALES" "DOLLAR_SALES_YA"
FROM invoices_data id
LEFT JOIN invoices_data id1
ON  id."DATE"=id1."DATE" AND id."ITEM_NO"=id1."ITEM_NO"  and id."ITEM_DESCRIPTION"=id1."ITEM_DESCRIPTION" and  id."RECIPE_ID"=id1."RECIPE_ID"   and id."PART_TYPE"=id1."PART_TYPE" 
			 and  id."CUSTOMER_NAME"=id1."CUSTOMER_NAME"  and id."CUST_NO"=id1."CUST_NO"  and  id.CATEGORY=id1.CATEGORY and id."OWNERSHIP_ID" =id1."OWNERSHIP_ID"
			 and  id."OWNERSHIP" =id1."OWNERSHIP" and  id."SHIP QTY" =id1."SHIP QTY"
			 and   id."ITEMS_WEIGHT"=id1."ITEMS_WEIGHT" and  id."STATE"=id1."STATE" and  id."SEGMENT"=id1."SEGMENT"  and  id."LOCATION" =id1."LOCATION"
			 and id."ITEM_WEIGHT_CLASS" =id1."ITEM_WEIGHT_CLASS"
			 --and  id."PRICE_UNIT"=id1."PRICE_UNIT"  and  id."COST_LB"=id1."COST_LB"  and  id."PRICE_LB" =id1."PRICE_LB" and  id."COST_UNIT"	 =id1."COST_UNIT"
WHERE 		 date_part('day',id."DATE")=date_part('day',id1."DATE") 
			 and date_part('month',id."DATE")=date_part('month',id1."DATE") 
	 		 and date_part('year',id."DATE")=date_part('year',id1."DATE")+1	
	