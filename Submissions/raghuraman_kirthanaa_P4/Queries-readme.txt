Query 1:
This is a simple select query where I select the nutrient with value 205 and the food id NDB_No from the FOOD_DES table.

Query 2:
Here, I am using GROUP BY to sum the nutrient values of all nutrients in a food and finding the food with the maximum
nutrient value.

Query 3:
In this query, I am trying to reproduce Table 1 in the src28_doc.pdf. The query basically lists the nutrient no, the
name of the nutrient and the number of food items the nutrient is in. I use the COUNT() aggregation operator to find the
number of food item each nutrient appears in.

Query 4:
In this query, I first find the NDB_No of the food that contains the maximum number of nutrients by using GROUP BY to
get the aggregate count of nutrients. Then I am using another GROUP BY to get the number of nutrients in that
particular food item chosen in the sub query.

Query 5:
In this query, I first find the average nutrient value of each food item by using AVG aggregate operator using GROUP BY. Then i use this computed value to determine if this food item should be selected by comparing its average
nutrient value with that of “McDONALD'S, Hamburger”.

Another query I came up with is :
SELECT FOOD_DES.NDB_No, FOOD_DES.Long_Desc FROM FOOD_DES WHERE NDB_NO IN
(SELECT TEMP.NDB_NO FROM
(SELECT NDB_No, AVG(Nutr_Val) AS AVERAGE FROM NUT_DATA
GROUP BY NDB_No) AS TEMP
WHERE TEMP.AVERAGE > (SELECT AVG(Nutr_Val) FROM NUT_DATA
                 WHERE NUT_DATA.NDB_No = (SELECT NDB_No FROM FOOD_DES WHERE Long_Desc = 'McDONALD''S, Hamburger')));

But this query requires to create another temporary table which may not be efficient.

Query 6:
Here I am using 2 sub queries to get a list of food items that contain water and calcium. Then I use another SELECT
query to select only those food items that are present the in food items list containing water but not in the list of
food items containing calcium.

Query 7:
For this, I follow a logic similar to query 6 to find the list of foods containing both caffeine and ethyl alcohol and
display only those food items that contain both these nutrients.



