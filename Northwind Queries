/* --------------------------- Group C Queries---------------------------------------------------*/
/*-----------------------------------CLV --------------------------------------------- */
WITH ORDERRANK AS
(SELECT *,
Dense_rank() over (partition by customerID order by orderDate asc) order_rank,
date_format(orderDate, '%Y%m') as Month_year,
lag(date_format(orderDate,'%Y%m'),1)over (partition by CustomerID) previous_order_month
FROM northwind.orders),

retention AS
(Select Month_year, count(distinct customerID) AS Totalcustomers,
Count(distinct case when PERIOD_DIFF(Month_year, previous_order_month) = 1 then CustomerID end) AS retain_customer,
lag(count(distinct CustomerID),1) over (order by Month_year) prev_month_total_customer
from ORDERRANK
group by Month_year),

churn as (Select month_year,   retain_customer*100/prev_month_total_customer as retention_rate,  
100 -(retain_customer *100/prev_month_total_customer) as churn_rate
from retention),

lifespan as (select
1/(avg(churn_rate)/100) as customer_lifespan
from churn),


total as
(SELECT
COUNT(DISTINCT c.CustomerID) as total_customers,
SUM(od.unitprice*od.quantity) as total_revenue
from customers c
right join orders o
on c.CustomerID=o.CustomerID
left join `order details` od
on o.orderID=od.orderID
),

arpa as(select
avg(total_revenue/total_customers) as ARPA
from total)

select ARPA*customer_lifespan as CLV
from arpa
join lifespan
on 1=1;

/*----------------------------Monthly Churn and Retention rates----------------------- */
WITH ORDERRANK AS
(SELECT *,
Dense_rank() over (partition by customerID order by orderDate asc) order_rank,
date_format(orderDate, '%Y%m') as Month_year,
lag(date_format(orderDate,'%Y%m'),1)over (partition by CustomerID) previous_order_month
FROM northwind.orders),

retention AS
(Select Month_year, count(distinct customerID) AS Totalcustomers,
Count(distinct case when PERIOD_DIFF(Month_year, previous_order_month) = 1 then CustomerID end) AS retain_customer,
lag(count(distinct CustomerID),1) over (order by Month_year) prev_month_total_customer
from ORDERRANK
group by Month_year)

Select month_year,   retain_customer*100/prev_month_total_customer as retention_rate,  
100 -(retain_customer *100/prev_month_total_customer) as churn_rate
from retention



/*------------------------------------------------------------ Order Lead Time-------------------------------------------- */

SELECT o.OrderID, o.shippeddate, o.orderdate, o.shipcountry, o.shipvia, s.CompanyName as shipper,
DATEDIFF(ShippedDate, OrderDate) AS order_lead_time
from orders o
left join shippers s
on o.shipvia=s.shipperID

/*------------------------------------------- Employee Performance -------------------------------- */
DELIMITER //

create procedure calculate_employee_performance ()
BEGIN
drop table employee_performance ;
create table employee_performance as (

SELECT e.EmployeeID, 
SUM(od.Quantity * od.UnitPrice) AS revenue,
count(distinct o.OrderID) as order_count,
count(distinct o.customerID) as customers_against_employee,
datediff(MAX(o.orderDate),MIN(o.orderDate)) as employee_time,
RANK() OVER (ORDER BY SUM(od.Quantity * (od.UnitPrice-od.discount)) DESC) AS _rank
from orders o
left join employees e
on o.EmployeeID=e.EmployeeID
right join `order details` od
on o.OrderID=od.OrderID
group by e.EmployeeID );
end//

/*------------------------------------------- Average Category Revenue------------------------------------- */
with cte AS
(SELECT C.CategoryID,CategoryName,count(distinct o.OrderID) AS totalorders, sum( o.UnitPrice * o.Quantity) AS Totalrevenue
FROM northwind.`order details` o
inner join products p
on p.ProductID = o.ProductID
inner join categories c
on p.CategoryID = c.CategoryID
group by c.CategoryID,CategoryName
order by Totalrevenue DESC)

SELECT categoryID,CategoryName, round(Totalrevenue/Totalorders,2) AS CategoryAverageRevenue
from cte
Order by categoryaveragerevenue desc;

/*------------------------------------- Top 5 products of each Category ----------------------------------- */

WITH ProductRevenue AS
(
    SELECT c.CategoryName,p.ProductName, SUM(od.UnitPrice * od.Quantity) AS Revenue,
    dense_rank() OVER (PARTITION BY c.CategoryName ORDER BY SUM((od.UnitPrice - od.Discount) * od.Quantity) DESC) AS RowNum
    FROM Categories c
    left JOIN Products p ON c.CategoryID = p.CategoryID
    left JOIN `Order Details` od ON p.ProductID = od.ProductID
    GROUP BY c.CategoryName, p.ProductName
)
SELECT
    CategoryName,ProductName,Revenue
FROM ProductRevenue
WHERE RowNum <= 5
ORDER BY CategoryName, Revenue DESC

/*----------------------------------------------- Inventory on Hands ------------------------------------------- */
Select  p.ProductName, p.unitsInstock,(SUM(od.Quantity)/COUNT(distinct date_format(orderdate, "%Y%m"))),
p.UnitsInStock /(SUM(od.Quantity)/COUNT(distinct date_format(orderdate, "%Y%m"))) AS MonthsOfInventoryOnHand

FROM Products p
LEFT JOIN `order details` od ON p.ProductID = od.ProductID
inner join orders o
On o.orderID = Od.OrderID
GROUP BY p.ProductName, p.UnitsInStock;

/*----------------------------------------------- Top 5 products by Revenue and Quantity Sold -------------------------- */
WITH ProductSalesCTE AS (
    SELECT
    p.ProductID,
    p.ProductName,
    SUM(od.Quantity) AS TotalQuantitySold,
    SUM((od.unitPrice - od.Discount) * od.Quantity) As TotalRevenue,
    DENSE_RANK() OVER (ORDER BY SUM(od.Quantity * (od.UnitPrice - od.Discount)) DESC) AS SalesRank
    
FROM
    Products AS p
    JOIN `Order Details` AS od ON p.ProductID = od.ProductID
GROUP BY
    p.ProductID, p.ProductName
ORDER BY
     TotalRevenue DESC
)

SELECT
    ProductID,
    ProductName,
    TotalRevenue,
    TotalQuantitySold
FROM
    ProductSalesCTE
WHERE
    SalesRank <= 5;
    
/* ------------------------------------------------- Monthly Customer Retention Rate --------------------------------------------------- */
WITH ORDERRANK AS
(SELECT *,
Dense_rank() over (partition by customerID order by orderDate asc) order_rank,
date_format(orderDate, '%Y%m') as Month_year,
lag(date_format(orderDate,'%Y%m'),1)over (partition by CustomerID) previous_order_month
FROM northwind.orders),

retention AS
(Select Month_year, count(distinct customerID) AS Totalcustomers,
Count(distinct case when PERIOD_DIFF(Month_year, previous_order_month) = 1 then CustomerID end) AS retain_customer,
lag(count(distinct CustomerID),1) over (order by Month_year) prev_month_total_customer
from ORDERRANK
group by Month_year)

Select month_year,   retain_customer*100/prev_month_total_customer as retention_rate
from retention


/*--------------------------------------------- RFM Analysis -------------------------------------------- */
/*--------------------Segmentation through RFM Analysis view-----------*/
 CREATE VIEW rfm AS
(
with cte_rfm as
(
select  c.CustomerID,c.CompanyName,sum((od.UnitPrice-od.Discount) * od.Quantity) as Monetary_value,
count(distinct o.OrderID) as frequency,
max(o.OrderDate) as Last_order_date,
(select max(OrderDate) from orders ) as current_date_, # assuming max order date as current date
datediff ((select max(OrderDate) from orders ), max(o.OrderDate) ) AS recency
from products p
left join `order details` od
on p.ProductID=od.ProductID
left join orders o
on o.OrderID=od.OrderID
left join customers c
on c.CustomerID=o.CustomerID
group by c.CustomerID,c.CompanyName
),rfm_score as
(
select * ,
NTILE(5) OVER (ORDER BY recency desc) R,
        NTILE(5) OVER (ORDER BY frequency) F,
        NTILE(5) OVER (ORDER BY Monetary_value) M

from cte_rfm r
)
select*,
  concat(R, F,M ) rfm_combination,
case when R in (4,5) and F in (4,5) and M in (4,5)then 'Champian'
when R in (2,3,4,5) and F in (3,4,5) and M in (3,4,5)then 'Loyal Customer'
     when R in (3,4,5) and F in (1,2,3) and M in (2,3,4)then 'Potential loyalest'
when R in (4,5) and F in (0,1,2) and M in (0,1)then 'New Customer'
     when R in (3,4) and F in (0,1) and M in (0,1)then 'Promising Customer'
     when R in (2,3) and F in (2,3) and M in (2,3)then 'Customer Needing Attention '
     when R in (2,3) and F in (0,1,2) and M in (0,1,2)then 'About to sleep'
     when R in (0,1,2) and F in (2,3,4,5) and M in (2,3,4,5)then 'At Risk'
     when R in (0,1) and F in (4,5) and M in (4,5)then 'Cannot lo0se them'
     when R in (1,2) and F in (1,2) and M in (1,2)then 'Hibernating'
     when R in (0,1,2) and F in (0,1,2) and M in (0,1,2)then 'Lost'
     else 'customers'
     end as Customer_Type
from rfm_score
  );
  /*----------------Customer Count By Segments-------*/
  select customer_Type , count(CustomerID) as customer_count
 from rfm
group by customer_Type;
 /*-------------Percentage Per Segment-----------*/
 with segentationcount as
 (
 select customer_Type , count(CustomerID) as customer_count
 from rfm
group by customer_Type
)
select
*, (customer_count/(sum(customer_count)over ())*100) as Percentage
from segentationcount;
/*--------------------Average order Value ------------------------*/

select  c.CustomerID,c.CompanyName,sum((od.UnitPrice-od.Discount) * od.Quantity) as Revenue,
count(distinct o.OrderID) as Total_transactions,
(sum((od.UnitPrice-od.Discount) * od.Quantity))/count(distinct o.OrderID) as AOV
from products p
left join `order details` od
on p.ProductID=od.ProductID
left join orders o
on o.OrderID=od.OrderID
left join customers c
on c.CustomerID=o.CustomerID
group by c.CustomerID,c.CompanyName;
/*---------------------Three Favourite catagories of Customers on basis of revenue and ordercount ------------------------*/
with Top_three_catagories as (
    select
        c.CustomerID,ca.CategoryName , count(distinct o.OrderID) as ordercount,sum((od.UnitPrice-od.Discount) * od.Quantity) as revenue,
        dense_rank() over (partition by c.CustomerID order by count(distinct o.OrderID) desc, sum((od.UnitPrice-od.Discount) * od.Quantity) desc) as rank_
    from `order details` od
    left join orders o
    on o.OrderID = od.OrderID
    left join customers c
    on c.CustomerID = o.CustomerID
    left join Products p
    on od.ProductID = p.ProductID
    left join Categories ca
    ON p.CategoryID = ca.CategoryID
    group by c.CustomerID, ca.CategoryName
)
select  *
from Top_three_catagories
where rank_ <= 3;
/*-------------------order Lead Time Per country--------------------*/

select o.ShipCountry,s.ShipperID,s.CompanyName,
avg(DATEDIFF(o.ShippedDate, o.OrderDate ))as Average_shiping_time
from shippers s
left join orders o
on s.ShipperID= o.ShipVia
    group by o.ShipCountry,s.ShipperID,s.CompanyName
    order by o.ShipCountry;
   
    /*--------------------------Purchasing Power of each country------------------------------*/
   
WITH CustomerOrderCTE AS (
    SELECT
        C.Country,
        COUNT(distinct c.CustomerID) AS CustomerCount,
        SUM(OD.Quantity * (OD.UnitPrice - (OD.UnitPrice * OD.Discount))) AS OrderTotal
    FROM Customers AS C
    JOIN Orders AS O ON C.CustomerID = O.CustomerID
    JOIN `order details` AS OD ON O.OrderID = OD.OrderID
    GROUP BY C.Country, C.CustomerID
)
SELECT
    Country,
    SUM(CustomerCount) AS TotalCustomerCount,
    SUM(OrderTotal) AS TotalOrderValue,
    SUM(OrderTotal) / SUM(CustomerCount) AS AveragePurchasePower
FROM CustomerOrderCTE
GROUP BY Country
ORDER BY AveragePurchasePower DESC;
    /*---------------Month to Month Revnue Comparision--------------------*/
   
with cte as
(
select DATE_FORMAT( o.OrderDate, '%Y-%m') as Month_wise,
sum((od.UnitPrice-od.Discount) * od.Quantity) as sales_Revenue
from orders o
left join `order details` od
on o.OrderID=od.OrderID
group by Month_Wise
),cte2 as
(
select *,
lag(Month_Wise, 12 ) over () as Last_yearMonth,
lag(sales_Revenue, 12 ) over () as Last_YearMonth_revenue
from cte)
select * , round(((sales_Revenue -Last_YearMonth_revenue)/Last_YearMonth_revenue)*100,2) as growth
from cte2
