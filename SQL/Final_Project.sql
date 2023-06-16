create database Final_Project;
use Final_Project;

ALTER TABLE outlet
ADD primary key(Outlet_ID);

ALTER TABLE employee
ADD primary key(Employee_Id);

ALTER TABLE employee
ADD CONSTRAINT FK_Outlet_ID
FOREIGN KEY(Outlet_ID) REFERENCES Outlet(Outlet_ID);

Alter table shift
add primary key(shift_ID);

Alter table shift_employee
add primary key(Employee_Id,shift_ID);

ALTER TABLE orders
ADD primary key(order_Id);

ALTER TABLE orders
add FOREIGN KEY(Outlet_ID) REFERENCES Outlet(Outlet_ID);

Alter table material
modify Material_ID varchar(255);

Alter table material
Add primary key(Material_ID);

Alter table order_material
modify Material_ID varchar(255);

Alter table order_material
add primary key(Material_Id,order_ID);

Alter table product
Add primary key(product_ID);

ALTER TABLE sales
ADD primary key(sales_Id);

ALTER TABLE sales
add FOREIGN KEY(Outlet_ID) REFERENCES Outlet(Outlet_ID);

alter table sales
modify sales_Date datetime;

alter table menu
add primary key(outlet_ID,product_ID);

#Basic Queries
select max(price) from product;

select distinct(product_series) from product;

select count(shift_ID) as Mrng_Shifts from shift
where Morning_Night = 'M';


#Main Queries

#Top 5 products sold
select p.*,sum(sp.Product_Quantity) as Total_Sale from product p
left join sales_product sp on sp.product_ID = p.product_ID
group by p.product_ID
order by sum(sp.Product_Quantity) desc
Limit 5;

#Most ordered material (Top 5) 
select m.*,sum(om.quantity) as Total_Quantity from material m
left join order_material om on om.material_ID = m.material_ID
group by m.material_ID
order by sum(om.quantity) desc
Limit 5;

#Cost of Materials ordered over the entire time period
select m.*,sum(om.quantity) as Total_Quantity, m.value*sum(om.quantity) as Total_Amount from material m
left join order_material om on om.material_ID = m.material_ID
group by m.material_ID
order by Total_Amount desc;

#Total sales value grouped by Product_ID
select p.*,sum(sp.Product_Quantity) as Total_Sale from product p
left join sales_product sp on sp.product_ID = p.product_ID
group by p.product_ID
order by p.product_ID;

#Employee with max shifts
select e.employee_ID, e.employee_name, count(*) as Shift_Count from employee e
left join shift_employee se on e.employee_ID = se.employee_ID
group by e.employee_ID
order by count(*) desc
limit 1;

#Are there any employees with no shift
select e.employee_ID, e.employee_name from employee e
left join shift_employee se on e.employee_ID = se.employee_ID
group by e.employee_ID
Having count(se.shift_ID) =0
order by e.employee_ID;

#Products not sold at all
select p.*,sum(sp.Product_Quantity) as Total_Sale from product p
left join sales_product sp on sp.product_ID = p.product_ID
group by p.product_ID
Having sum(sp.Product_Quantity) =0;

#No. of sales with sales value greater than $500 in a day
select count(sales_ID) from sales
where sales_value > 500;

#No. of products sold starting with “coffee” in product_name
select p.*,sum(sp.Product_Quantity) as Total_Sale from product p
left join sales_product sp on sp.product_ID = p.product_ID
where product_name like 'coffee%'
group by p.product_ID
order by sum(sp.Product_Quantity) desc;

#Total sale volume
select sum(sales_value) as Total_Sale from sales;

#Sum of sales value grouped by month
select month(sales_Date) as Sale_Month,sum(sales_Value) as Total_sale_Value 
from sales
group by month(sales_Date)
order by sum(sales_Value) desc;

#Products sold and their quantities when there is maximum sale
select p.*, s.*, sp.product_quantity from product p
left join sales_product sp on sp.product_ID = p.product_ID
left join (select * from sales order by sales_value desc Limit 1) s on s.sales_ID = sp.sales_ID
where s.sales_ID is not null;

#Maximum average value of material across all the orders
select avg(m.value) from orders o
left join order_material om on o.order_ID = om.order_ID
left join material m on m.material_ID = om.material_ID
group by o.order_ID 
order by avg(m.value) desc
limit 1;

#Most ordered product within each series
With top_sold as 
(select p.*,sp.Total_Quantity, Row_Number() over(partition by p.product_series order by sp.Total_Quantity desc) as Max_Quantity 
from product p
left join (select product_ID,sum(product_quantity) as Total_Quantity from sales_product group by product_ID) sp 
on p.product_ID = sp.product_ID)
select product_ID, Product_Name, Price, Total_Quantity from top_sold where Max_Quantity = 1;