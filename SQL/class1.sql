/*here we creating class1 database in trasactional database*/
create database class1;

/*go inside database you have to first select that database you can do it by using use keyword along with database name*/
use class1;

/*If you want to see how many databases are present */
show databases;

/*same command for tables but here we get list of tables of selected database*/
show tables;

/*create simple table using create keyword*/
create table employee(
id int,
first_name varchar(50),
last_name varchar(50),
salary int,
city varchar(20)
);

/*Insert record in table*/
insert into employee values(1,'waman','birajdar',70000,'pune');

/*to see content of tables */
select * 
from employee;

/*you can inserted more records in one single expression by comma seperated*/
insert into employee values(2,'kalpesh','shelar',50000,'nagar'),(3,'pankaj','more',40000,'mumbai'),(4,'roahn','babar',55000,'gujarat');

select * from employee;

/*list down all tables*/
show create table employee;


/*Example for integrity constraint*/
Create table if not exists employee_with_constraints
(
    id int NOT NULL,
    name VARCHAR(50) NOT NULL,
    salary DOUBLE,
    hiring_date DATE DEFAULT '2021-01-01',
    UNIQUE (id),
    CHECK (salary > 1000)
);


insert into employee_with_constraints values(null,'waman',300000,'2023-01-01');

select * from employee_with_constraints;


/*add alias name for constraint*/

Create table if not exists employee_with_constraints_tmp
(
    id int NOT NULL,
    name VARCHAR(50) NOT NULL,
    salary DOUBLE,
    hiring_date DATE DEFAULT '2021-01-01',
    CONSTRAINT unique_id UNIQUE (id),
    CONSTRAINT salary_check CHECK (salary > 1000)
);
/*below script will throw an error*/
insert into employee_with_constraints_tmp values(1,'Wmaan',50000,'2021-09-15');
insert into employee_with_constraints_tmp values(null,'kalan',11000,'2021-09-11');


desc employee_with_constraints_tmp;

