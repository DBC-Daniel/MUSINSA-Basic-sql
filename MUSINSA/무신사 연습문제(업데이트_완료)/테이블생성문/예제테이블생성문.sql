-- Databricks notebook source
CREATE TABLE musin.emp_sample
(EMPNO int not null,
ENAME string,
JOB string,
MGR int,
HIREDATE date,
SAL int,
COMM int,
DEPTNO int);


INSERT INTO musin.emp_sample 
VALUES
(7369,'SMITH','CLERK',7902,'1980-12-17',800,null,20),
(7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30),
(7521,'WARD','SALESMAN',7698,'1981-02-22',1250,200,30),
(7566,'JONES','MANAGER',7839,'1981-04-02',2975,30,20),
(7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,300,30),
(7698,'BLAKE','MANAGER',7839,'1981-04-01',2850,null,30),
(7782,'CLARK','MANAGER',7839,'1981-06-01',2450,null,10),
(7788,'SCOTT','ANALYST',7566,'1982-10-09',3000,null,20),
(7839,'KING','PRESIDENT',null,'1981-11-17',5000,3500,10),
(7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30),
(7876,'ADAMS','CLERK',7788,'1983-01-12',1100,null,20),
(7900,'JAMES','CLERK',7698,'1981-10-03',950,null,30),
(7902,'FORD','ANALYST',7566,'1981-10-3',3000,null,20),
(7934,'MILLER','CLERK',7782,'1982-01-23',1300,null,10);

CREATE TABLE musin.dept_sample 
(DEPTNO int,
DNAME string,
LOC string);


INSERT INTO musin.dept_sample 
VALUES 
(10,'ACCOUNTING','NEW YORK'),
(20,'RESEARCH','DALLAS'),
(30,'SALES','CHICAGO'),
(40,'OPERATIONS','BOSTON');


CREATE TABLE musin.salgrade
(GRADE int,
LOSAL int,
HISAL int);

INSERT INTO musin.salgrade
VALUES 
(1,700,1200),
(2,1201,1400),
(3,1401,2000),
(4,2001,3000),
(5,3001,9999);

create table musin.books
(id string, title string, type string, author_id string, editor_id string, translator_id string);

insert into musin.books
values('1', 'Time to Grow Up!', 'original', '11', '21', null),
      ('2', 'Your Trip', 'translated', '15', '22', '32'),
      ('3', 'Lovely Love', 'original', '14', '24', null),
      ('4', 'Dream Your Life', 'original', '11', '24', null),
      ('5', 'Oranges', 'translated', '12', '25', '31'),
      ('6', 'Your Happy Life', 'translated', '15', '22', '33'),
      ('7', 'Applied AI', 'translated', '13', '23', '34'),
      ('8', 'My Last Book', 'original', '11', '28', null);
	  
	  
create table musin.authors
(id string, first_name string, last_name string);

insert into musin.authors
values("11", "Ellen", "Writer"),
      ("12", "Olga", "Savelieva"),
      ("13", "Jack", "Smart"),
      ("14", "Donald", "Brain"),
      ("15", "Yao", "Dou");
	  
	  
	  
create table musin.editors
(id string, first_name string, last_name string);

insert into musin.editors
values("21", "Daniel", "Brown"),
      ("22", "Mark", "Johnson"),
      ("23", "Maria", "Evans"),
      ("24", "Cathrine", "Roberts"),
      ("25", "Sebastian", "Wright"),
      ("26", "Barbara", "Jones"),
      ("27", "Matthew", "Smith");
	  
	  
create table musin.translators
(id string, first_name string, last_name string);

insert into musin.translators
values("31", "Ira", 'Davies'),
      ("32", "Ling", 'Weng'),
      ("33", "Kristian", 'Green'),
      ("34", "Roman", 'Edwards');
	  
	  


-- COMMAND ----------

CREATE TABLE if not exists musin.employee(id int, name string, deptno int);

insert into musin.employee
     VALUES(105, 'Chloe', 5),
           (103, 'Paul' , 3),
           (101, 'John' , 1),
           (102, 'Lisa' , 2),
           (104, 'Evan' , 4),
           (106, 'Amy'  , 6);

CREATE TABLE if not exists musin.department(deptno int, deptname string);
 
insert into musin.department
VALUES(8, 'CEO'),
(7, 'Engineering'),
          (2, 'Sales'      ),
          (1, 'Marketing'  );
		  
		  

CREATE TABLE if not exists musin.transactions
   (id int, dates date, city STRING, amount float);
   
INSERT INTO musin.transactions
   VALUES (1, '2020-11-01', 'San Francisco', 420.65),
          (2, '2020-11-01', 'New York', 1129.85),
          (3, '2020-11-02', 'San Francisco', 2213.25),
          (4, '2020-11-02', 'New York', 499.00),
          (5, '2020-11-02', 'New York', 980.30),
          (6, '2020-11-03', 'San Francisco', 872.60),
          (7, '2020-11-03', 'San Francisco', 3452.25),
          (8, '2020-11-03', 'New York', 563.35),
          (9, '2020-11-04', 'New York', 1843.10),
          (10, '2020-11-04', 'San Francisco', 1705.00);
		  
		  
		  


-- COMMAND ----------

-- MAGIC %fs ls /FileStore/tables/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/aac_intakes.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "true"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC # temp_table_name = "aac_intakes.csv"
-- MAGIC # df.createOrReplaceTempView(temp_table_name)
-- MAGIC df.write.option('header','true').saveAsTable("musin.aac_intakes")
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/aac_outcomes.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "true"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df2 = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC df2.write.option('header','true').saveAsTable("musin.aac_outcomes")
-- MAGIC 
-- MAGIC display(df2)

-- COMMAND ----------


