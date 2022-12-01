-- Databricks notebook source
-- DBTITLE 1,1. SQL이란?
-- MAGIC %md
-- MAGIC ### SQL을 이해하기 위한 키워드
-- MAGIC 1) 데이터
-- MAGIC 2) 데이터베이스
-- MAGIC 3) DBMS
-- MAGIC 4) 테이블
-- MAGIC 5) SQL
-- MAGIC 6) 쿼리

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### 1. 데이터
-- MAGIC - 우리가 수집하고 싶은 `정보`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. 데이터베이스
-- MAGIC - 데이터베이스(Database)는 `데이터의 집합`
-- MAGIC - 데이터베이스에는 일상생활의 대부분의 정보(우리가 모으고 싶은 정보)들이 `저장`되고 `관리`된다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. DBMS
-- MAGIC - DBMS는 간단히 말해서 `DB의 소프트웨어`
-- MAGIC - 즉, DB(데이터베이스)를 사용하기 위해 `다운받아야하는 소프트웨어`
-- MAGIC - 대표적인 예로 Mysql, oracle, postgresql... (이 예들은 정확히는 RDBMS이지만 보편적으로 DBMS라 함)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. 테이블
-- MAGIC - 이런 `데이터베이스`들을 이루고있는 구성단위 중, `최소 구성단위`가 바로 테이블(세포)
-- MAGIC - 테이블은 하나 이상의 `열(column)`와 `행(row)`으로 이루어져 있으며, 모든 데이터가 이 테이블 형태로 저장

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 5. SQL
-- MAGIC - 그러면 SQL은 무엇일까?(db가 이해할 수 있는 언어 -  우리가 원하는 데이터를 db에 요청)
-- MAGIC - 바로 DBMS(db를 사용하기 위한 소프트웨어)에서 사용하는 `언어`이다.
-- MAGIC - SQL(Structured Query Language)의 약자.
-- MAGIC - 원하는 데이터(정보)를 가져오거나(추출) 추가, 삭제 등 dbms 안의 `데이터를 만지고 가공`할 수 있다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 6. 쿼리
-- MAGIC - sql을 사용하는걸 '쿼리한다'고 한다.
-- MAGIC - 쿼리 = '질의문'이라는 뜻을 가지고 있다.
-- MAGIC - 예를 들어 Google(Databricks)에서 검색할 때 입력하는 '검색어(SQL)'가 일종의 '쿼리' 
-- MAGIC - Goole에서 검색하면, Google 내에 존재하는 데이터에서 검색어로 `필터링`하는 것.
-- MAGIC - 따라서, '쿼리'는 저장되어 있는 `데이터를 필터하기 위한 질의문`이다.
-- MAGIC - DDL, DML, DCL 등으로 나뉨.
-- MAGIC - `DML`: select, 등(데이터베이스 내 table을 필터링하거나 가공하는 방법)
-- MAGIC - DDL: create, alter, drop, rename, truncate 등(테이블과 같은 데이터 '구조'를 정의하는데 사용됨. 생성, 변경, 삭제, 이름 변경 등)
-- MAGIC - DCL: 데이터베이스 권한 관련

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - select, from 등은 예약어이다. 마음대로 choose, pick 등으로 바꿀 수 없다.
-- MAGIC - 즉, select는 데이터를 가져오는거라는 규칙.
-- MAGIC - s,e,l,e,c,t로 하나씩 읽다가 띄어쓰기가 나오면 합쳐서 select로 읽음. (sql 언어의 구분은 띄어쓰기)

-- COMMAND ----------

-- DBTITLE 1,2. 해당 SQL 실행하여 Database 설정
use musin;

-- COMMAND ----------

-- DBTITLE 1,이번 예제에서 사용할 테이블
show tables;

-- COMMAND ----------

-- DBTITLE 1,3. DML
-- MAGIC %md
-- MAGIC ### 사용해볼 명령어
-- MAGIC 1. select / distinct
-- MAGIC 2. where
-- MAGIC 3. order by
-- MAGIC 4. 집계함수
-- MAGIC 5. group by
-- MAGIC 6. having

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. select
-- MAGIC 
-- MAGIC - 위에서 `show tables`로 확인한 테이블을 예제로 사용.
-- MAGIC - 데이터베이스 내 테이블에 있는 데이터를 조회하거나 검색하기 위한 명령어.
-- MAGIC - `select`를 통해 조회된 쿼리는 처음 1,000행만 샘플로 보여짐. (1000개)
-- MAGIC - 실제 데이터는 10,000,000행을 가지고 있음. (1000만개)

-- COMMAND ----------

-- DBTITLE 1,1. 모든 ‘도서이름’과 ‘가격＇을 검색하시오
SELECT bookname, price FROM book;

-- COMMAND ----------

-- DBTITLE 1,2. 모든 도서의 ‘도서번호‘, ‘도서이름‘, ‘출판사’, ‘가격’을 검색하시오
SELECT bookid, bookname, publisher, price FROM book;

-- COMMAND ----------

-- DBTITLE 1,2. 모든 도서의 ‘도서번호‘, ‘도서이름‘, ‘출판사’, ‘가격’을 검색하시오
SELECT * FROM book;

-- COMMAND ----------

-- DBTITLE 1,3. Book테이블에 있는 모든 출판사를 검색하시오.
SELECT publisher FROM book;

-- COMMAND ----------

-- DBTITLE 1,4. Book테이블에 있는 모든 출판사를 중복을 제거하고 검색하시오.
SELECT DISTINCT publisher FROM book;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. where
-- MAGIC 
-- MAGIC - 데이터를 조회할 때 '특정 조건'을 기준으로 원하는 행을 출력하는데 사용.
-- MAGIC - where절이 포함된 select문을 실행하면 조회할 테이블의 각 컬럼에 where절의 조건식을 대입하여 결과가 True인 것만 출력. (`필터링`)
-- MAGIC 
-- MAGIC |술어|연산자|사용 예|
-- MAGIC |------|---|---|
-- MAGIC |1. 비교|=, <>, <, <=, =>, >=|Price < 20000|
-- MAGIC |2. 범위|BETWEEN|Price BETWEEN 10000 and 20000|
-- MAGIC |3. 집합|IN, NOT IN|Price IN (10000, 20000, 30000)|
-- MAGIC |4. 패턴|LIKE|Bookname LIKE ‘축구의 역사’|
-- MAGIC |5. 복합 조건|AND, OR, NOT|(price < 20000) AND (bookname LIKE ‘축구의 역사‘)|

-- COMMAND ----------

-- DBTITLE 1,<비교> 1. 가격이 20,000원 미만인 도서를 검색하시오
SELECT * 
FROM book
WHERE price < 20000;

-- COMMAND ----------

-- DBTITLE 1,<범위> 1. 가격이 10,000원 이상 20,000원 이하인 도서를 검색하시오.
SELECT * 
FROM book
WHERE price BETWEEN 10000 AND 20000;

-- COMMAND ----------

-- DBTITLE 1,<비교> 2. 가격이 10,000원 이상 20,000원 이하인 도서를 검색하시오.
SELECT * 
FROM book
WHERE price >= 10000 AND price <=  20000;

-- COMMAND ----------

-- DBTITLE 1,<집합> 1. 출판사가 ＇굿스포츠’ 혹은 ‘대한미디어＇인 도서를 검색하시오.
SELECT * 
FROM book
WHERE publisher IN ('굿스포츠', '대한미디어');

-- COMMAND ----------

-- DBTITLE 1,<집합> 2. 출판사가 ＇굿스포츠’ 혹은 ‘대한미디어＇가 아닌 도서를 검색하시오.
SELECT * 
FROM book
WHERE publisher NOT IN ('굿스포츠', '대한미디어');

-- COMMAND ----------

-- DBTITLE 1,<패턴> 1. ‘축구의 역사＇를 출간한 출판사를 검색하시오.
SELECT bookname, publisher 
FROM book
WHERE bookname LIKE '축구의 역사';

-- COMMAND ----------

-- DBTITLE 1,<패턴> 2. 도서이름에 ‘축구＇가 포함된 출판사를 검색하시오.
SELECT bookname, publisher 
FROM book
WHERE bookname LIKE '%축구%';

-- COMMAND ----------

-- DBTITLE 1,<패턴> 3. 도서이름의 왼쪽 두 번째 위치에 ‘구＇라는 문자열을 갖는 도서를 검색하시오.
SELECT * 
FROM book
WHERE bookname LIKE '_구%';

-- COMMAND ----------

-- DBTITLE 1,<복합 조건> 1. 축구에 관한 도서 중 가격이 20,000원 이상인 도서를 검색하시오
SELECT * 
FROM book
WHERE bookname LIKE '%축구%' AND price >= 20000;

-- COMMAND ----------

-- DBTITLE 1,<복합 조건> 2. 출판사가 ‘굿스포츠＇ 혹은 ‘대한미디어＇인 도서를 검색하시오.
SELECT * 
FROM book
WHERE publisher = '굿스포츠' OR publisher = '대한미디어';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. order by
-- MAGIC 
-- MAGIC - SQL 문장으로 조회된 데이터들을 다양한 목적에 맞게 정렬(특정 컬럼 기준)

-- COMMAND ----------

-- DBTITLE 1,1. 도서를 이름순으로 검색하시오.
SELECT * 
FROM book
ORDER BY bookname;

-- COMMAND ----------

-- DBTITLE 1,2. 도서를 가격순으로 검색하고, 가격이 같으면 이름순으로 검색하시오.
SELECT * 
FROM book
ORDER BY price, bookname;

-- COMMAND ----------

-- DBTITLE 1,3. 도서를 가격의 내림차순으로 검색하시오. 만약 가격이 같다면 출판사의 오름차순으로 출력하시오.
SELECT * 
FROM book
ORDER BY price DESC, publisher ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. 집계 함수
-- MAGIC - 여러 행의 수치를 단 1개의 수치로 반환(즉, 여러 행들을 합쳐서 1개의 값을 반환)
-- MAGIC   - SUM() : 여러 행의 수치의 총 합을 반환합니다.
-- MAGIC   - AVG() : 여러 행의 수치의 평균 값을 반환합니다.
-- MAGIC   - COUNT() : 여러 행의 수치의 총개수를 반환합니다.
-- MAGIC   - MAX()와 MIN() : 여러 행의 수치 내에서 각각 최댓값과 최솟값을 반환합니다.
-- MAGIC   
-- MAGIC   
-- MAGIC |술어|연산자|사용 예|
-- MAGIC |------|---|---|
-- MAGIC |SUM|SUM([ALL / DISTINCT] 컬럼명)|SUM(price)|
-- MAGIC |AVG|AVG([ALL / DISTINCT] 컬럼명)|AVG(price|
-- MAGIC |COUNT|COUNT({[[ALL / DISTINCT] 컬럼명] /*})|COUNT(*)|
-- MAGIC |MAX|MAX([ALL / DISTINCT] 컬럼명)|MAX(price)|
-- MAGIC |MIN|MIN([ALL / DISTINCT] 컬럼명)|MIN(price)| 

-- COMMAND ----------

-- DBTITLE 1,1. 고객이 주문한 도서의 총 판매액을 구하시오
SELECT SUM(saleprice) 
FROM orders;

-- COMMAND ----------

-- DBTITLE 1,2. 고객이 주문한 도서의 총 판매액을 구하시오 (의미 있는 열 이름을 출력하고 싶으면 별칭을 부여하는 AS 키워드를 사용할 수 있음)
SELECT SUM(saleprice) AS `총매출` 
FROM orders;

-- COMMAND ----------

-- DBTITLE 1,3. 2번 김연아 고객이 주문한 도서의 총 판매액을 구하시오
SELECT SUM(saleprice) AS `총매출` 
FROM orders
WHERE custid = 2;

-- COMMAND ----------

-- DBTITLE 1,4. 고객이 주문한 도서의 총 판매액, 평균값, 최저가, 최고가를 구하시오
SELECT SUM(saleprice) AS Total,
              AVG(saleprice) AS Average,
              MIN(saleprice) AS Minimum,
              MAX(saleprice) AS Maximum
FROM orders;

-- COMMAND ----------

-- DBTITLE 1,5. 전체 도서 판매 건수를 구하시오.
SELECT COUNT(*) FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. group by
-- MAGIC 
-- MAGIC - where절을 통해 조건에 맞는 데이터를 1차 가공했으나 테이블을 작은 그룹으로 묶는 2차 가공을 할 때 사용.
-- MAGIC - 주로 group by로 묶인 대상의 count값을 많이 조회.

-- COMMAND ----------

-- DBTITLE 1,1. 고객별로 주문한 도서의 총 수량과 총 판매액을 구하시오
SELECT custid, COUNT(*) AS `도서수량`, SUM(saleprice) AS `총액` 
FROM orders
GROUP BY custid;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. having
-- MAGIC 
-- MAGIC - group by와 함께 사용되며, 집계함수를 가지고 `조건 비교`할 때, 사용
-- MAGIC - 즉, `group by절에 의해 생성된 결과값 중`, 원하는 조건에 부합하는 자료만 보고자할 때 사용
-- MAGIC - 보기에 where과 필터링이라는 점에서 공통점이 있다.
-- MAGIC - 차이점은 having은 전체 결과, where은 개별 행에 대해 적용된다는 점이다.
-- MAGIC - 즉, having은 그룹을 나타내는(group by 후)결과 집합 행에만 적용된다. (having은 그룹용 필터링/where은 행 필터링)
-- MAGIC - 집계함수는 where절과 함께 사용할 수 없으나, having절과는 사용 가능하다.
-- MAGIC - having은 group by 뒤에 사용 / where은 group by 앞에 사용

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - where은 groupby 앞에 있으니, 우선 where로 필터링 하고 여기서 group by 됨
-- MAGIC - having은 groupby 뒤에 있으니, groupby한 상태에서 having으로 필터링(이때 having에 사용된 컬럼은 반드시 select 뒤에 있어야 함.)
-- MAGIC - 모든 필드를 조건에 둘 수 있습니다. 하지만 having은 group by 된 이후 특정한 필드로 그룹화 되어진 새로운 테이블에 조건을 줄 수 있음

-- COMMAND ----------

-- DBTITLE 1,1. 가격이 8,000원 이상인 도서를 구매한 고객에 대하여 고객별 주문 도서의 총 수량을 구하시오. (단, 2권 이상 구매한 고객 한정)
SELECT custid, COUNT(*) AS `도서수량`
FROM orders
WHERE saleprice >= 8000
GROUP BY custid
HAVING COUNT(*) >= 2;

-- COMMAND ----------

-- DBTITLE 1,작업환경 비우기(안하셔도 됩니다.)
-- %run "./Includes/Classroom-Cleanup"
