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
-- MAGIC - sql을 사용하는걸 쿼리한다고 한다.
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
use musinsa;

-- COMMAND ----------

-- DBTITLE 1,이번 예제에서 사용할 테이블
show tables;

-- COMMAND ----------

-- DBTITLE 1,3. DML
-- MAGIC %md
-- MAGIC ### 사용해볼 명령어
-- MAGIC 1. select
-- MAGIC 2. 집계함수
-- MAGIC 3. distinct
-- MAGIC 4. describe
-- MAGIC 5. where
-- MAGIC 6. group by
-- MAGIC 7. order by
-- MAGIC 8. having

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. select
-- MAGIC 
-- MAGIC - 위에서 `show tables`로 확인한 테이블을 예제로 사용.
-- MAGIC - 데이터베이스 내 테이블에 있는 데이터를 조회하거나 검색하기 위한 명령어.
-- MAGIC - `select`를 통해 조회된 쿼리는 처음 1,000행만 샘플로 보여짐. (1000개)
-- MAGIC - 실제 데이터는 10,000,000행을 가지고 있음. (1000만개)

-- COMMAND ----------

-- DBTITLE 1,전체 테이블 열어보기
SELECT
* 
FROM sales;

-- COMMAND ----------

-- DBTITLE 1,전체 테이블 row 수 확인하기
select count(*) from sales;

-- COMMAND ----------

-- DBTITLE 1,as 이용하여 원하는 컬럼명으로 출력(alias)
select date as `날짜`
from sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. 집계 함수
-- MAGIC - 여러 행의 수치를 단 1개의 수치로 반환(즉, 여러 행들을 합쳐서 1개의 값을 반환)
-- MAGIC   - COUNT() : 여러 행의 수치의 총개수를 반환합니다.
-- MAGIC   - AVG() : 여러 행의 수치의 평균 값을 반환합니다.
-- MAGIC   - SUM() : 여러 행의 수치의 총 합을 반환합니다.
-- MAGIC   - MAX()와 MIN() : 여러 행의 수치 내에서 각각 최댓값과 최솟값을 반환합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - select `count(카운트할 컬럼)` from `테이블`

-- COMMAND ----------

-- DBTITLE 1,count(): 수량 계산 (해당 테이블의 전체 row 수 확인)
select count(*) from sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - select avg `(카운트할 컬럼)` from `테이블`
-- MAGIC 
-- MAGIC - 만약 avg가 소수점으로 나왔는데, 소수점 보여주기 싫으면?
-- MAGIC - select `round(avg(카운트할 컬럼), 0)` from `테이블`          -> (이때, 0의 의미는 0의 자리까지 보여주겠다는 의미)

-- COMMAND ----------

-- DBTITLE 1,avg(): 평균
select avg(sales_price) from sales;

-- COMMAND ----------

-- DBTITLE 1,avg(): 평균 - round를 이용해서 소수점 조절
select round(avg(sales_price),0) as `평균 판매가` from sales;

-- COMMAND ----------

-- DBTITLE 1,avg(): 평균 - round를 이용해서 소수점 조절
select round(avg(sales_price),1) as `평균 판매가` from sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - select `max(카운트할 컬럼)` from `테이블`

-- COMMAND ----------

-- DBTITLE 1,max(): 최대값
select max(sales_price) as `최대 판매가` from sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - select `min(카운트할 컬럼)` from `테이블`

-- COMMAND ----------

-- DBTITLE 1,min(): 최소값
select min(sales_price) as `최소 판매가` from sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - select `sum(카운트할 컬럼)` from `테이블`

-- COMMAND ----------

-- DBTITLE 1,sum(): 합계
select sum(sales_price) as `총 판매가` from sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. distinct
-- MAGIC 
-- MAGIC - `중복`을 제외하고 select한 모든 것을 출력

-- COMMAND ----------

select * from sales;

-- COMMAND ----------

-- DBTITLE 1,gender라는 컬럼이 가지고 있는 고유값을 확인
SELECT distinct artist_id FROM sales;

-- COMMAND ----------

-- DBTITLE 1,count를 통해 distinct한 값의 갯수 파악
SELECT count(distinct artist_id) FROM sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. describe
-- MAGIC 
-- MAGIC - `describe`는 조회하는 테이블의 스키마 정보를 보여줌.
-- MAGIC - 출력에 관한 몇가지 옵션이 있음

-- COMMAND ----------

-- DBTITLE 1,기본
describe table sales

-- COMMAND ----------

-- DBTITLE 1,약자
desc table sales

-- COMMAND ----------

-- DBTITLE 1,extended 옵션 추가 시, 추가 메타정보 확인 가능(저장 경로, 테이블 생성 시간 등)
describe table Extended sales

-- COMMAND ----------

-- DBTITLE 1,뒤에 detail 추가 시, 테이블 포멧, 저장경로, 생성 시간, 파티션 정보 등 확인 가능
desc detail sales

-- COMMAND ----------

-- DBTITLE 1,뒤에 query 추가 시, 원하는 쿼리문의 컬럼명, 데이터 타입 등 확인 가능
desc query select date from sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. where
-- MAGIC 
-- MAGIC - 데이터를 조회할 때 '특정 조건'을 기준으로 원하는 행을 출력하는데 사용.
-- MAGIC - where절이 포함된 select문을 실행하면 조회할 테이블의 각 컬럼에 where절의 조건식을 대입하여 결과가 True인 것만 출력. (`필터링`)

-- COMMAND ----------

-- DBTITLE 1,Where절에는 집계함수를 사용할 수 없다. (아래 예시는 집계함수를 사용했기 때문에 Fail 발생)
select *
from sales
where count(id) > 3;

-- COMMAND ----------

-- DBTITLE 1,원하는 조건으로 필터링 하기(collector_id가 103인 데이터만 출력)
select *
from sales
where collector_id = 103;

-- COMMAND ----------

-- DBTITLE 1,원하는 조건으로 필터링 하기(or 사용)
select *
from sales
where collector_id = 103
and (id = 1004 or id = 1005);

-- COMMAND ----------

-- DBTITLE 1,원하는 조건으로 필터링 하기(and 사용)
select *
from sales
where collector_id = 103 and id =1004;

-- COMMAND ----------

-- DBTITLE 1,원하는 조건으로 필터링 하기(and / or 사용) 
select *
from sales
where (collector_id = 103 and id =1004) 
or (collector_id = 104 and id = 1001);

-- COMMAND ----------

-- DBTITLE 1,원하는 조건으로 필터링 하기(부등호) - 판매가가 1000이상인 데이터 조회해보기
select *
from sales
where sales_price > 1000;

-- COMMAND ----------

-- DBTITLE 1,원하는 조건으로 필터링 하기(Not 사용)
select distinct(artist_id)
from sales
where not collector_id = 103;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. group by
-- MAGIC 
-- MAGIC - where절을 통해 조건에 맞는 데이터를 1차 가공했으나 테이블을 작은 그룹으로 묶는 2차 가공을 할 때 사용.
-- MAGIC - 주로 group by로 묶인 대상의 count값을 많이 조회.

-- COMMAND ----------

-- DBTITLE 1,select 절의 컬럼과 group by절의 컬럼이 같지 않으면 Fail(올바른 수정 방법은 select 절에서 선택한 모든 컬럼을 group by에 추가하는 것)
select id
from sales
group by collector_id;

-- COMMAND ----------

-- DBTITLE 1,group by로 collector_id를 묶어 각 collector_id별 수를 파악하고 앞에서 사용했던 as를 이용해서 컬럼명 변경
select collector_id, count(collector_id) as collector_id_count
from sales
group by collector_id;

-- COMMAND ----------

-- DBTITLE 1,묶은 그룹의 집계함수값 group by에 없어도 가능
select collector_id, count(collector_id) as collector_id_count, min(sales_price)
from sales
group by collector_id;

-- COMMAND ----------

-- DBTITLE 1,임의의 값의 경우, 추가해도 쿼리가 가능
select '2022년' as Year, collector_id, count(collector_id) as collector_id_count, min(sales_price)
from sales
group by collector_id;

-- COMMAND ----------

-- DBTITLE 1,alias(as)를 한글로 주고 싶을 땐, ``를 사용해야 함.
select '2022년' as `연도`, collector_id, count(collector_id) as collector_id_count, min(sales_price)
from sales
group by collector_id;

-- COMMAND ----------

select * from sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. order by
-- MAGIC 
-- MAGIC - SQL 문장으로 조회된 데이터들을 다양한 목적에 맞게 정렬(특정 컬럼 기준)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - 몇 년도, 몇 월, 며칠에 그림이 가장 많이 팔렸을까요?
-- MAGIC - year(date) as year, month(date) as month, day(date) as day

-- COMMAND ----------

select year(date) as year, month(date) as month, day(date) as day from sales;

-- COMMAND ----------

select day(date) as day, count(*) as `판매수`
from sales
group by day;

-- COMMAND ----------

select day(date) as day, count(*) as `판매수`
from sales
group by day
order by day;

-- COMMAND ----------

-- DBTITLE 1,연봉 기준으로 정렬하여 가장 작은 sales_price순으로 보임. (오름차순 - 작은 값부터 큰 값)
select *
from sales
order by sales_price asc;

-- COMMAND ----------

-- DBTITLE 1,연봉 기준으로 정렬하여 가장 큰 salary순으로 보임. (내림차순 - 큰 값부터 작은 값)
select *
from sales
order by sales_price desc;

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

-- DBTITLE 1,우선 group by까지 적용된(앞에서 진행한 모든 function 적용) 쿼리 출력(똑같은 이름을 가진 사람은 몇명일까? desc을 통해 2명임을 확인)
select day(date) as day, count(*) as `판매수`
from sales
group by day
order by day;

-- COMMAND ----------

-- DBTITLE 1,조건문을 걸어 '판매수'가 2회 이상인 결과만 남기기
select day(date) as day, count(*) as `판매수`
from sales
group by day
having `판매수` > 1
order by day;

-- COMMAND ----------

-- DBTITLE 1,작업환경 비우기(안하셔도 됩니다.)
-- %run "./Includes/Classroom-Cleanup"
