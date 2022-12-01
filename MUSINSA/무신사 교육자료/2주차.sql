-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 3-2. Join문(서브쿼리 포함)
-- MAGIC ##### 개요
-- MAGIC 1. inner join
-- MAGIC 2. left (outer) join
-- MAGIC 3. right (outer) join
-- MAGIC 4. full (outer) join
-- MAGIC 5. cross join
-- MAGIC 6. self join
-- MAGIC 7. anti join
-- MAGIC 8. semi join

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Inner Join
-- MAGIC - 2개의 테이블에서 값이 일치하는 행을 반환.
-- MAGIC - 즉, 교집합

-- COMMAND ----------

-- DBTITLE 1,1. Emp 테이블과 dept 테이블을 Inner Join 해보시오
SELECT emp_id, name, a.deptno, deptname, direct_supervisor_id
FROM musin.emp a
INNER JOIN musin.dept b
ON a.deptno = b.deptno;

-- COMMAND ----------

-- DBTITLE 1,2. Emp 테이블과 dept 테이블을 Inner Join 해보시오
SELECT emp_id, name, a.deptno, deptname, direct_supervisor_id
FROM musin.emp a
JOIN musin.dept b
ON a.deptno = b.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Outer Join
-- MAGIC - 두 테이블에서 지정된 쪽인 Left 또는 Right 쪽의 모든 결과를 보여줍니다.
-- MAGIC - 반대쪽에 매칭되는 값이 없어도 보여주는 Join
-- MAGIC - 쉽게 말해서 쿼리 내 Join문 기준, Join 이전에 나오는 테이블이 왼쪽(Left)가 되고, Join 이후에 나오는 테이블은 오른쪽(Right)이 됩니다.
-- MAGIC 
-- MAGIC ##### 2. Left Outer Join
-- MAGIC - 좌측을 기준으로 합니다.
-- MAGIC - 예를 들어, `...FROM A LEFT JOIN B...`인 경우, 왼쪽에 있는 A의 모든 값을 다 보여주면서(A데이터 범위 안에 한정) 이에 매치되는 B까지 함께 보여줌.

-- COMMAND ----------

-- DBTITLE 1,1. Emp 테이블을 기준으로 dept 테이블과 Left Join 해보시오
SELECT emp_id, name, a.deptno, direct_supervisor_id, deptname
FROM musin.emp a
LEFT JOIN musin.dept b
ON a.deptno = b.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Right (Outer) Join
-- MAGIC - 우측을 기준으로 합니다.
-- MAGIC - 예를 들어, `...FROM A LEFT JOIN B...`인 경우, 오른쪽에 있는 B의 모든 값을 다 보여주면서(B데이터 범위 안에 한정) 이에 매치되는 A까지 함께 보여줌.

-- COMMAND ----------

-- DBTITLE 1,1. dept 테이블을 기준으로 emp 테이블과 Right Join 해보시오
SELECT emp_id, name, a.deptno, direct_supervisor_id, deptname
FROM musin.emp a
RIGHT JOIN musin.dept b
ON a.deptno = b.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 4. Full (Outer) Join
-- MAGIC - 일종의 합집합으로 이해하면 쉬움

-- COMMAND ----------

-- DBTITLE 1,1. Emp 테이블을과 dept 테이블을 Full Join 해보시오
SELECT emp_id, name, a.deptno, direct_supervisor_id, deptname
FROM musin.emp a
FULL JOIN musin.dept b
ON a.deptno = b.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 5. cross Join
-- MAGIC - 상호 모든 행에 대한 결합 (on을 사용하지 않는다)
-- MAGIC - employee는 6행, department는 4행
-- MAGIC - 둘의 조합은 6*4 = 24행
-- MAGIC - cross join 시 출력되어야할 행은 24

-- COMMAND ----------

-- DBTITLE 1,1. Emp 테이블과 dept 테이블을 Cross Join 해보시오
SELECT emp_id, name, a.deptno, direct_supervisor_id, deptname
FROM musin.emp a
CROSS JOIN musin.dept b;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 6. self Join
-- MAGIC - 동일 테이블 사이의 조인
-- MAGIC - 따라서 From절에 동일한 테이블이 2번 이상 작성됨.
-- MAGIC - 동일 테이블 join을 할때는 테이블과 컬럼 이름이 모두 동일하기 때문에 식별을 위해 반드시 alias(as)를 사용해야함.

-- COMMAND ----------

-- DBTITLE 1,1. 각 사원들의 직속상관을 뽑아보시오
SELECT a.emp_id AS `사원번호`, a.name AS `직원명`, b.name AS `직속상관`, b.direct_supervisor_id AS `직속상관번호`
FROM musin.emp a
INNER JOIN musin.emp b
ON a.direct_supervisor_id = b.emp_id;

-- COMMAND ----------

-- DBTITLE 1,2. 각 사원들의 직속상관을 뽑아보시오(유부장 포함)
SELECT a.emp_id AS `사원번호`, a.name AS `직원명`, b.name AS `직속상관`, b.direct_supervisor_id AS `직속상관번호`
FROM musin.emp a
LEFT JOIN musin.emp b
ON a.direct_supervisor_id = b.emp_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 7. Anti Join
-- MAGIC - Anti Join은 요약하면 기준 테이블에 대한 차집합임. 앞서 진행한 Left / Right Join 결과에서 공통 부분을 빼는 방법임.
-- MAGIC - (IS NULL / NOT IN / NOT EXISTS)

-- COMMAND ----------

-- DBTITLE 1,1. Left Join 사용해서 Anti Join 수행하시오
SELECT emp_id, name, a.deptno 
FROM musin.emp a 
LEFT JOIN musin.dept b
ON a.deptno = b.deptno
WHERE b.deptno IS NULL;

-- COMMAND ----------

-- DBTITLE 1,2. NOT IN 사용해서 Anti Join 수행하시오
SELECT emp_id, name, a.deptno
FROM musin.emp a
WHERE a.deptno NOT IN
  (
  SELECT b.deptno
  FROM musin.dept b
  );

-- COMMAND ----------

-- DBTITLE 1,3. Not Exists 사용해서 Anti Join 수행하시오
SELECT emp_id, name, a.deptno
FROM musin.emp a
WHERE NOT EXISTS
  (
  SELECT b.deptno
  FROM musin.dept b
  WHERE a.deptno = b.deptno
  );


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 8. Semi Join
-- MAGIC - Semi Join은 EXISTS 연산자를 활용해서 Join하는 방법임. 서브 쿼리 내에서 존재하는 데이터만 추출하며 중복을 제거함. Anti Join의 반대임. 
-- MAGIC - (IS NOT NULL / IN / EXISTS)

-- COMMAND ----------

-- DBTITLE 1,1. Left Join 사용해서 Semi Join 수행하시오
SELECT emp_id, name, a.deptno 
FROM musin.emp a 
LEFT JOIN musin.dept b
ON a.deptno = b.deptno
WHERE b.deptno IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,2. IN 사용해서 Semi Join 수행하시오
SELECT emp_id, name, a.deptno
FROM musin.emp a
WHERE a.deptno IN
  (
  SELECT b.deptno
  FROM musin.dept b
  );

-- COMMAND ----------

-- DBTITLE 1,3. Exists 사용해서 Semi Join 수행하시오
SELECT emp_id, name, a.deptno
FROM musin.emp a
WHERE EXISTS
  (
  SELECT b.deptno
  FROM musin.dept b
  WHERE a.deptno = b.deptno
  );
