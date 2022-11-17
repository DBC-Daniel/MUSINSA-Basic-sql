-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 사용할 테이블
-- MAGIC - emp_sample
-- MAGIC - dept_sample

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. 사원들의 이름(ename), 부서번호(deptno), 부서이름(dname)을 출력하라.
-- MAGIC 
-- MAGIC - 사용할 테이블: `emp_sample`, `dept_sample`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. DALLAS에서 근무(근무지 컬럼은 LOC)하는 사원의 이름(ename), 직위(job), 부서번호(deptno), 부서이름(dname)을 출력하라.
-- MAGIC - 사용할 테이블: `emp_sample`, `dept_sample`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. 이름(ename)에 'A'가 들어가는 사원들의 이름(ename)과 부서이름(dname)을 출력하라.
-- MAGIC - 사용할 테이블: `emp_sample`, `dept_sample`
-- MAGIC - 이름에 A가 들어가는 필터링 하는 법: `WHERE ENAME LIKE '%A%'`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 4. 사원이름(ename)과 그 사원이 속한 부서의 부서명(dname), 그리고 월급을 출력하는데 월급(sal)이 3000이상인 사원을 출력하라.
-- MAGIC - 사용할 테이블: `emp_sample`, `dept_sample`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 5. 직위(job)가 'SALESMAN'인 사원들의 직위와 그 사원이름(ename), 그리고 그 사원이 속한 부서 이름(dname)을 출력하라.
-- MAGIC - 사용할 테이블: `emp_sample`, `dept_sample`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 사용할 테이블
-- MAGIC - books
-- MAGIC - authors
-- MAGIC - editors
-- MAGIC - translators

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 6. 저자(first_name, last_name)와 함께 책 제목(title)을 출력하라
-- MAGIC 
-- MAGIC - 사용할 테이블: `books`, `authors`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 7. 번역(번역 여부는 type 컬럼에서 확인)된 책(title)과 이를 번역한 사람(translator)의 이름을 출력하라
-- MAGIC 
-- MAGIC - 사용할 테이블: `books`, `translators`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 8. 편집자의 성(editors 테이블의 last_name)과 함께 기본 도서 정보(id, title)를 출력하라
-- MAGIC 
-- MAGIC - 사용할 테이블: `books`, `editors`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 9. 각 책의 기본 정보(id, title, type)과 저자와 번역한 사람에 대한 정보(예: last_name)를 출력하라
-- MAGIC 
-- MAGIC - 사용할 테이블: `books`, `translators`, `authors`
-- MAGIC - 힌트: left join을 두 번 사용해야 합니다.

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 10. editors 테이블의 모든 레코드를 유지한 채로 편집자의 성(editors 테이블의 last_name)과 함께 기본 도서 정보(id, title)를 출력하라
-- MAGIC 
-- MAGIC - 사용할 테이블: `books`, `editors`
-- MAGIC - 힌트: 8번과 동일하며 left인지 right인지의 차이.

-- COMMAND ----------


