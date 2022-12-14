-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 사용할 테이블
-- MAGIC - 테이블명: aac_intakes, aac_outcomes
-- MAGIC - 해당 테이블은 동물 보호소에 들어온 동물의 정보를 담은 테이블 입니다.
-- MAGIC - 테이블 구조는 아래와 같습니다. (테이블은 모든 데이터베이스에 담아두었습니다.)
-- MAGIC 
-- MAGIC 
-- MAGIC ### 테이블 컬럼 설명
-- MAGIC - ANIMAL_ID : 동물의 아이디
-- MAGIC - ANIMAL_TYPE : 생물종
-- MAGIC - DATETIME : 보호 시작일
-- MAGIC - INTAKE_CONDITION : 보호 시작 시 상태
-- MAGIC - NAME : 이름
-- MAGIC - SEX_UPON_INTAKE : 성별 및 중성화 여부

-- COMMAND ----------

use musin;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### 집계 함수

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### max
-- MAGIC - 가장 최근에 들어온 동물은 언제 들어왔는지 조회하는 SQL 문을 작성해주세요.

-- COMMAND ----------

SELECT MAX(DATETIME) FROM aac_intakes

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### min
-- MAGIC - 동물 보호소에 가장 먼저 들어온 동물은 언제 들어왔는지 조회하는 SQL 문을 작성해주세요

-- COMMAND ----------

SELECT MIN(DATETIME) FROM aac_intakes

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### count
-- MAGIC - 동물 보호소에 동물이 몇 마리 들어왔는지 조회하는 SQL 문을 작성해주세요.

-- COMMAND ----------

SELECT COUNT(ANIMAL_ID) count
FROM aac_intakes;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### JOin문제 1
-- MAGIC - 천재지변으로 인해 일부 데이터가 유실됨. 입양을 간 기록은 있는데 보호소에 들어온 기록이 없는 animal_id와 name을 ID순으로 조회하기
-- MAGIC   - acc_intakes 테이블은 '동물 보호소에 들어온 동물 정보'를 담은 테이블입니다.
-- MAGIC   - acc_outcomes 테이블은 '동물 보호소에서 입양보낸 동물 정보'를 담은 테이블입니다.

-- COMMAND ----------

select * from aac_intakes limit 3;

-- COMMAND ----------

select * from aac_outcomes limit 3;

-- COMMAND ----------

use musin;

select a.animal_id as `보호소 온 동물명`, b.animal_id as `입양간 동물명`, b.name, a.intake_condition
from aac_intakes a
right join aac_outcomes b
on a.animal_id = b.animal_id

-- COMMAND ----------

use musin;

select a.animal_id as `보호소 온 동물명`, b.animal_id as `입양간 동물명`, b.name, a.intake_condition
from aac_intakes a
right join aac_outcomes b
on a.animal_id = b.animal_id
where a.animal_id is null
order by b.animal_id, b.name;

-- COMMAND ----------

use musin;

select b.animal_id, b.name, a.intake_condition
from aac_intakes as a
right join aac_outcomes as b
on a.animal_id = b.animal_id
where a.animal_id is null
order by b.animal_id, b.name;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### JOin문제 2 
-- MAGIC - 관리자의 실수로 일부 동물의 입양일이 잘못 입력되었습니다. 보호 시작일보다 입양일이 더 빠른 동물의 아이디와 이름을 조회하는 SQL문을 작성해주세요. 이때 결과는 보호 시작일이 빠른 순으로 조회해야합니다.
-- MAGIC   - acc_intakes 테이블은 '동물 보호소에 들어온 동물 정보'를 담은 테이블입니다.
-- MAGIC   - acc_outcomes 테이블은 '동물 보호소에서 입양보낸 동물 정보'를 담은 테이블입니다.

-- COMMAND ----------

SELECT B.ANIMAL_ID, B.NAME, A.DATETIME as `보호시작일`, B.DATETIME as `입양일`
FROM aac_intakes A
inner JOIN aac_outcomes B
ON A.ANIMAL_ID = B.ANIMAL_ID

-- COMMAND ----------

SELECT B.ANIMAL_ID, B.NAME, A.DATETIME as `보호시작일`, B.DATETIME as `입양일`
FROM aac_intakes A
inner JOIN aac_outcomes B
ON A.ANIMAL_ID = B.ANIMAL_ID
WHERE A.DATETIME > B.DATETIME
ORDER BY A.DATETIME;

-- COMMAND ----------

SELECT B.ANIMAL_ID, B.NAME
FROM aac_intakes A
inner JOIN aac_outcomes B
ON A.ANIMAL_ID = B.ANIMAL_ID
WHERE A.DATETIME > B.DATETIME
ORDER BY A.DATETIME ;

-- COMMAND ----------

select aac_outcomes.animal_id as `번호`, aac_outcomes.name as `이름`, aac_intakes.datetime as `입소일`, aac_outcomes.datetime as `입양일`
from musin.aac_outcomes
left join musin.aac_intakes on aac_outcomes.animal_id = aac_intakes.animal_id
where not aac_outcomes.datetime < aac_intakes.datetime
order by aac_outcomes.animal_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### JOin문제 3 
-- MAGIC - 아직 입양을 못 간 동물 중, 가장 오래 보호소에 있었던 동물 3마리의 이름과 보호 시작일을 조회하는 SQL문을 작성해주세요. 이때 결과는 보호 시작일 순으로 조회해야 합니다.
-- MAGIC   - acc_intakes 테이블은 '동물 보호소에 들어온 동물 정보'를 담은 테이블입니다.
-- MAGIC   - acc_outcomes 테이블은 '동물 보호소에서 입양보낸 동물 정보'를 담은 테이블입니다.

-- COMMAND ----------

SELECT A.animal_id, A.DATETIME
FROM aac_intakes A
LEFT JOIN aac_outcomes B
ON A.ANIMAL_ID = B.ANIMAL_ID
WHERE B.ANIMAL_ID IS NULL
ORDER BY A.DATETIME
LIMIT 3;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### JOin문제 4 
-- MAGIC - 보호소에서 중성화 수술을 거친 동물 정보를 알아보려 합니다. 보호소에 들어올 당시에는 중성화되지 않았지만, 보호소를 나갈 당시에는 중성화된 동물의 아이디와 생물 종, 이름을 조회하는 아이디 순으로 조회하는 SQL 문을 작성해주세요.
-- MAGIC   - acc_intakes 테이블은 '동물 보호소에 들어온 동물 정보'를 담은 테이블입니다.
-- MAGIC   - acc_outcomes 테이블은 '동물 보호소에서 입양보낸 동물 정보'를 담은 테이블입니다.

-- COMMAND ----------

SELECT A.ANIMAL_ID, A.ANIMAL_TYPE, A.NAME
FROM aac_intakes A
JOIN aac_outcomes B
ON A.ANIMAL_ID = B.ANIMAL_ID
WHERE A.SEX_UPON_INTAKE != B.SEX_UPON_OUTCOME
ORDER BY ANIMAL_ID

-- COMMAND ----------


