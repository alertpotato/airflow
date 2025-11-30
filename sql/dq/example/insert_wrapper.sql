/*
    тестовая обёртка для правил DQ

    не выполняет вставку строк, только SELECT
*/


-- insert into dq.log_dq_example
SELECT
    now() AS check_dtm,
    q.value
FROM
    (   -- в скобках должен быть запрос из файла проверки:

{{ params.inner_query }}
    
    ) AS q
