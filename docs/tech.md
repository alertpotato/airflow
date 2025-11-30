# Технические особенности

## Table of contents

- [YAML DAG](#yaml-dag)
- [DAG variables](#dag-variables)
- [Common DAG params](#common-dag-params)
- [Airflow variables](#airflow-variables)
- [Environment variables](#environment-variables)
 

## YAML DAG
На примере [examples/yaml_dag_example](https://git.element-lab.ru/oms-management-services/etl/airflow/-/blob/master/dags/examples/yaml_dag_example.py):

Существует два способа добавить переменные в DAG, которые будут доступны в Jinja шаблонах при рендеринге SQL файлов:
1. Статичные переменные можно добавить в секцию `variables`.
Так же этот функционал можно использовать для переопределения значений общих параметров на уровне DAG'а.
1. Переменные, значение которых нужно получить во время выполнения, можно определить в `task` (или `TaskGroup`), передав результат в аргумент `pre_processing_task` метода `yaml_reader.create_tasks(...)`.
В этом случае сохранять значения нужно в XCOM, в переменную __`sql_vars`__.\
В стандартном методе обработки шага ([process_step(...)](utils/yaml_dag/dag_defaults.py)) учитывается именно это имя. 



## DAG variables
В переменных уровня DAG (свойство `params`) можно указывать все эти переменные.\
Они же будут доступны в SQL скриптах через шаблоны Jinja: `{{ params.var_name }}`

| параметр          	| значение по умолчанию<br>у DAG созданных из YAML 	| описание              	|
|-------------------	|--------------------------------------------------	|-----------------------	|
| `dry_run`          	| `False`    	| если True - CustomSQLOperator в этом DAG не выполняет SQL 	|
| `clean_temp_tables`	| `True`    	| выполнять ли [файлы очистки](https://git.element-lab.ru/oms-management-services/etl/sql#%D1%84%D0%B0%D0%B9%D0%BB%D1%8B-%D0%BE%D1%87%D0%B8%D1%81%D1%82%D0%BA%D0%B8) повторно в конце DAG	|
| `skip_tasks`       	| `[]`       	| список task_id для пропуска                               	|
| `etl_run_id`       	| `0`       	| id группы ETL процессов, 0 если не требуется              	|
| `etl_proc_id`       	| auto       	| id этого ETL процесса, новое значение при каждом запуске  	|
| `period_cd`        	| `NULL`    	| код периода в формате YYYYMM                              	|
| `region_cd`        	| `NULL`    	| код региона                                               	|
| `period_dt`        	| auto      	| начало периода в виде строки "YYYY-MM-01"                 	|
| `temp_suffix`      	| auto      	| окончание для временных таблиц                            	|
| `ods_schema`       	| auto      	| имя схемы ods = регион данных для загрузки                	|
| `stage_schema`     	| `stage`    	| имя схемы временных таблиц                                	|
| `dds_schema`       	| `dds`     	| имя схемы нормализованных таблиц                           	|
| `meta_schema`      	| `meta`    	| имя схемы метаданных                                      	|
| `dq_schema`        	| `dq`      	| имя схемы таблиц качества данных                           	|
| `dma_schema`       	| `dma`     	| имя схемы витрин                                           	|
| `dma_calc_schema`  	| `dma_calc`	| имя схемы предрасчетных витрин                            	|


### Автоматизация
Некоторым параметрам устанавливаются значения по умолчанию на основе других:

- `period_dt` = `"YYYY-MM-01"` из `period_cd`, если указан
- `temp_suffix` = `"{region_cd}_{period_cd}_stage_1"`, если указаны `period_cd` и `region_cd`
- `ods_schema` = `"ods_{region_cd}"`, если указан `region_cd`
- `etl_run_id` = по умолчанию `0`, может браться из головного DAG'а
- `etl_proc_id` - генерируется автоматически при запуске

### Common DAG params
Наиболее простой и удобный способ добавить все доступные параметры с описаниями к ним:
```python
from utils import common_dag_params
@dag(
    ...
    params=common_dag_params.PARAMS_DICT,
    description=DAG_DESCRIPTION,
    doc_md=DAG_DESCRIPTION + common_dag_params.get_md_description(),
    ...
)
def mydag():
    ...
```



## Airflow variables

Все переменные Airflow - не обязательные, на случай их отсутствия есть значения по умолчанию.

| имя переменной            	| описание                                                                          	| значение по умолчанию                                                	|
|---------------------------	|-----------------------------------------------------------------------------------	|----------------------------------------------------------------------	|
| `debug-mode`              	| при значении "`on`" функция `is_debug_mode()` (`utils/common.py`) возвращает True 	| нет, функция возвращает False                                        	|
| `log_conn_id`             	| имя подключения Airflow (`conn_id`) для внутренней системы логирования            	| из переменной `DEFAULT_CONN_ID` в `utils/logs_meta/functions.py`  	|
| `debug_logs_meta`         	| при значении "`on`" в лог задачи будут выводиться sql-запросы модуля logs_meta    	| нет                                                                  	|
| `table_depends.hours`     	| количество часов по умолчанию для модуля зависимостей таблиц (float значение)     	| 24                                                                   	|
| `audit_enabled`           	| при значении "`off`" сообщения аудита перестают отправляться                      	| `on`                                                              	|
| `dq_conn_id`              	| имя подключения Airflow (`conn_id`) для выполнения DQ запросов                    	| из переменной `DEFAULT_DQ_CONN_ID` в `utils/yaml_dag/yaml_reader.py` 	|
| `dq_sql_rule_files_directory`	| относительный путь к SQL файлам DQ правил (от корня DQ репозитория)                  	| "sql/RULES"                                                       	|
| `debug_logs_dq`           	| отладочные логи для модуля DQ                                                     	| нет                                                                  	|

## Environment variables

| имя переменной  	| описание                                                                                                                                                                                                                                           	|
|-----------------	|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| `PYTHONPATH`    	| должна указывать на ту же директорию, что и [AIRFLOW_HOME](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#envvar-AIRFLOW_HOME), без этой переменной не будут работать import'ы нашего кода (например utils/) 	|
| `SQL_REPO_PATH` 	| Путь к репозиторию SQL: [git.element-lab.ru/../sql](https://git.element-lab.ru/oms-management-services/etl/sql)                                                                                                                                    	|
| `DQ_REPO_PATH`  	| Путь к репозиторию Data Quality: [git.element-lab.ru/../data-quality](https://git.element-lab.ru/oms-management-services/etl/data-quality)                                                                                                         	|

