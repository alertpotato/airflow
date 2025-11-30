""" Набор общих SQL параметров со значениями по умолчанию и
документацией.

Используется для определения параметров DAG'а, например:
```python
DAG_DESCRIPTION = 'описание DAG'
...
@dag(
    ...
    params={
        **common_dag_params.PARAMS_DICT,
        # "dq_schema": "test_dq",
    },
    description=DAG_DESCRIPTION,
    doc_md=DAG_DESCRIPTION + common_dag_params.get_md_description(),
)
def my_dag_name():
    ...
```
"""

from utils.clean_files import PARAM_NAME as CLEAN_FILES_KEY
from utils.skipping_tasks import SKIP_TASKS_KEY

PARAMS_DICT = {
    "dry_run": False,
    CLEAN_FILES_KEY: True,
    "etl_run_id": 0,
    "period_cd": None,
    "region_cd": None,
    "ods_schema": None,
    "stage_schema": "stage",
    "dds_schema": "dds",
    "meta_schema": "meta",
    "dq_schema": "dq",
    "dma_schema": "dma",
    "dma_calc_schema": "dma_calc",
    SKIP_TASKS_KEY: []
}

_css = """
<style type="text/css">
table {
    border-collapse:collapse;
    border-spacing:0
}

table td:first-child {
    font-family: monospace;
}

table, tg, th, td {
    border: 1px solid lightgray;
    padding: 5px;
    word-break: normal;
}

td:first-child {
    text-align: right;
}
</style>
"""

_header = """Общие параметры DAG'ов: """

_params_descr_md = """
| параметр         	| описание                                                 	|
|------------------	|----------------------------------------------------------	|
| dry_run          	| если True - CustomSQLOperator в этом DAG не выполняет SQL	|
| {clean_files_key}	| выполнять ли файлы очистки повторно в конце DAG          	|
| etl_run_id       	| идентификатор ETL процесса, значение 0 - ручной запуск   	|
| period_cd        	| код периода в формате YYYYMM                             	|
| region_cd        	| код региона                                              	|
| ods_schema       	| имя схемы ods = регион данных для загрузки            	|
| stage_schema     	| имя схемы временных таблиц                            	|
| dds_schema       	| имя схемы нормализованных таблиц                      	|
| meta_schema      	| имя схемы метаданных                                  	|
| dq_schema        	| имя схемы таблиц качества данных                      	|
| dma_schema       	| имя схемы dma                                         	|
| dma_calc_schema  	| имя схемы dma_calc                                    	|
| {skip_tasks_key} 	| список task_id для пропуска                              	|
""".format(
    skip_tasks_key=SKIP_TASKS_KEY,
    clean_files_key=CLEAN_FILES_KEY
)

_footer = '''
Все они доступны в SQL скриптах через `params.`,
например: `{{ params.period_cd }}`

Подробнее:
[Подход к работе с sql в рамках etl-процессов]\
(https://wiki.element-lab.ru/pages/viewpage.action?pageId=34277044)
'''


def get_md_description():
    return '\n\n'.join(['', _css, _header, _params_descr_md, _footer])
