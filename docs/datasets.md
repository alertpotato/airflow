# Взаимодействие с наборами данных

## в YAML ([Генерация DAG из YAML-файла](https://git.element-lab.ru/oms-management-services/etl/sql#%D0%B3%D0%B5%D0%BD%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D1%8F-dag-%D0%B8%D0%B7-yaml-%D1%84%D0%B0%D0%B9%D0%BB%D0%B0))

В любой шаг можно добавить ключ `dataset` с именем (строка или [URI](https://ru.wikipedia.org/wiki/URI)), тогда успешное выполнение этого шага так же будет генерировать событие обновления Dataset'а.
[Пример обработки событий](#примеры).

На момент актуализации этой страницы не реализован способ передачи доп.данных в событие.


## Примеры

Рассмотрим тестовые DAG из директории [dags/examples/datasets](https://git.element-lab.ru/oms-management-services/etl/airflow/-/tree/production/dags/examples/datasets):

### `ds_producer_dag.py`
- создаёт 5 параллельных задач, каждая из которых генерирует событие обновления Dataset'а с именем `example_ds_item` и сохраняет в событие данные (регион и период).
- после их успешного завершения - запускается ещё одна пустая задача, генерирующая обновление Dataset'а с именем `example_ds_finish`

### `ds_consumer_dag.py`
- настроен на запуск только при наличии хотя бы одного обновления в каждом из двух Dataset'ов: `example_ds_item` и `example_ds_finish`.
- выводит имя Dataset'а, данные в нём и dagrun, где создалось событие.

Таким образом DAG, обрабатывающий события (`ds_consumer_dag` в данном примере) будет запускаться только после успешной обработки параллельных задач в `ds_producer_dag`.

## Логические операторы в привязке DAG к событиям Dataset

Начиная с Airflow 2.9 для привязки DAG к событиям Dataset можно использовать логические операторы `|` (ИЛИ) и `&` (И) в парамете `schedule`.

Например следующий DAG будет запущен если будет хотя бы одно событие в `dataset1` или `dataset2`, а так же хотя бы одно в `dataset3` или `dataset4`

```python
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=(
        (Dataset("dataset1") | Dataset("dataset2"))
        & (Dataset("dataset3") | Dataset("dataset4"))
    ),  # NOTE: для поддержки операторов нужно использовать (), вместо []!
    catchup=False
)
def downstream2_one_in_each_group():
    ...
```