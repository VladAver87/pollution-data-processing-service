# pollution-data-processing-service
#### Тестовое задание на позицию BigData Developer
##### Условия задачи:
> В рамках реализации Задачи, состоящего из 4х этапов, предполагается сбор данных из источников, 
> с целью использования в дальнейших расчетах, которые будут содержать данные о звонках/сообщениях в рабочей/жилой зонах, 
> а так же уровень загрязнения в данных зонах.

##### Форматы данных.
```
Имеются три источника данных: два datasets и карта-сетка Милана.
Datasets: 
TELECOMMUNICATIONS_MI - В данном dataset хранятся данные о звонках/сообщениях  и интернет активности за два месяца(11.2013, 12.2013)
MI_POLLUTION - В данном dataset хранятся данные о расположении датчиков загрязнения и описание этих датчиков.
MI_POLLUTION подразделяется  на две папки:
POLLUTION_MI 	- Датчики
LEGEND		- Описание датчиков
Карта-сетка Милана:
MI_GRID - Карта – сетка представлена в формате geojson и имеет следующую структуру:
FeatureCollection:
Features
Geometry
5x lat/long (4 основные коордианты + 1 замыкающая)
Properties
cellId (Square id)
```
###### Таблица 1. Dataset TELECOMMUNICATIONS_MI
<img width="482" alt="image" src="https://user-images.githubusercontent.com/44060213/217475380-ec566246-ca33-428c-9a6f-98b643d34fae.png">

###### Таблица 2. Dataset MI_POLLUTION/LEGEND
<img width="524" alt="image" src="https://user-images.githubusercontent.com/44060213/217476326-a64f88ab-ba1b-4715-9b56-831e8fa5ce5c.png">

###### Таблица 3. Dataset MI_POLLUTION/POLLUTION_MI
<img width="482" alt="image" src="https://user-images.githubusercontent.com/44060213/217476626-4c88850f-9f0c-427b-b06b-c2135bd82b58.png">

###### Описание этапов Задачи
<img width="484" alt="image" src="https://user-images.githubusercontent.com/44060213/217476892-fa1fd2dc-03fa-478d-8cac-6fb58feabb0a.png">

> В результате выполнения данной Задачи необходимо составить отчет со следующими данными: 
> - Топ 5 «загрязненных» рабочих и спальных зон.
> - Топ 5 «чистых» рабочих и спальных зон.
> - список не определенных по классу или по уровню загрязнения зон
> В отчете сохранить данные о концентрации веществ, количестве датчиков в зоне и пороговых значениях концентрации и активности.

#### Описание решения
###### Stage 1
[UserActivitiesService](https://github.com/VladAver87/pollution-data-processing-service/blob/main/src/main/scala/com/vladaver/data_processing/service/impl/UserActivitiesServiceImpl.scala)
* Группируем данные об активностях по времени, для определения активности в рабочие часы
```
+---------+------------------------------------------+-----------------+
|square_id|time_interval                             |total_activity   |
+---------+------------------------------------------+-----------------+
|1        |{2013-12-01 01:00:00, 2013-12-01 09:00:00}|315.0508365351707|
|1        |{2013-12-01 09:00:00, 2013-12-01 17:00:00}|610.5464986870065|
|1        |{2013-12-01 17:00:00, 2013-12-02 01:00:00}|750.0730889966944|
|1        |{2013-12-02 01:00:00, 2013-12-02 09:00:00}|160.0209963992238|
+---------+------------------------------------------+-----------------+
```
* Группируем общую активность по рабочему и не рабочему времени
```
+---------+-----------------+---------------+
|square_id|total_activity   |is_working_time|
+---------+-----------------+---------------+
|1        |750.0730889966944|false          |
|1        |315.0508365351707|false          |
|1        |610.5464986870065|true           |
|1        |160.0209963992238|false          |
+---------+-----------------+---------------+
```
* Аггрегируем рабочую и общую активность
```
+---------+-----------------------+------------------+
|square_id|total_worktime_activity|total_activity    |
+---------+-----------------------+------------------+
|1        |610.5464986870065      |1835.6914206180954|
|2        |613.1115720239468      |1842.7209470512462|
|3        |615.8419946320355      |1850.203611407429 |
|4        |603.1167134474963      |1815.3302443632856|
```
* Выясняем рабочий район или нет по max_total_activity < total_worktime_activity < avg_total_activity
```
+---------+-----------------------+------------------+------------------+------------------+------------------+--------------+
|square_id|total_worktime_activity|total_activity    |max_total_activity|min_total_activity|avg_total_activity|is_work_square|
+---------+-----------------------+------------------+------------------+------------------+------------------+--------------+
|1        |610.5464986870065      |1835.6914206180954|361709.42956550827|33.97265310480725 |9315.355801071053 |false         |
|2        |613.1115720239468      |1842.7209470512462|361709.42956550827|33.97265310480725 |9315.355801071053 |false         |
|3        |615.8419946320355      |1850.203611407429 |361709.42956550827|33.97265310480725 |9315.355801071053 |false         |
```
* Результат
```
+---------+--------------+
|square_id|is_work_square|
+---------+--------------+
|1        |false         |
|2        |false         |
|3        |false         |
```
###### Stage 2
[Парсинд координат](https://github.com/VladAver87/pollution-data-processing-service/blob/main/src/main/scala/com/vladaver/data_processing/service/impl/GeoServiceImpl.scala)
[Общий pipeline](https://github.com/VladAver87/pollution-data-processing-service/blob/main/src/main/scala/com/vladaver/data_processing/Pipeline.scala)
* Объединяем с картой-сеткой для ассоциации районов и их координат
```
+---------+--------------+------------------------------------------------------------------------------------+
|square_id|is_work_square|coordinates                                                                         |
+---------+--------------+------------------------------------------------------------------------------------+
|1        |false         |[9.011491, 45.358803, 9.014491, 45.358803, 9.014491, 45.356686, 9.011491, 45.356686]|
|2        |false         |[9.014491, 45.358803, 9.017492, 45.3588, 9.017491, 45.356686, 9.014491, 45.356686]  |
|3        |false         |[9.017492, 45.3588, 9.020493, 45.3588, 9.020492, 45.356686, 9.017491, 45.356686]    |
|4        |false         |[9.020493, 45.3588, 9.023493, 45.3588, 9.023492, 45.356686, 9.020492, 45.356686]    |
```
###### Stage 3
[PollutionDataService](https://github.com/VladAver87/pollution-data-processing-service/blob/main/src/main/scala/com/vladaver/data_processing/service/impl/PollutionDataServiceImpl.scala)
* Объединяем легенду с показаниями датчиков по загрязнениям
```
+---------+---------------+----------------------------+----------+-----------+-----------------+
|sensor_id|sum_measurement|sensor_street_name          |sensor_lat|sensor_long|sensor_type      |
+---------+---------------+----------------------------+----------+-----------+-----------------+
|17126    |209            |Milano -via Carlo Pascal    |45.47845  |9.235016   |Benzene          |
|17127    |856            |Milano - viale Marche       |45.496067 |9.193023   |Benzene          |
|10278    |120754         |Milano -via Carlo Pascal    |45.47845  |9.235016   |Total Nitrogen   |
|10279    |50071          |Milano -via Carlo Pascal    |45.47845  |9.235016   |Nitrogene Dioxide|
```
###### Stage 4
[Общий pipeline](https://github.com/VladAver87/pollution-data-processing-service/blob/main/src/main/scala/com/vladaver/data_processing/Pipeline.scala)
* Объеденяем данные по районам и загрязнениям через вхождение координат точки датчика в координаты района [Функция isPointInSquare](https://github.com/VladAver87/pollution-data-processing-service/blob/main/src/main/scala/com/vladaver/data_processing/utils/Utils.scala)
> Так как датчиков мало, координаты районов были расширены на 10% для обработки случая, если датчик находится близко к границе раздела районов
* Формируем отчеты по задаче [Общий pipeline](https://github.com/VladAver87/pollution-data-processing-service/blob/main/src/main/scala/com/vladaver/data_processing/Pipeline.scala)

```
- Топ 5 «загрязненных» рабочих
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+---------------------------------------------------------------------+
|square_id|coordinates                                                                        |total_concentration|sensors_count|sensors_type                                                         |
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+---------------------------------------------------------------------+
|5362     |[9.194902, 45.470745, 9.197909, 45.47074, 9.197902, 45.468624, 9.194896, 45.46863] |220932             |5            |Benzene,Carbon Monoxide ,Nitrogene Dioxide,Total Nitrogen,BlackCarbon|
|5462     |[9.19491, 45.472862, 9.197916, 45.47286, 9.197909, 45.47074, 9.194902, 45.470745]  |220932             |5            |Benzene,Carbon Monoxide ,Nitrogene Dioxide,Total Nitrogen,BlackCarbon|
|5644     |[9.140804, 45.477173, 9.14381, 45.47717, 9.1438055, 45.47505, 9.140799, 45.475056] |203894             |4            |Total Nitrogen,Carbon Monoxide ,Nitrogene Dioxide,Benzene            |
|5645     |[9.14381, 45.47717, 9.146817, 45.477165, 9.1468115, 45.475048, 9.1438055, 45.47505]|203894             |4            |Total Nitrogen,Carbon Monoxide ,Nitrogene Dioxide,Benzene            |
|6561     |[9.191982, 45.496136, 9.19499, 45.496128, 9.194983, 45.494015, 9.191976, 45.49402] |183339             |4            |Nitrogene Dioxide,Carbon Monoxide ,Benzene,Total Nitrogen            |
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+---------------------------------------------------------------------+

- Топ 5 «загрязненных» спальных
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+----------------------------------------------------------------------------------+
|square_id|coordinates                                                                        |total_concentration|sensors_count|sensors_type                                                                      |
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+----------------------------------------------------------------------------------+
|6661     |[9.19199, 45.49825, 9.194998, 45.498245, 9.19499, 45.496128, 9.191982, 45.496136]  |183339             |4            |Nitrogene Dioxide,Carbon Monoxide ,Benzene,Total Nitrogen                         |
|5775     |[9.23402, 45.479134, 9.237027, 45.47913, 9.237019, 45.477013, 9.234012, 45.47702]  |182494             |7            |Benzene,Nitrogene Dioxide,Sulfur Dioxide ,Ammonia,BlackCarbon,Total Nitrogen,Ozone|
|6379     |[9.246102, 45.491802, 9.24911, 45.491795, 9.249101, 45.489677, 9.246094, 45.489685]|118970             |3            |Total Nitrogen,Nitrogene Dioxide,Ozono                                            |
|6479     |[9.246112, 45.493916, 9.24912, 45.493908, 9.24911, 45.491795, 9.246102, 45.491802] |118970             |3            |Total Nitrogen,Nitrogene Dioxide,Ozono                                            |
|3455     |[9.173735, 45.43059, 9.176739, 45.430584, 9.176732, 45.42847, 9.173728, 45.428474] |111883             |1            |Total Nitrogen                                                                    |
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+----------------------------------------------------------------------------------+

- Топ 5 «чистых» спальных
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+----------------------------------------------------------------------------------+
|square_id|coordinates                                                                        |total_concentration|sensors_count|sensors_type                                                                      |
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+----------------------------------------------------------------------------------+
|3455     |[9.173735, 45.43059, 9.176739, 45.430584, 9.176732, 45.42847, 9.173728, 45.428474] |111883             |1            |Total Nitrogen                                                                    |
|3555     |[9.173741, 45.432705, 9.176745, 45.4327, 9.176739, 45.430584, 9.173735, 45.43059]  |111883             |1            |Total Nitrogen                                                                    |
|6379     |[9.246102, 45.491802, 9.24911, 45.491795, 9.249101, 45.489677, 9.246094, 45.489685]|118970             |3            |Total Nitrogen,Nitrogene Dioxide,Ozono                                            |
|6479     |[9.246112, 45.493916, 9.24912, 45.493908, 9.24911, 45.491795, 9.246102, 45.491802] |118970             |3            |Total Nitrogen,Nitrogene Dioxide,Ozono                                            |
|5775     |[9.23402, 45.479134, 9.237027, 45.47913, 9.237019, 45.477013, 9.234012, 45.47702]  |182494             |7            |Benzene,Nitrogene Dioxide,Sulfur Dioxide ,Ammonia,BlackCarbon,Total Nitrogen,Ozone|
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+----------------------------------------------------------------------------------+

- Топ 5 «чистых» рабочих
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+---------------------------------------------------------+
|square_id|coordinates                                                                        |total_concentration|sensors_count|sensors_type                                             |
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+---------------------------------------------------------+
|5062     |[9.1948805, 45.4644, 9.197886, 45.464394, 9.19788, 45.46228, 9.194874, 45.462284]  |143163             |3            |Nitrogene Dioxide,Ozono,Total Nitrogen                   |
|4154     |[9.170775, 45.4454, 9.17378, 45.445396, 9.173774, 45.443283, 9.170769, 45.443287]  |179762             |3            |Total Nitrogen,Carbon Monoxide ,Nitrogene Dioxide        |
|6561     |[9.191982, 45.496136, 9.19499, 45.496128, 9.194983, 45.494015, 9.191976, 45.49402] |183339             |4            |Nitrogene Dioxide,Carbon Monoxide ,Benzene,Total Nitrogen|
|5644     |[9.140804, 45.477173, 9.14381, 45.47717, 9.1438055, 45.47505, 9.140799, 45.475056] |203894             |4            |Total Nitrogen,Carbon Monoxide ,Nitrogene Dioxide,Benzene|
|5645     |[9.14381, 45.47717, 9.146817, 45.477165, 9.1468115, 45.475048, 9.1438055, 45.47505]|203894             |4            |Total Nitrogen,Carbon Monoxide ,Nitrogene Dioxide,Benzene|
+---------+-----------------------------------------------------------------------------------+-------------------+-------------+---------------------------------------------------------+

- Список «неопределенных»
+---------+------------------------------------------------------------------------------------+
|square_id|coordinates                                                                         |
+---------+------------------------------------------------------------------------------------+
|1        |[9.011491, 45.358803, 9.014491, 45.358803, 9.014491, 45.356686, 9.011491, 45.356686]|
|2        |[9.014491, 45.358803, 9.017492, 45.3588, 9.017491, 45.356686, 9.014491, 45.356686]  |
|3        |[9.017492, 45.3588, 9.020493, 45.3588, 9.020492, 45.356686, 9.017491, 45.356686]    |
|4        |[9.020493, 45.3588, 9.023493, 45.3588, 9.023492, 45.356686, 9.020492, 45.356686]    |
|5        |[9.023493, 45.3588, 9.026493, 45.3588, 9.026492, 45.35668, 9.023492, 45.356686]     |
|6        |[9.026493, 45.3588, 9.029493, 45.3588, 9.029492, 45.35668, 9.026492, 45.35668]      |
|7        |[9.029493, 45.3588, 9.032495, 45.3588, 9.032493, 45.35668, 9.029492, 45.35668]      |
|8        |[9.032495, 45.3588, 9.035495, 45.358795, 9.035493, 45.35668, 9.032493, 45.35668]    |
|9        |[9.035495, 45.358795, 9.038495, 45.358795, 9.038493, 45.35668, 9.035493, 45.35668]  |
|10       |[9.038495, 45.358795, 9.041495, 45.358795, 9.041494, 45.356678, 9.038493, 45.35668] |
.....etc
```
