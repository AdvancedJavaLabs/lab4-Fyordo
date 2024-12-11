#!/bin/bash

# Удаление предыдущего результата
rm -rf ./output_unordered
rm -rf ./input_unordered
rm -rf ./output
mkdir input_unordered

# Запуск Hadoop задачи с пользовательскими параметрами
hadoop jar category-hadoop/target/category-hadoop-1.0-SNAPSHOT.jar

# Извлечение результата
hdfs dfs -get ./output_unordered/part-r-00000 ./input_unordered/result.csv

hadoop jar sorting-hadoop/target/sorting-hadoop-1.0-SNAPSHOT.jar

hdfs dfs -get ./output/part-r-00000 ./output/result.csv

echo "Результат сохранён в ./output/result.csv"
