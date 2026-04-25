#!/bin/bash

echo "=============================="
echo "Running DataFrame Queries"
echo "=============================="

for i in {1..10}
do
    echo "Running q${i}_df.py"
    spark-submit ~/project/dataframe/q${i}_df.py
done

echo "=============================="
echo "Running RDD Queries"
echo "=============================="

for i in {1..10}
do
    echo "Running q${i}_rdd.py"
    spark-submit ~/project/rdd/q${i}_rdd.py
done

echo "=============================="
echo "Running SQL Queries"
echo "=============================="

for i in {1..10}
do
    echo "Running q${i}_sql.py"
    spark-submit ~/project/sql/q${i}_sql.py
done

echo "=============================="
echo "Running Optimization Scripts"
echo "=============================="

spark-submit ~/project/optimization/join_optimization.py
spark-submit ~/project/optimization/caching_test.py
spark-submit ~/project/optimization/partitioning_test.py

echo "=============================="
echo "All tasks completed"
echo "=============================="
