# Semester Project: Model-Based Approximate Query Processing #
This is the repository for the semester project "Model-Based Approximate Query Processing" at DIAS at EPFL in the spring semester.

## Introduction
This project explores the possibilities to build sample-less model-based AQP methods based on the statistics of the data, where three model-based approach are implemented with kernel density estimation (KDE): 2D-KDE, regression-based KDE and weighted KDE. TPC-H benchmark is used to compare the methods along with stratified sampling. Weighted KDE arguably shows the best performance as it is fast to construct, uses small memory space, responses swiftly and yields accurate result. It can outperform the stratified sampling method in some cases, but there are also cases where the KDE-based method encounter difficulties. Stratified sampling may after all be a suitable method for some AQP tasks.


## Files
`Query 1.ipynb`: Implemtation of query 1 of TPC-H benchmark. Exact query, stratified sampling, 2D-KDE, regression-based KDE and weighted KDE are implemented and compared. The effect of kernel bandwidth and selectivity is discussed. 

`Query 5.ipynb`: Implemtation of query 5 of TPC-H benchmark. Exact query, stratified sampling and weighted KDE are implemented. The effect of model dispersion is discussed. 

`Query 6.ipynb`: Implemtation of query 6 of TPC-H benchmark. Exact query, stratified sampling and weighted KDE are implemented. The effect of dimesnionality is discussed. 

`Query 10.ipynb`: Implemtation of query 10 of TPC-H benchmark. Exact query, stratified sampling and weighted KDE are implemented. The effect of the number of groups in the query is discussed. 

`stratified_sampling.py`: Generation of a stratified sample given a dataset and its corresponding QCS. 


## Main Python packages required
Scipy, Numpy, PySpark, matplotlib, pandas
