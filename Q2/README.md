# 5950Lab2
Lab 2: Question 2
Given a Black vehicle parking illegally at 34510, 10030, 34050 (street codes). What is the probability that it will get a ticket? (very rough prediction).


Description
Using the NYC Parking Data from 2022, the CSV file will be stripped of all data that does not contain a Vehicle Color name that is associated with the color black and that was not parked illegally at the specified street codes, 34510, 10030,34050. The KMeans algorithm is then run and the probability is calculated. 


 Running the Program
Starting from the Virtual Instance root the parking data is stored in the path: /spark-examples/lab2 and the program and test.sh file is stored in the path: /spark-examples/lab2/Question2 under the file name streetCodesKmeans.py.
