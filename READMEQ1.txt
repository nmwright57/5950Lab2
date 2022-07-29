Lab 2: Question 1
For each player, we define the comfortable zone of shooting as a matrix of 
{SHOT_DIST, CLOSE_DEF_DIST, SHOT_CLOCK}
Please develop a K-Means algorithm to classify each player’s records into 4 comfortable zones. Consider the hit rate, which zone is the best for James Harden, Chris Paul, Stephen Curry, and Lebron James.


Description
Using the NBA Shot Logs Data on Kaggle, the CSV file will be stripped of all data that does not contain the specified player of interest’s shot distance, closest defender distance, and shot clock time, by default the player is Lebron James. The KMeans algorithm is then run and the centroids are returned. 


 Running the Program
Starting from the root of the Virtual Instance the shot logs data is stored in the path: /spark-examples/lab2 under the name shot_logs.csv and the program and test.sh file is stored in the path: /spark-examples/lab2/Question1 under the filename NBA-Kmeans-q1.py. If the comfortable zone of a player other than Lebron James needs to be found, simply change his name to any within the dataset to get that player’s comfortable zone.