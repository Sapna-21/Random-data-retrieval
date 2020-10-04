# Random Data Retrieval

In this project we have implemented an algorithm which aims to ease in the process of retrieving data randomly.

Hadoop is an open source distributed processing framework that manages data processing and storage for big data applications running in clustered systems. It is at the center of a growing ecosystem of big data technologies that are primarily used to support advanced analytics initiatives, including predictive analytics, data mining and machine learning applications. Hadoop can handle various forms of structured and unstructured data, giving users more flexibility for collecting, processing and analyzing data than relational databases and data warehouses provide.

However one limitation of Hadoop is that it only allows records to be accessed in a sequential fashion. For a very large file in which only a few records are to be fetched, this process is time consuming and slow. We have written mapreduce codes to allow selective data to be fetched in any order. This can allow the use of our algorithm in professional institutes where records of particular students or faculty is to accessed. We have encapsulated the functionality of query languages like SQL in our algorithm. 

We have used hashing techniques to improve the efficiency of algorithm. 
offset.java file has the code to create the hash file. It maps the offset value of each record with their primary key. 

Then we use this hash file to access random records in O(1) time. 
access.java file is used to serve this purpose.
