In the queries folder there are the MDX queries, there is also a subfolder with the equivalent queries tested with SQL.

There are 2 versions for the second query because of 2 interpretations of the query (more than a user with the highest total correct answers for each subject or just the first user that appears in the list picked in a somewhat random way between the first ranking users).

There is also a difference in terms of query efficiency, the fastest has the lowest number of output records (just one user per subject), the other produce instead more records using a trick with a filter in the topcount in the generate function.

Finally in the folder powerbi there is the file with two dashboards produced according to the information request.