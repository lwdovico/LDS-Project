# Student Assesment Datawarehouse
 Repository of a Data Science Project

# Part I
This part covers the ETL and integration of the data sources into a datawarehouse. As tools we employed SQL Server Management Studio, while most of the work was done directly in python code.

Here's the schema we built from the information that was available.


<img src="https://raw.githubusercontent.com/lwdovico/LDS-Project/main/Part%20I/schema.png" alt="Schema" width="1000">

The python script to run is "LDS Project (Part I).py", we also kept the jupyter notebook "... - Data Integration Notebook.ipynb" we used to write and test the functions. Some outputs are visible and the order of cells and functions is divided by topic and it follows the one of develepoment, thus it is slightly easier to read than the *.py file.

The credentials.txt is a file containing the IP address of the server and the credentials, it was done a security measure to keep the code safe to be shared and uploaded later.

The file must be in the same folder of the script for it to run without problems.

In a later update we updated the code to upload in batches manually, reducing the execution time to 2 minutes.

<img src="https://raw.githubusercontent.com/lwdovico/LDS-Project/main/Part%20I/Execution%20Time.png" alt="Execution time" width="1000">

# Part II

In this part we automated three tasks of data collection from the datawarehouse we mantain. The tool we used was SQL Server Integration Services (SSIS)</br>

For every subject, the number of correct answers of male and female students: </br>
<img src="https://raw.githubusercontent.com/lwdovico/LDS-Project/main/Part%20II/_Images/Assignment_0.png" alt="Execution time" width="500"></br></br>
A  subject is said to be easy if it has more than 90% correct answers, while it is said to be hard if it has less than 20% correct answers. List every easy and hard subject, considering only subjects with more than 10 total answers. </br></br>
<img src="https://raw.githubusercontent.com/lwdovico/LDS-Project/main/Part%20II/_Images/Assignment_1.png" alt="Execution time" width="700"></br></br>
For each country, the student or students that answered the most questions correctly for that country. </br>
<img src="https://raw.githubusercontent.com/lwdovico/LDS-Project/main/Part%20II/_Images/Assignment_2.png" alt="Execution time" width="500">

# Part III

We built an OLAP cube to further provide a multidimensional analysis and build some interactive dashboards. The tools we used were Microsoft Analysis Services (to build the cube), MDX to answer the business questions and PowerBI to provide the dashboards.

The business questions we needed to answer to where the following:

- Show the student that made the most mistakes for each country
- For each subject, show the student with the highest total correct answers
- For each contintent, show the student with the highest ratio between his total correct answers and the average correct answers of that continent

Furthermore with reference to the dashboard the most important need was to provide geographical informations about correct and wrong answer.

Here we provide a screenshot of the interface we deployed:

<img src="https://raw.githubusercontent.com/lwdovico/LDS-Project/main/Part%20III/powerbi/Dashboard_screen.png" alt="Dashboard" width="700">
