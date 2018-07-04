# EDGAR-Log-File-Missing-Data-Analysis

**EDGAR**, the **Electronic Data Gathering, Analysis, and Retrieval** system, performs automated collection,
validation, indexing, acceptance, and forwarding of submissions by companies and others who are required
by law to file forms with the **U.S. Securities and Exchange Commission** (the "SEC"). The database is freely
available to the public via the Internet (Web or FTP).

# Details
The EDGAR Log File Data Set [https://www.sec.gov/data/edgar-log-filedata-
set.html ] is analyzed and a pipeline is developed which gets data for the first day of the month(by programmatically generating the url http://www.sec.gov/dera/data/Public-
EDGAR-log-file-data/2003/Qtr1/log20030101.zip for Jan 2003 for example )for every month in year in Python and processes the file for -<br><br>
* Handle missing data<br>
* Compute summary metrices<br>
* Checks for observed anomalies<br>
* To log all the operations (with time stamps) into a log file<br>
* Compiles all the data of first day of every month for the given year into a file<br>
* The compiled data file and the log file is uploaded to the AWS S3 for the user entered details<br><br><br> 


# Docker Commands

**_Command to pull docker image :_**<br><br>
docker pull rishabhjain27/edgarlogdataset:1.0<br><br><br>
**_Command to run docker image :_**<br><br>
docker run rishabhjain27/edgarlogdataset:1.0 python3 edgarlog.py yr=2010 accessKey=**<aws_accessKey>** secretKey=**<aws_accessKey>** location=us-east-1
<br><br><br>

# Reference
https://www.sec.gov/data/edgar-log-file-data-set.html<br><br>
