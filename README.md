# EDGAR-Log-File-Missing-Data-Analysis

**EDGAR**, the **Electronic Data Gathering, Analysis, and Retrieval** system, performs automated collection,
validation, indexing, acceptance, and forwarding of submissions by companies and others who are required
by law to file forms with the **U.S. Securities and Exchange Commission** (the "SEC"). The database is freely
available to the public via the Internet (Web or FTP).

# Details

# Docker Commands

**_Command to pull docker image_**<br><br>
docker pull rishabhjain27/edgarlogdataset:1.0<br><br><br>
**_Command to run docker image_**<br><br>
docker run rishabhjain27/edgarlogdataset:1.0 python3 edgarlog.py yr=2010 accessKey=**<aws_accessKey>** secretKey=**<aws_accessKey>** location=us-east-1
<br><br><br>

# Reference
https://www.sec.gov/data/edgar-log-file-data-set.html<br><br>
