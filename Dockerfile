# Use an official Python runtime as a parent image
FROM python:3.6

# Set the working directory to /edgarlogdata
WORKDIR /edgarlogdata
# Copy the current directory contents into the container at /edgarlogdata
ADD . /edgarlogdata

# Installing packages
RUN pip3 install numpy
RUN pip3 install pandas
RUN pip3 install requests
RUN pip install boto3
RUN pip3 install lxml




























