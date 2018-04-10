FROM ubuntu:latest
MAINTAINER David Rheinheimer "drheinheimer@umass.edu"

ARG VERSION=0.1

RUN apt-get update && apt-get install -y build-essential
RUN apt-get install -y glpk-utils
RUN apt-get install -y python3 python3-pip

RUN pip3 install ably boto3 requests urllib3 xlsxwriter
RUN pip3 install attrdict pendulum numpy pandas
RUN pip3 install pyomo

WORKDIR ~
ADD /model /model
WORKDIR /model
RUN python3 ./setup.py build_ext --inplace
