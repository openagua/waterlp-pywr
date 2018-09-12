FROM ubuntu:16.04
MAINTAINER David Rheinheimer "drheinheimer@umass.edu"

ARG VERSION=0.1

RUN apt-get update && apt-get install -y build-essential
RUN apt-get install -y glpk-utils
RUN apt-get install -y python3 python3-pip
RUN pip3 install --upgrade pip3

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

WORKDIR /user/local
ADD /model /model
WORKDIR /model
#RUN python3 ./setup.py build_ext --inplace
