FROM ubuntu:latest
MAINTAINER David Rheinheimer "drheinheimer@umass.edu"

ARG VERSION=0.1

RUN apt-get update && apt-get install -y build-essential
RUN apt-get install -y libgmp3-dev libglpk-dev glpk-utils
RUN apt-get install -y python3 python3-pip

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# install pywr
RUN wget -qO- https://github.com/pywr/pywr/archive/v0.5.1.tar.gz | tar xvz
RUN python ./pywr-0.5.1/setup.py install --with-glpk | rm -r ./pywr-0.5.1

WORKDIR /user/local
ADD /model /model
WORKDIR /model
#RUN python3 ./setup.py build_ext --inplace
