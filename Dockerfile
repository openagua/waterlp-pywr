# TODO: convert to Alpine to reduce image size
FROM ubuntu:latest
MAINTAINER David Rheinheimer "drheinheimer@umass.edu"

RUN apt-get update && apt-get install -y build-essential
RUN apt-get install -y glpk-utils
RUN apt-get install -y python3 python3-pip python3-dev
RUN python3 -m pip install --upgrade pip

ADD . /user/local/model
WORKDIR /user/local/model

# do not use "--no-cache-dir" when installing requirements
RUN pip3 install -r requirements.txt

CMD ["python3", "listen.py"]