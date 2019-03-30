# TODO: convert to Alpine to reduce image size
FROM ubuntu:18.04
MAINTAINER David Rheinheimer "drheinheimer@umass.edu"
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y build-essential
RUN apt-get install -y glpk-utils
RUN apt-get install -y python3 python3-pip python3-dev
RUN python3 -m pip install --upgrade pip

COPY requirements.txt /
RUN pip3 install -r /requirements.txt

COPY . /app
WORKDIR /app

CMD ["python3", "run.py"]