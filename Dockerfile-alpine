FROM frolvlad/alpine-miniconda3
MAINTAINER David Rheinheimer "drheinheimer@umass.edu"
ENV PYTHONUNBUFFERED=1

# bash is needed for installing pywr
RUN apk add bash
RUN conda update conda
RUN conda config --add channels conda-forge
RUN conda install pywr
RUN conda install celery redis-py
RUN conda install attrdict pendulum requests
RUN conda install boto3 s3fs
RUN pip install pubnub ably

ADD . /code
WORKDIR /code

CMD ["python3", "run.py"]