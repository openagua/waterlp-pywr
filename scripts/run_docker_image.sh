#!/usr/bin/env bash
docker run --name oa-redis -d redis
docker pull openagua/waterlp-pywr:celery
docker rm --force waterlp
docker run -d --env-file ./env.list --link oa-redis:redis --volume /home/ubuntu:/home/root --volume /etc/localtime:/etc/localtime  --name waterlp openagua/waterlp-pywr
docker image prune --all --force
