#!/usr/bin/env bash
docker run --name redis -p 6379:6379 -d redis
docker pull openagua/waterlp-pywr:celery
docker rm --force waterlp
docker run -d --env-file ./instance/env.list --link redis:redis --volume /home/ubuntu:/home/root --volume /etc/localtime:/etc/localtime  --name waterlp openagua/waterlp-pywr:celery
