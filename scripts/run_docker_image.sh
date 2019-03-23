#!/usr/bin/env bash
docker run --name redis -p 6379:6379 -d redis
docker pull openagua/waterlp-pywr:sanjoaquin
docker rm --force waterlp
docker run -d --env-file ./variables.env --link redis:redis --volume /home/ubuntu:/home/root --volume /etc/localtime:/etc/localtime  --name waterlp openagua/waterlp-pywr:sanjoaquin
