#!/usr/bin/env bash
apt-get install -y docker-compose
docker pull openagua/waterlp-pywr:sanjoaquin
docker rm --force waterlp
wget https://github.com/openagua/waterlp-pywr/raw/sanjoaquin/docker-compose.yml
docker-compose up