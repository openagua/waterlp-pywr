#!/usr/bin/env bash
apt-get install -y docker-compose
wget https://github.com/openagua/waterlp-pywr/raw/daily/docker-compose.yml
docker-compose up