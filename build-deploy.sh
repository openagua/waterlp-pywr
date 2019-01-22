#!/usr/bin/env bash
docker build -t openagua/waterlp-pywr:test .
docker push openagua/waterlp-pywr:test
