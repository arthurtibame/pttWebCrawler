#!/bin/bash

docker-compose -f docker-compose-pttcrawler.yml up --build -d

sleep 5s

sh ngurl.sh
