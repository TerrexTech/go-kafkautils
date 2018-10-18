#!/usr/bin/env bash

cd test
echo "===> Changing directory to \"./test\""

echo "===> Running Zookeeper and Kafka"
docker-compose up -d --build --force-recreate zookeeper kafka

echo "===> Running test-container"
docker-compose up go-kafkautils-test
exit $?
