#!/bin/bash

if [ "${1}" == "" ]
then
  echo "Usage: ${0} <properties file>"
  exit 1
fi

java -cp "target/*:target/dependencies/*" com.github.dhoard.kafka.ProducerTest "${1}"
