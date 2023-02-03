#!/bin/bash

if [ "${1}" == "" ]
then
  echo "Usage: ${0} <properties file>"
  exit 1
fi

java -jar target/producer-test-0.0.3.jar "${1}"
