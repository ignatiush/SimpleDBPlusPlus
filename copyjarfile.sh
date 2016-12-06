#!/bin/bash

clear

echo "Creating jar"
jar cf simpledb.jar simpledb/*/*.class simpledb/*/*/*.class
echo "Moving jar"
cp simpledb.jar studentClient/simpledb/
cp simpledb.jar demoClient/simpledb/
