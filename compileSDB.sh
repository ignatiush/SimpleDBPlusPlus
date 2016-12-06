#!/bin/bash

clear
echo "Compiling simpledb"
javac -cp . simpledb/*/*.java simpledb/*/*/*.java
echo "Compiling studentClient"
javac -cp . studentClient/simpledb/*.java
echo "Compiling demoClient"
javac -cp . demoClient/simpledb/*.java
