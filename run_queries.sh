#!/bin/bash

for FILE in ./queries/*
do
    echo running $FILE
    python3 $FILE
done

