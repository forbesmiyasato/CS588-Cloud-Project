#!/bin/bash


for FILE in /tmp/Eurovision-files/*
do
    echo "Reformating $FILE"
    python3 reformat_json.py --file $FILE
done

echo "Done"
