#!/bin/bash

all_start_time=`date +%s`
for FILE in /tmp/Eurovision-files/*
do
    echo "Start loading data from $FILE"
    # UM_JSON_OBJECTS=`cat $FILE | jq length`
    # echo "$NUM_JSON_OBJECTS JSON Objects in $FILE"
    # start_time=`date +%s`
    # python3 load_tweet_data.py --file $FILE
    # python3 load_user_data.py --file $FILE
    # python3 load_hashtag_data.py --file $FILE
    # python3 load_country_data.py --file $FILE
    # end_time=`date +%s`
    # echo total loading time for $FILE was `expr $end_time - $start_time` s.
    python3 load_all_data.py --file $FILE
done
echo "Done"
all_end_time=`date +%s`
echo total loading time for all files was `expr $all_end_time - $all_start_time` s.
