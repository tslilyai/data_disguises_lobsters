#!/bin/sh

FILENAME=$1

curl -F file=@$FILENAME.pdf \
       -F key=`cat api.key` \
       https://6824.scripts.mit.edu/2020/handin.py/upload
