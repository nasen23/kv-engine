#!/usr/bin/env bash

for skew in 0 1
do
    for ratio in 0 50 100
    do
        for threads in 1 2 4 8 16 32
        do
            ./bench $threads $ratio $skew | tee ${threads}_${ratio}_${skew}.txt
            rm -rf data/*
        done
    done
done
