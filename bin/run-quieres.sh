#!/bin/bash

# $1 queries script
# $2 output file

mongo --quiet < $1 > $2
