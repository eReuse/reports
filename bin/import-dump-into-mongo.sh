#!/bin/bash

find $1 -type f -iname "events.bson" -print0 | while IFS= read -r -d $'\0' line; do     echo "$line";     mongorestore --drop "$line"    ; done
