#!/bin/bash
echo "..:: Script to copy source files/directories to another target location on sharepoint ::.."

SOURCE_DIR="./sql"
SHAREPOINT_DIR="/c/Users/byrnea2/Paddy\\ Power\\ Betfair/apb_analytics\\ -\\ Documents/Projects/Copy_Test/"

# generate and execute copy command
CP_OPTIONS="-vr"
CP_COMMAND="cp "$CP_OPTIONS" "$SOURCE_DIR$" "$SHAREPOINT_DIR
echo $CP_COMMAND
eval $CP_COMMAND