#!/bin/sh

find download* -name index.txt -mmin +30 | xargs rm -f 
