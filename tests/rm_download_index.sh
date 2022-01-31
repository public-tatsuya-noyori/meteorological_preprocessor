#!/bin/sh

find download* -name index.txt -mmin +10 | xargs rm -f 
