#!/bin/sh

find /home/noyori/tests/cache_tile_dataset/RJTD/analysis_forecast -mindepth 6 -type d -mtime +5 | grep 20 | xargs -r rm -rf
find /home/noyori/tests/cache_tile_dataset/RJTD/grib -mindepth 3  -type d -mtime +5 | grep 20 | xargs -r rm -rf
find /home/noyori/tests/cache_tile_dataset -mindepth 5 -type d -mtime +16 | grep 20 | xargs -r rm -rf
find /home/noyori/tests/cache_tile_dataset_satellite -mindepth 5 -type d -mtime +16 | grep 20 | xargs -r rm -rf
find /home/noyori/tests/cache_4all_dataset -mindepth 5 -type d -mtime +16 | grep 20 | xargs -r rm -rf
