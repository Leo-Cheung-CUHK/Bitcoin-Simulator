#!/bin/bash

# Configure the following values
#RAPIDJSON_FOLDER=~/Documents/rapidjson-master
NS3_FOLDER=~/Documents/ns3/ns-allinone-3.28/ns-3.28

# Do not change
#mkdir $NS3_FOLDER/rapidjson
#cp  -r $RAPIDJSON_FOLDER/include/rapidjson/* $NS3_FOLDER/rapidjson/
cp  src/applications/model/* $NS3_FOLDER/src/applications/model/
cp  src/applications/helper/* $NS3_FOLDER/src/applications/helper/
cp  src/internet/helper/* $NS3_FOLDER/src/internet/helper/
cp  scratch/* $NS3_FOLDER/scratch/

cp  wscript $NS3_FOLDER/
cp  src/applications/wscript $NS3_FOLDER/src/applications/
