#!/bin/sh
# locutus_utcout.sh
# parse the UTC timing output from a processing run
# creates output of the same $1 input name, with additional suffix of .utc_out.csv

#echo "hello"
infname=$1
outfname="$1.utc_out.csv"

# if infname empty, bail:
if [ ${infname:="UNDEF"} == "UNDEF" ]; then
   echo "usage: $0 input_log"
   echo "will create an output of \"@\" UTC timing lines into input_log.utc_out.csv"
   exit -1
else
   echo "$0 parsing from ${infname} to ${outfname}..."
   cat ${infname} | grep "@" > ${outfname}
fi


#echo "bye"
