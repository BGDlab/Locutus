#!/bin/sh
# locutus_manout.sh
# parse the MANIFEST_OUTPUT from a processing run
# creates output of the same $1 input name, with additional suffix of .man_out.csv

#echo "hello"
infname=$1
outfname="$1.man_out.csv"

# if infname empty, bail:
if [ ${infname:="UNDEF"} == "UNDEF" ]; then
   echo "usage: $0 input_log"
   echo "will create an output of MANIFEST_OUTPUT lines into input_log.man_out.csv"
   exit -1
else
   echo "$0 parsing from ${infname} to ${outfname}..."
   cat ${infname} | grep MANIFEST_OUTPUT | sed 's/MANIFEST_OUTPUT:,//' > ${outfname}
fi


#echo "bye"
