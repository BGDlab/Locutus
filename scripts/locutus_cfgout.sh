#!/bin/sh
# locutus_cfgout.sh
# parse the CFG_OUTPUT from a processing run
# creates output of the same $1 input name, with additional suffix of .cfg_out.csv

#echo "hello"
infname=$1
outfname="$1.cfg_out.csv"

# if infname empty, bail:
if [ ${infname:="UNDEF"} == "UNDEF" ]; then
   echo "usage: $0 input_log"
   echo "will create an output of CGF_OUT lines into input_log.cfg_out.csv"
   exit -1
else
   echo "$0 parsing from ${infname} to ${outfname}..."
   cat ${infname} | grep CFG_OUT | sed 's/CFG_OUT:,//' > ${outfname}
fi


#echo "bye"
