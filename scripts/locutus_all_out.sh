#!/bin/sh
# locutus_all_out.sh
# a wrapper script to call the other post-processing parsing scripts of:
#	locutus_manout.sh
#	locutus_cfgout.sh
#	locutus_utcout.sh
# creates output of the same $1 input name, with additional suffix of from each.....


#echo "hello"
infname=$1
#outfname="$1.utc_out.csv"

# if infname empty, bail:
if [ ${infname:="UNDEF"} == "UNDEF" ]; then
   echo "usage: $0 input_log"
   echo "will create an output of MANIFEST_OUTPUT, CFG_OUT, and \"@\" UTC timing lines into the respective input_log.[man|cfg|utc]_out.csv"
   exit -1
else
   echo "$0 parsing from ${infname}..."
   # WARNING: the following are all hardcoded AS IF sitting at the locutus repo's top level
   # TODO: resolve to something a bit more robust
   ./scripts/locutus_manout.sh $1
   ./scripts/locutus_cfgout.sh $1
   ./scripts/locutus_utcout.sh $1
fi


#echo "bye"
