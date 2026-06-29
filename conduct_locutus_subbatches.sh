#!/bin/bash
# conduct_locutus_subbatches.sh
###################################################
# 18 June 2026: with PR into dev_bgd_lab from wip_batches_manifest_once branch, updating
#     arg_branch='dev_bgd_lab' (WAS: arg_branch='wipbranch') for hardcoded assist to the deploy script.
# TODO: introduce cmd-line argument for arg_branch
#
# 13 May 2026: added support for manifest-free Batch deployments via the following args:
#   -B batch_name to indicate manifest-free
#       BATCH NOTE: this will automatically set: locutus_load_batch_from_DB_bypass_CSV_manifest
#       as well as the batch_name itself, as via: locutus_batch_name
#   -S Subtotal for Batch (batch size *following* any batch_filters from the current config.yaml)
#   -F corresponding to locutus_batch_filter_statuses for a single manifest_status
# and using existing:
#   -N num_containers
#   -r range of containers to deploy on this VM
#   -dD actually deploy, and pre-Delete containers
#
# For example, to Conduct 2,830 accessions currently in batch30 of the configured workspace
# with manifest_status=ZZZ-ONDECK-4-PROCESSING_CHANGE:Viveks_OnPrem_scit605_batch30default
# and deploy across 8 different baby Locutus containers, all on this node:
#      ./conduct_locutus_subbatches.sh -dD -B batch30 -S 2830 \
#        -N 8 -r 1:8 -m r3m0_test_butterball_onprem.csv -s scit605ConductTest05 \
#       -F ZZZ-ONDECK-4-PROCESSING_CHANGE:Viveks_OnPrem_scit605_batch30default
# Until such time that the Conductor is integrated into the Locutus DB, the -Subtotal value (e.g. 2,830 in this case)
# will need to come from the user via a Summarizer report,
# or via direct SQL query of the DB, such as via:
#   SELECT manifest_status, COUNT(*), MIN(last_datetime_processed), MAX(last_datetime_processed)
#       FROM  onprem_dicom_ws_onprem_bgd_scit605_2025_manifest
#       WHERE active and batch_name='batch30'
#       GROUP BY manifest_status ORDER BY manifest_status;
#
# Further NOTES:
# Also exporting new LOCUTUS_CONDUCTOR_BATCH_FILTER_ARGS env var to support the Conductor's new Batch Filter manifest-once parms
# into the downstream deploy_locutus_* and subsequent scripts/run_docker_* (currently only those specific to the wipbranch_onprem)
# TODO: once tested in the WIPbranch, add to all other deploy_locutus_* and subsequent scripts/run_docker_*
#
# Example LOCUTUS_CONDUCTOR_BATCH_FILTER_ARGS:
#   -e locutus_load_batch_from_DB_bypass_CSV_manifest=true \
#   -e locutus_batch_name=bogus_batch \
#   -e locutus_batch_filter_counter_range 1:27 \
#   -e locutus_batch_filter_statuses=ZZZ-ONDECK-4-PROCESSING_CHANGE:Viveks_OnPrem_scit605_batch30default
#
# BEWARE: locutus_load_batch_from_DB_bypass_CSV_manifest is only set via the -B batch_name, and then, only to true
# One might imagine a case wherein the desired Conductor is to use a manifest, but that the config.yaml contains the following sort of lines:
#   locutus_load_batch_from_DB_bypass_CSV_manifest: True
#   locutus_batch_name: batch30
# without the Conductor/deploy/run_docker scripts explicitly overriding locutus_load_batch_from_DB_bypass_CSV_manifest to False,
# Locutus will still be attempting to operate manifest-once from the (potentially already preloaded) batch30 Batch.  As such....
#
#       General CAVEAT EMPTOR:
#       The Conductor utilizes the pre-existing config.yaml, and only overrides particular configuration parameters of interest.
#       Unless so overriden, all other config.yaml settings (including, e.g., locutus_workspaces_enable & locutus_workspace_name)
#       shall be used by Locutus.
#
# For example, in a former scenario (prior to the Conductor's default explicit false),
# Should the Conductor ever need to explicitly force locutus_load_batch_from_DB_bypass_CSV_manifest=false,
# another flag should be employed here within the Conductor, rather than the implicit check of:
#       if [[ "$PASS_use_batch_DB_bypass_manifest_CSV" == "" ]]; then
#   DONE!  via the new use_Batches=true
#
# PLEASE NOTE: `force_reprocess` might be another such setting that would require modifying the config.yaml directly,
# at least until such time that it is added here into the Conductor to override, if ever (?)
###################################################
# 31 March 2026 with new WIP branch for manifest-once batch feature
# NOTE: will need to update this default branch once PR'd into dev_bgd_lab, and then again in develop, etc.
###
# prior: 2024 October 31.... Boo!
# prior last revision of: 20 Nov 2024
# last updated: 05 May 2025, Happy Cinco de Mayo!
# w/ minor optimization update to the use_M_max_subbatch_size computation given use_N_num_subbatches
# -r3m0
#
# Call with:
#
# % sudo -E ./conduct_locutus_subbatches.sh -dD -B batch30 -s scit605ConductTest05 -m r3m0_test_butterball_onprem.csv \
#       -S 2830 -N 16 -r 1:8 -F ZZZ-ONDECK-4-PROCESSING_CHANGE:Viveks_OnPrem_scit605_batch30default
# NOTE: while -N 8 might be used if just running on one node,
# the above illustrates -N 16 -r 1:8 for the 1st 8 to be on this node,
# so as to highlight the following sub-batch locutus_batch_filter_counter_range values for each sub-batch container:
#   to generate sub-batches of S/N sub-batch sizes, ~= 2830/16 = 176.25 ~= 176 accessions per sub-batch (+ remainder)
#   UPDATED if in case of a remainder to generate sub-batches of (S/N)+1 == 177 accessions per sub-batch (w/ less)
#   -B batches to generate an accessionless NOOP manifest to re-use (or, just using the very same manifest for each sub-batch)
#   each one to be deployed w/ locutus_batch_filter_counter_range: "start(Bi) : end(Bi)", such as:
#       sub-batch 01 to use... locutus_batch_filter_counter_range: 1 : 177
#       sub-batch 02 to use... locutus_batch_filter_counter_range: 178 : 355
#       sub-batch 03 to use... locutus_batch_filter_counter_range: 356 : 531
#       [...]
#       sub-batch 13 to use... locutus_batch_filter_counter_range: 2125 : 2301
#       sub-batch 14 to use... locutus_batch_filter_counter_range: 2302 : 2478
#       sub-batch 15 to use... locutus_batch_filter_counter_range: 2479 : 2655
#       sub-batch 16 to use... locutus_batch_filter_counter_range: 2656 : 2830 <=== NOTE: +177 would have gone to 2832, but max'd out at -S
#   Further NOTE: each of these sub-batch deployments would specify export their locutus_batch_filter_counter_range,
#   leaving the remaining batch_filter settings as configured within the current config.yaml
#   with only the -F arg_Batch_Filter_Status currently understood by the Conductor,
#   to override the config/yaml's locutus_batch_filter_statuses
#
# % sudo -E ./conduct_locutus_subbatches.sh -m manifest_input -s suffix -K -M 100
#   to generate sub-batches of -M max size 100,
#   and -K Keep the generated sub-batch manifests
#
# % sudo -E ./conduct_locutus_subbatches.sh -m manifest_input -s suffix -K -N 15
#   to generate -N 15 sub-batches,
#   and -K Keep the generated 15x sub-batch manifests
#
# % sudo -E ./conduct_locutus_subbatches.sh -m manifest_input -s suffix -dD -N 15 -r 1:8
#   to generate -N 15 sub-batches,
#   -d deploying sub-batches for those matching -r range 1 through 8 on this node
#   -D pre-Deleting any same-named containers,
#   (NOT Keeping the sub-batch manifests)
#   w/ OnPrem De-ID module as default deployed
#
# % sudo -E ./conduct_locutus_subbatches.sh -m manifest_input -s suffix -dD -N 15 -r 1:8 -O
#   deploy OnPrem module to generate -N 15 sub-batches,
#   -d deploying sub-batches for those matching -r range 1 through 8 on this node
#   -D pre-Deleting any same-named containers,
#   (NOT Keeping the sub-batch manifests)
#   -O OnPrem De-ID module deployed
#
###############################################################################
# NOTE: see below "NOTE: build on_branch_deploy_script using:" for on_branch_deploy_script generation, using:
#   arg_DICOM_DeID_module (onprem, default)
# TODO: incorporate the following, once options other than their defaults are warranted:
#   arg_on (default='VM')
#   arg_branch (default="dev_bgd_lab")
###############################################################################
###############################################################
# TODO: update remainder of the deploy_*.sh scripts to support -m & -s & -b, etc.
#   NOTE: currently only testing with....
# NOTE: 12/20/2024 tested:  ./deploy_locutus_onVM_dev_bgd_lab_onprem.sh
###############################################################


###############################################################
# NOTE: the Deployer may now receive this basename as a -b
# as it was previously defined in both this Conductor and the Deployer, as:
#   export LOCUTUS_CONTAINER_BASENAME='locutus_container_'
# NOTE: no need to offer this as a cmd-line arg, though, just pass it on to the Deployer:
DEPLOYER_CONTAINER_BASENAME="locutus_container_"


# for direct clean output, set DEBUG_OUTPUT=0
# for additional per-line prefaces, set DEBUG_OUTPUT=1
#DEBUG_OUTPUT=1
DEBUG_OUTPUT=0

# sleep time before each deployment, to TRY reducing demand on DB connection pools
DEPLOY_SLEEP=10

# DEFAULTS (to override from cmd-line args)
################################################################
# default for: -m(anifest):
arg_input_manifest='./test_1500batch_4conductor_2024oct29.csv'
################################################################
# default for: -s(uffix)
arg_suffix='test1500batch01'
################################
# default for: -d(eploy) containers: >= 1 to actually deploy:
arg_do_deploy=0
################################
# default for: -D(elete) containers prior to deployment, if so:
arg_delete_containers_predeploy=0
################################
#arg_keep_subbatch_manifests=1
arg_keep_subbatch_manifests=0
################################ ################################
# BATCH NOTE:
use_Batches="false"
arg_Size_of_batch_filtered=0
arg_Batch_name=""
PASS_use_batch_DB_bypass_manifest_CSV="-e locutus_load_batch_from_DB_bypass_CSV_manifest=false"
PASS_batch_name=""
arg_Batch_Filter_Status=""
PASS_batch_status=""

################################ ################################
# TODO: currently exploring the -N num_subbatches -vs- -M max_subbatch_size
# NOTE: set both arg_N_subbatches=-1 & M arg_M_max_subbatch_size=-1
# to confirm after getopts that only one OR the other is set; bailing if both
################################
arg_N_subbatches=-1
arg_M_max_subbatch_size=-1
################################ ################################

###############################################################################
arg_on='VM'
arg_branch='dev_bgd_lab'
# WAS: (T)Here in the WIP branch:
#arg_branch='wip_batches_manifest_once'
# SHORTHAND, for the deploy script:
# arg_branch='wipbranch'
# 6/18/20206: returning default arg_branch to above dev_bgd_lab
#####
# NOTE: using lowercase `onprem` for the following to DEFAULT_DICOM_DeID_MODULE='OnPrem'
# to align w/ existing deploy_locutus_onVM_dev_bgd_lab_${arg_DICOM_DeID_module}.sh script names.
DICOM_DeID_MODULE_ONPREM='onprem'
DEFAULT_DICOM_DeID_MODULE=${DICOM_DeID_MODULE_ONPREM}
arg_DICOM_DeID_module=${DEFAULT_DICOM_DeID_MODULE}
################################
# NOTE: build on_branch_deploy_script using:
#   TODO: arg_on
#   TODO: arg_branch
#   TODO'ing: arg_DICOM_DeID_module
# NOTE: 12/20/2024 adding option: ./deploy_locutus_onVM_dev_bgd_lab_onprem.sh. as via:
#on_branch_deploy_script="deploy_locutus_onVM_dev_bgd_lab_"${arg_DICOM_DeID_module}".sh"
# NOTE: 03/30/2026 added a deploy_locutus_onVM_WIPbranch_onprem.sh, as callable via:
on_branch_deploy_script="deploy_locutus_on"${arg_on}"_"${arg_branch}"_"${arg_DICOM_DeID_module}".sh"
# WARNING: NOTE that this is currently ALSO being built up in the "while getopts ${OPTSTRING} opt" section for each:
#        O) # OnPrem De-ID module:
#            arg_DICOM_DeID_module=${DICOM_DeID_MODULE_ONPREM}
#            echo "using arg_DICOM_DeID_module=${arg_DICOM_DeID_module} to generate the deploy_script..."
#            on_branch_deploy_script="deploy_locutus_onVM_dev_bgd_lab_"${arg_DICOM_DeID_module}".sh";;
# TODO: just rebuild em there like THIS:
# on_branch_deploy_script="deploy_locutus_on"${arg_on}"_"${arg_branch}"_"${arg_DICOM_DeID_module}".sh"
###############################################################################

# given VM nodes of up to 8 CPUs each, set a maximum default range to deploy:
# 05/04/2025 NOTE: bumping from 8 to 16 deployments per mode,
# in case we want to start testing performance response up to containers per node = 2x # CPUs (8) per node
# Suspicion is that with much asyncronous wait time, multiple containers per CPU might be reasonable.
MAX_DEPLOYMENTS_PER_NODE=16

arg_range_to_deploy=''
###############################################################################
# TODO: extract from arg_range_to_deploy='5:5'
# resubmit batch15, failed DB pool even with the 10 seconds delay
# resubmit batch06 after re-enabled MAIN_Locutus re-raise for traceback with a Nonetype escaped exception:
# resubmit all (on ops03)
#deploy_start=1
#deploy_end=8
# then resubmit all (on ops01)
#deploy_start=1
#deploy_end=8
#############
deploy_start=-1
deploy_end=-1
###############################################################################


# SAFETY MEASURE LIMITS:
###################################
MAX_BLANK_LINES=2   # else trigger an EOF
# LOW TESTING: MAX_ACCESSIONS_LIMIT=25
# LOW TESTING: MAX_ANYLINE_LIMIT=30
###########
# for earlier manifests of 1,500 accesions:
#MAX_ACCESSIONS_LIMIT=2000
#MAX_ANYLINE_LIMIT=3000
###########
# 11/21/2024 now working with manifests of 3,000 accessions:
MAX_ACCESSIONS_LIMIT=4000
MAX_ANYLINE_LIMIT=5000
###########

#########################################################################
# STOP: this marks the end of the above user-configuration settings.    #
# NOTE: please do not edit below here,                                  #
# unless intentionally modifying the actual Conductor code.             #
#########################################################################


show_usage()
{
   # Display Usage/Help
   echo "conduct_locutus_subbatches.sh:"
   #echo
   echo "syntax: conduct_locutus_subbatches.sh -m manifest_in -s suffix [-d] [-D] [-K] [-M] [-N] [-O]"
   echo "options:"
   echo "m     manifest input file; REQUIRED"
   echo "s     suffix for Locutus container name upon deployment; REQUIRED"
   #####
   # deploy * deploy range:
   echo "d     deploy; else using default arg_do_deploy=${arg_do_deploy}"
   echo "r     FROM:TO range to deploy (min:max, e.g., 1:N); REQUIRED if -d(eploying), using default of 1:N,"
   echo -e "${TAB} w/ a max(N) range_size of <= MAX_DEPLOYMENTS_PER_NODE=${MAX_DEPLOYMENTS_PER_NODE}"
   #   + -r(ange_to_deploy), ex: 1:M, M+1:N
   #       # to help orchestration across both VMs,
   #       # sub-batch numbers to deploy on this node:
   #       # example, on trigops01: -d -r 1:7
   #       # example, on trigops03: -d -r 8:15
   #####
   echo "D     Delete same-named docker containers prior to deployment; else using default arg_delete_containers_predeploy=${arg_delete_containers_predeploy}"
   echo "K     KEEP generated sub-batch manifests; else using default arg_keep_subbatch_manifests=${arg_keep_subbatch_manifests}"
   echo "M     MAX accessions per sub-batch (REQUIRED: *either* M *or* N)"
   echo "N     N sub-batches (REQURIED: *either* N *or* M)"
   echo "O     OnPrem Locutus DeID module (${DICOM_DeID_MODULE_ONPREM})"
   echo "-     - - - - and new options for manifest-once batches: - - - -"
   echo "B     Batch_Name to use from the DB rather than the input manifest (still needed as a placeholder, even if NOOP)"
   echo "S     Size of the filtered batch or the Total, if no batch_filters (NOTE: ignores batch_filter_counter in the config.yaml, as it is overriden for each sub-batch)"
   echo "F     Batch Filter for manifest_Status, currently expecting but a single status [while dreaming of future double-quoted space-delimited for multiple statuses]"
   # r3m0: =====>  TODO: finish up the following...
   ###############################################################################
   # TODO: To more fully build up on_branch_deploy_script, add the following....
   #   + -o(n) VM: deploy_script to use? or all to be assumed as onVM?
   #   + -b(ranch) git branch to use
   ###############################################################################
   # and then...
   echo "h     help: print this"
   echo "u     usage: print that"
   #echo
}

arg_input_manifest=''
arg_suffix=''
# TODO: include an explicit -h(elp)/-u(sage) in the below (and to deploy_locutus_onVM_dev_bgd_lab_onprem.sh, from whence it came)
#while getopts ":d:D:K:u:hs:m::" flag
OPTSTRING=":hudDKs:m:r:M:N:B:S:F:O"
while getopts ${OPTSTRING} opt
do
    case "${opt}" in
        s) # suffix OPTARG:
            arg_suffix=${OPTARG};;
        m) # manifest OPTARG:
            arg_input_manifest=${OPTARG};;
        M) # arg_M_max_subbatch_size OPTARG:
            arg_M_max_subbatch_size=${OPTARG};;
        N) # arg_N_subbatches OPTARG:
            arg_N_subbatches=${OPTARG};;
        O) # OnPrem De-ID module:
            arg_DICOM_DeID_module=${DICOM_DeID_MODULE_ONPREM}
            echo "using arg_DICOM_DeID_module=${arg_DICOM_DeID_module} to generate the deploy_script..."
            # WAS: on_branch_deploy_script="deploy_locutus_onVM_dev_bgd_lab_"${arg_DICOM_DeID_module}".sh";;
            on_branch_deploy_script="deploy_locutus_on"${arg_on}"_"${arg_branch}"_"${arg_DICOM_DeID_module}".sh";;
        r) # range_to_deploy OPTARG:
            arg_range_to_deploy=${OPTARG};;
        d) # deploy:
            arg_do_deploy=1;;
        D) # delete containers prior to deploy
            arg_delete_containers_predeploy=1;;
        K) # keep manifests
            arg_keep_subbatch_manifests=1;;
        B) # arg_Batch_name OPTARG (& automatically "keep" the nonexistent subbatch manifests):
            use_Batches=true
            arg_Batch_name=${OPTARG}
            arg_keep_subbatch_manifests=1
            PASS_use_batch_DB_bypass_manifest_CSV="-e locutus_load_batch_from_DB_bypass_CSV_manifest=true"
            PASS_batch_name="-e locutus_batch_name=${arg_Batch_name}";;
        S) # arg_Batch_name OPTARG:
            arg_Size_of_batch_filtered=${OPTARG};;
        F) # arg_Batch_Filter_Status OPTARG:
            arg_Batch_Filter_Status=${OPTARG}
            #PASS_batch_status="-e locutus_batch_filter_statuses=\\\"${arg_Batch_Filter_Status}\\\"";;
            #PASS_batch_status="-e locutus_batch_filter_statuses=\"${arg_Batch_Filter_Status}\"";;
            # NOTE: still determining how to best pass on a string-quoted Filter_Status list to support multiple such statuses;
            # FOR NOW, merely support but a single status, without quotes:
            PASS_batch_status="-e locutus_batch_filter_statuses=${arg_Batch_Filter_Status}";;
        h | u) # help/usage:
            show_usage
            exit 0;;
        \?) #  (\? or *) == any others, unknown:
            echo "ERROR: unknown flag ${OPTARG}"
            show_usage
            exit -1 ;;
    esac
done


TAB="\t"

echo "Welcome to the Locutus Conductor, conduct_locutus_subbatches.sh"
echo "Using cmd-line/default values of...."
echo -e "${TAB}arg_input_manifest: ${arg_input_manifest}"
echo -e "${TAB}arg_suffix: ${arg_suffix}"

echo -e "${TAB}arg_do_deploy: ${arg_do_deploy}"
echo -e "${TAB}arg_delete_containers_predeploy: ${arg_delete_containers_predeploy}"
echo -e "${TAB}arg_keep_subbatch_manifests: ${arg_keep_subbatch_manifests}"

echo -e "${TAB}arg_N_subbatches: ${arg_N_subbatches}"
echo -e "${TAB}arg_M_max_subbatch_size: ${arg_M_max_subbatch_size}"
echo -e "${TAB}arg_range_to_deploy: ${arg_range_to_deploy}"
echo -e "${TAB}arg_DICOM_DeID_module: ${arg_DICOM_DeID_module}"


echo -e "${TAB}use_Batches: ${use_Batches}"
echo -e "${TAB}arg_Batch_name: ${arg_Batch_name}"
echo -e "${TAB}PASS_batch_name: ${PASS_batch_name}"
echo -e "${TAB}PASS_use_batch_DB_bypass_manifest_CSV: ${PASS_use_batch_DB_bypass_manifest_CSV}"
echo -e "${TAB}arg_Size_of_batch_filtered: ${arg_Size_of_batch_filtered}"
echo -e "${TAB}arg_Batch_Filter_Status: ${arg_Batch_Filter_Status}"
echo -e "${TAB}PASS_batch_status: ${PASS_batch_status}"

echo "-------------------------------------------"
# echo "as used to choose/generate/indicate the following on_branch_deploy_script"
echo -e "${TAB}on_branch_deploy_script: ${on_branch_deploy_script}"
echo "-------------------------------------------"

# TODO: SOME DAY, also finish up the following...
echo "-------------------------------------------"
echo "TODO: eventually add to the above deploy_script w/ configuration for the following, currently hardcoded as...."
echo -e "${TAB}arg_on: ${arg_on}"
echo -e "${TAB}arg_branch: ${arg_branch}"
echo "-------------------------------------------"

# SAFETY CHECK:
# if PASS_use_batch_DB_bypass_manifest_CSV need arg_Size_of_batch_filtered & arg_N_subbatches:
let batch_size=0
let batch_remainder=0
if [[ use_Batches == "" ]]; then
    # BATCH NOTE:
    echo "NOTE: no Batches being used from DB, shall fully use the -m manifest"
else
    echo "BATCH NOTE: -B set, batch \"${arg_Batch_name}\" being used from DB; the -m manifest is still required for linking, but its contents shall be ignored."
    if [[ ${arg_Size_of_batch_filtered} -lt 1 ]]; then
        echo "BATCH ERROR: missing -S for Size of the filtered batch."
        exit -1
    elif [[ ${arg_N_subbatches} -lt 1 ]]; then
        echo "BATCH ERROR: missing -N for Number of sub-batches."
        exit -1
    fi
    #
    let batch_size=(arg_Size_of_batch_filtered/arg_N_subbatches)
    let batch_remainder=(arg_Size_of_batch_filtered%arg_N_subbatches)
    echo "BATCH NOTE: given S=${arg_Size_of_batch_filtered} / N=${arg_N_subbatches} == batch_size of ${batch_size} * ${arg_N_subbatches} containers, with a remainder of ${batch_remainder}."
    if [[ ${batch_remainder} -gt 0 ]]; then
        # NOTE: with any non-0 remainder, rather than adding to the last sub-batch,
        # distribute this throughout the sub-batches by adding the +1 to each.
        # This will effectively reduce the last batch size by that initial remainder.
        let batch_size=(batch_size+1)
        echo "BATCH NOTE: incrementing batch_size to ${batch_size} * ${arg_N_subbatches} containers, to distribute the remainder and effectively reduce the last batch."
    fi
    let batch_last_less=(arg_N_subbatches*batch_size)-arg_Size_of_batch_filtered
    echo "BATCH NOTE: generating batch_size of ${batch_size} * ${arg_N_subbatches} sub-batches containers, with a final sub-batch reduction of batch_last_less=${batch_last_less}."
fi
echo "-------------------------------------------"

any_linenum=0
blanks_in_a_row=0
num_accessions=0

# NOTE: MANIFEST SUB-DIVISION
# that can afford us more control over counting the total number of accessions,
# while also bringing across any additional comments
# ..... and all with bash, LOL!


########################################################
# Part 0a: check for REQUIRED cmd-line arguments
########################################################

missing_reqd_args=0
if [[ "$arg_input_manifest" == "" ]]; then
    echo 'ERROR: missing REQUIRED -m argument for manifest'
    missing_reqd_args=1
fi
if [[ "$arg_suffix" == "" ]]; then
    echo 'ERROR: missing REQUIRED -s argument for suffix'
    missing_reqd_args=1
fi

# And, not technically "missing", per se, but using the same safety check section
# to ensure that either -M or -N were set, but not both:
if [[ $arg_M_max_subbatch_size -lt 0 && arg_N_subbatches -lt 0 ]]; then
    echo 'ERROR: neither -M nor -N set; must specify either of -M (MAX accessions per sub-batch) or -N (N sub-batches)'
    missing_reqd_args=1
elif [[ $arg_M_max_subbatch_size -gt 0 && arg_N_subbatches -gt 0 ]]; then
    echo 'ERROR: both -M and -N set; must specify only one of -M (MAX accessions per sub-batch) or only -N (N sub-batches)'
    missing_reqd_args=1
fi

if [[ "$missing_reqd_args" -gt 0 ]]; then
    show_usage
    exit -1
fi

########################################################
# Part 0b: check input manifest file size and contents
########################################################
# BATCH NOTE: Q: might it still be interesting to see these input manifest stats, even if not applicable? Nah:
#WAS: if [[ "$PASS_use_batch_DB_bypass_manifest_CSV" == "" ]]; then
if [[ "$use_Batches" = "false" ]]; then
    # BATCH NOTE:
    # echo "NOTE: no Batches being used from DB, shall fully use the -m manifest"
    echo "-------------------------------------------"

    # FIRST PASS num lines in file, even if comments or blank:
    # NOTE: adding the final sed to trim leading spaces from wc:
    num_lines_total=`cat ${arg_input_manifest} | wc -l | sed 's/[[:blank:]]//g'`
    echo "${num_lines_total} total lines in ${arg_input_manifest}"
    if [[ ${num_lines_total} -lt 1 ]]; then
        echo "ERROR: input manifest ${arg_input_manifest} has no content to conduct."
        exit -1
    fi

    # NOTE either `grep -c` OR  `grep | wc -l` work fine, with the former saving the pipe:

    num_lines_headers=`egrep -c '(subject_id,)' ${arg_input_manifest}`
    echo "${num_lines_headers} header line(s) in ${arg_input_manifest}"

    num_lines_blank=`grep -cvE -e '[^[:space:]]' ${arg_input_manifest}`
    echo "${num_lines_blank} blank line(s) in ${arg_input_manifest}"

    num_lines_comments=`grep -c  "#" ${arg_input_manifest}`
    echo "${num_lines_comments} comment line(s) in ${arg_input_manifest}"

    num_lines_active=`egrep -v '(#|subject_id)' ${arg_input_manifest} | grep -cE -e '[^[:space:]]'`
    echo "${num_lines_active} active non-comment/non-blank/non-header line(s) in ${arg_input_manifest}"


    ################################ ################################
    # NOTE: using the -N num_subbatches -vs- -M max_subbatch_size
    # NOTE: set both arg_N_subbatches=-1 & M arg_M_max_subbatch_size=-1
    # to confirm after getopts that only one OR the other is set; bailing if both
    ################################
    # INPUT:
    # arg_N_subbatches=-1 or set (if > 0)
    # arg_M_max_subbatch_size=-1 or set (if )> 0)
    ################################ ################################
    # NOTE: CALCULATE the total size,
    # either using: arg_N_subbatches (if > 0)
    # or using: arg_M_max_subbatch_size (if > 0)
    ################################
    # GIVES:
    # use_N_num_subbatches
    # use_M_max_subbatch_size
    # extra_beyond_M_max_size_for_subbatch_N
    ################################ ################################

    echo "-------------------------------------------"
    extra_beyond_M_max_size_for_subbatch_N=0
    if [[ $arg_M_max_subbatch_size -gt 0 ]]; then
        use_M_max_subbatch_size=$arg_M_max_subbatch_size
        echo "====> use_M_max_subbatch_size=${use_M_max_subbatch_size}"
        echo "Computing use_N_num_subbatches, given the corresponding num_lines_active / arg_M_max_subbatch_size ..."
        echo "let use_N_num_subbatches=num_lines_active/arg_M_max_subbatch_size == ${num_lines_active}/${arg_M_max_subbatch_size}"
        let use_N_num_subbatches=num_lines_active/arg_M_max_subbatch_size
        echo "WHOLE use_N_num_subbatches=${arg_N_suse_N_num_subbatchesubbatches}"
        # + remainder:
        let remainder=num_lines_active%arg_M_max_subbatch_size
        echo -e "${TAB}REMAINDER: $num_lines_active % $arg_M_max_subbatch_size == ${remainder}"
        if [[ $remainder -gt 0 ]]; then
            echo -e "${TAB}incrementing WHOLE subbatches to accommodate non-zero REMAINDER..."
            (( use_N_num_subbatches++ ))
        else
            echo -e "${TAB}leaving WHOLE subbatches as is, since no REMAINDER..."
        fi
        echo "====> use_N_num_subbatches=${use_N_num_subbatches}"
    elif [[ $arg_N_subbatches -gt 0 ]]; then
        use_N_num_subbatches=$arg_N_subbatches
        echo "====> use_N_num_subbatches=${use_N_num_subbatches}"
        #WAS: if [[ "$PASS_use_batch_DB_bypass_manifest_CSV" == "" ]]; then
        if [[ "$use_Batches" = "false" ]]; then
            # BATCH NOTE:
            # NOTE: using the input manifest to compute the overall batch size, etc:
            echo "Computing use_M_max_subbatch_size, given the corresponding num_lines_active / arg_N_subbatches ..."
            echo "let use_M_max_subbatch_size=num_lines_active/arg_N_subbatches == ${num_lines_active}/${arg_N_subbatches}"
            let use_M_max_subbatch_size=num_lines_active/arg_N_subbatches
            #####
            # OPTIMIZATION NOTE: if the above ^^^ integer floor(num_lines/arg_N) would leave a remainder at the end,
            # then instead simulate an  integer celing(num_lines/arg_N)
            # with the below += 1:
            let use_M_mod=num_lines_active%arg_N_subbatches
            if [[ $use_M_mod -ne 0 ]]; then
                echo "since (num_lines_active % arg_N_subbatches) is non-zero, incrementing use_M_max_subbatch_size = ${use_M_max_subbatch_size} + 1"
                let use_M_max_subbatch_size+=1
            fi
            #####
            echo "OPTIMIZED WHOLE use_M_max_subbatch_size=${use_M_max_subbatch_size}"
            # + remainder:
            let remainder=num_lines_active%arg_N_subbatches
            echo -e "${TAB}REMAINDER: $num_lines_active % $arg_N_subbatches == ${remainder}"
            if [[ $remainder -gt 0 ]]; then
                extra_beyond_M_max_size_for_subbatch_N=${remainder}
                echo -e "${TAB}locking WHOLE subbatch_size to ${use_M_max_subbatch_size}, so last batch shall accommodate additional non-zero REMAINDER of ${extra_beyond_M_max_size_for_subbatch_N}..."
            else
                echo -e "${TAB}WHOLE subbatch_size perfect as is; no REMAINDER..."
            fi
            echo "====> use_M_max_subbatch_size=${use_M_max_subbatch_size}"
        else
            # NOTE: no longer to get here, but to the overall final else:
            echo "BATCH NOTE: ALREADY Computed batch_size=${batch_size}."
        fi
    fi
    echo "-------------------------------------------"
    echo "Gives:"
    echo "use_M_max_subbatch_size=${use_M_max_subbatch_size}"
    echo "use_N_num_subbatches=${use_N_num_subbatches}"
    echo "extra_beyond_M_max_size_for_subbatch_N=${extra_beyond_M_max_size_for_subbatch_N}"
    # pre-compute the extended batch size for subbatch N:
    let subbatch_N_size=use_M_max_subbatch_size+extra_beyond_M_max_size_for_subbatch_N
    echo "subbatch_N_size=${subbatch_N_size}"
else
    echo "BATCH NOTE: ALREADY Computed batch_size=${batch_size}, using use_N_num_subbatches=arg_N_subbatches=${arg_N_subbatches}"
    use_N_num_subbatches=$arg_N_subbatches
fi

####################################################
# TODO: upon return, use the above 3 sub-batch vars in the main sub-batch divider while
# NOTE: especially for cases like 20 accessions across 6 batches for M=3, with a remainder of 2 for the 6th,
# be sure to capture all of these prior to any of the blank lines in the while !EOF loop,
# such that the residual zombie file at the end has none, and is removed, leaving back to the 6 batches.
####################################################

########################################################
# Part 0c: ensure that VAULT_TOKEN is set for later deploy
########################################################
if [[ "$VAULT_TOKEN" == "" ]]; then
    echo "WARNING: VAULT_TOKEN is EMPTY"
    if [[ $arg_do_deploy -lt 1 ]]; then
        echo "BUT, VAULT_TOKEN is not needed since this is a DRY RUN **NOT** actually deploying, thanks to arg_do_deploy: ${arg_do_deploy}"
    else
        echo "ERROR: VAULT_TOKEN is needed since this is NOT a DRY RUN (i.e., arg_do_deploy = ${arg_do_deploy}"
        echo "Please be sure to: export VAULT_TOKEN='your-current-vault-token', and re-conduct, thanks!"
        exit -1
    fi
else
    echo "r3m0 DEBUG: VAULT_TOKEN is non-empty; proceeding as if it is currently valid..."
    echo "TODO: perform precursory vault test, even just a vault token to check for validity"
    # TODO: add a vault token to check such as the following....
    ##################
    # Locutus $ vtoken
    # Error looking up token: Error making API request.
    # Namespace: [NAMESPACE]
    # URL: GET [VAULT_URL]]
    # Code: 403. Errors:
    # * permission denied
    # Locutus $ echo $status
    #
    # Locutus $
    ##################
fi

echo "--------------------------------------------------"
# good ol reminder, from:
# https://kodekloud.com/blog/read-file-in-bash/
# Read the input file line by line

header_read=0
subbatch_num=1
subbatch_out_linenum=0
subbatch_out_accession_num=0
done=0

########################################################
# Part 1a: sub-divide the manifest into its sub-batches
########################################################

# BATCH NOTE: alternate approach to follow, of merely referencing the source same input manifest
#WAS: if [[ "$PASS_use_batch_DB_bypass_manifest_CSV" == "" ]]; then
if [[ "$use_Batches" = "false" ]]; then
    # BATCH NOTE:
    # echo "NOTE: no Batches being used from DB, shall fully use the -m manifest"

    subbatch_suffix=$(printf '%s_subbatch%02d'  "$arg_suffix" "$subbatch_num")
    output_file_subbatch=$(printf '%s.%s.csv' "$arg_input_manifest" "$subbatch_suffix")
    echo "TOUCHING output_file_subbatch=$output_file_subbatch"
    touch $output_file_subbatch
    # TODO: consider a pre-touch rm, yeah?
    # do so with a direct redirect, no append:
    #  NOTE: just not here, yet.... dunno the header yet!
    # maybe just a write empty to it:
    echo -n "" > $output_file_subbatch

    if [[ $DEBUG_OUTPUT -ne 0 ]]; then
        echo "= = = = = = = = = = = = = = = = = ="
    fi


    # NOTE: incorporates the following into the end of this while loop,
    # where it calculates: ${ready_for_next_file}
    ####################################################
    #echo "arg_M_max_subbatch_size=${arg_M_max_subbatch_size}"
    #echo "arg_N_subbatches=${arg_N_subbatches}"
    #echo "extra_beyond_M_max_size_for_last_subbatch=${extra_beyond_M_max_size_for_last_subbatch}"
    #echo "subbatch_N_size=${subbatch_N_size}"
    ####################################################
    # TODO: upon return, use the above 3+ sub-batch vars in the main sub-batch divider while
    # NOTE: especially for cases like 20 accessions across 6 batches for M=3, with a remainder of 2 for the 6th,
    # be sure to capture all of these prior to any of the blank lines in the while !EOF loop,
    # such that the residual zombie file at the end has none, and is removed, leaving back to the 6 batches.
    ####################################################

    while [[ $num_accessions -le $MAX_ACCESSIONS_LIMIT ]] && [[ $any_linenum -le $MAX_ANYLINE_LIMIT ]] && [[ $blanks_in_a_row -le $MAX_BLANK_LINES ]] ;
    do
        read -r LINE # < ${arg_input_manifest}
        # WAS: (( any_linenum++ ))
        # MOVED to the BOTTOM, to leave line #0 as the header

        line_len=${#LINE}
        if [[ $DEBUG_OUTPUT -ne 0 ]]; then
            echo -n "[anyline=$any_linenum, num_accessions=$num_accessions, sub-batch $subbatch_num, sub-line# $subbatch_out_linenum, sub-acc# $subbatch_out_accession_num, len=$line_len]: "
        fi

        is_comment=0
        is_blank=0

        if [[ $line_len -le 1 ]]; then
            # NOTE EOL may count as 1
            if [[ $DEBUG_OUTPUT -ne 0 ]]; then
                echo "IF: line is BLANK: $LINE"
            fi
            is_blank=1
            (( blanks_in_a_row++ ))
            #echo $LINE >> $output_file_subbatch
            # make it a commented not of a blank:
            ######################################
            if [[ $DEBUG_OUTPUT -ne 0 ]];  then
                echo -n "# (blank)" >> $output_file_subbatch
            fi
            ######################################
            echo "$LINE" >> $output_file_subbatch
        elif [[ ${LINE:0:1} == '#' ]]; then
            if [[ $DEBUG_OUTPUT -ne 0 ]]; then
                echo "ELIF: line is COMMENT: $LINE"
            fi
            blanks_in_a_row=0
            is_comment=1
            #echo $LINE >> $output_file_subbatch
            # with a little (debug):
            ######################################
            if [[ $DEBUG_OUTPUT -ne 0 ]];  then
                echo -n "# (comment) " >> $output_file_subbatch
            fi
            ######################################
            echo "$LINE" >> $output_file_subbatch
        elif [[ ${LINE:0:2} == '"#' ]]; then
            # or, comments with double-quotes in front, courtesy of Excel
            if [[ $DEBUG_OUTPUT -ne 0 ]]; then
                echo "ELIF: line is QUOTED COMMENT: $LINE"
            fi
            blanks_in_a_row=0
            is_comment=1
            # echo $LINE >> $output_file_subbatch
            # with a little (debug):
            ######################################
            if [[ $DEBUG_OUTPUT -ne 0 ]];  then
                echo -n "# (quoted-comment) " >> $output_file_subbatch
            fi
            ######################################
            #echo "$LINE" >> $output_file_subbatch
            # NOTE: for a potentially more robust Locutus experience,
            # go ahead and preface the double-quote with a non-quoted comment,
            # just in case:
            echo "# $LINE" >> $output_file_subbatch
        else
            # WAS: (( linenum++ ))
            (( subbatch_out_linenum++ ))
            blanks_in_a_row=0

            if [[ $DEBUG_OUTPUT -ne 0 ]]; then
                printf 'ELSE: valid-accession %d of any %d= %s\n' $num_accessions $any_linenum "$LINE"
            fi

            # TODO: introduce safety check that this first line is indeed the expected header: subject_id, object_*,...
            # WAS: if [[ $linenum -eq 0 ]];  then
            # WAS: if [[ $num_accessions -eq 0 ]];  then
            if [[ $header_read -eq 0 ]];  then
                # TODO: loop through the above to ensure that this first line ISN'T the comment
                header="$LINE"
                if [[ $DEBUG_OUTPUT -ne 0 ]]; then
                    echo "THIS ^^^ IS THE HEADER, LINE = $LINE"
                fi
                # == line #0
                (( header_read++ ))
                ######################################
                if [[ $DEBUG_OUTPUT -ne 0 ]];  then
                    echo -n "[INITIAL HEADER sub-batch $subbatch_num sub-line# $subbatch_out_linenum sub-acc# $subbatch_out_accession_num] " >> $output_file_subbatch
                fi
                ######################################
                echo "ADDING INITIAL HEADER LINE to output_file_subbatch=$output_file_subbatch"
                echo "$LINE" >> $output_file_subbatch
            else
                # accession:
                (( num_accessions++ ))
                (( subbatch_out_accession_num++ ))
                ######################################
                if [[ $DEBUG_OUTPUT -ne 0 ]];  then
                    echo -n "[ACC sub-batch $subbatch_num sub-line# $subbatch_out_linenum sub-acc# $subbatch_out_accession_num] " >> $output_file_subbatch
                fi
                ######################################
                echo "$LINE" >> $output_file_subbatch
            fi
        fi

        (( any_linenum++ ))

        #printf 'at START, pre-line %d = %s\n' $linenum "$LINE"
        #(( linenum++ ))

        # NOW, if we're counting the lines right, will have emitted another one above
        #WAS: if [[ $subbatch_out_accession_num -ge $arg_M_max_subbatch_size ]]; then
        #if [[ ( $subbatch_num -lt $use_N_num_subbatches && $subbatch_out_accession_num -ge $use_M_max_subbatch_size ) || ( $subbatch_num -ge $use_N_num_subbatches && $subbatch_out_accession_num -ge ( $use_M_max_subbatch_size + $extra_beyond_M_max_size_for_subbatch_N  ) ) ]]; then
        #####
        # NOTE: breaking the above into simpler components, to effectively OR:
        ready_for_next_file=0
        if [[ ( $subbatch_num -lt $use_N_num_subbatches && $subbatch_out_accession_num -ge $use_M_max_subbatch_size )  ]]; then
            # first N-1 sub-batches: up to use_M_max_subbatch_size
            ready_for_next_file=1
        elif [[  ( $subbatch_num -ge $use_N_num_subbatches && $subbatch_out_accession_num -ge subbatch_N_size ) ]]; then
            # sub-batch N: up to subbatch_N_size (==use_M_max_subbatch_size+extra_beyond_M_max_size_for_subbatch_N)
            ready_for_next_file=1
        fi
        if [[ $ready_for_next_file -gt 0 ]]; then
            if [[ $DEBUG_OUTPUT -ne 0 ]]; then
                echo "= = = = = = = = = = = = = = = = = ="
            fi
            echo "CLOSING $output_file_subbatch"
            echo "-------"

            (( subbatch_num++ ))
            subbatch_out_linenum=0
            subbatch_out_accession_num=0

            # WAS: subbatch_suffix=$(printf 'subbatch%02d_%s.csv' "$subbatch_num" "$arg_suffix")
            subbatch_suffix=$(printf '%s_subbatch%02d'  "$arg_suffix" "$subbatch_num")
            output_file_subbatch=$(printf '%s.%s.csv' "$arg_input_manifest" "$subbatch_suffix")
            echo "TOUCHING output_file_subbatch=$output_file_subbatch"
            #echo $header >> $output_file_subbatch
            # TODO: consider a pre-touch rm, yeah?
            # do so with a direct redirect, no append:

            touch $output_file_subbatch
            #echo $header > $output_file_subbatch
            # NOTE: the single redirect to CREATE this file:
            (( subbatch_out_linenum++ ))
            ######################################
            echo "ADDING HEADER to output_file_subbatch=$output_file_subbatch"
            if [[ $DEBUG_OUTPUT -ne 0 ]];  then
                echo -n "[RE-HEADER sub-batch $subbatch_num sub-line# $subbatch_out_linenum sub-acc# $subbatch_out_accession_num] " > $output_file_subbatch
                echo "$header" >> $output_file_subbatch
            else
                echo "$header" > $output_file_subbatch
            fi
            ######################################
            #echo "$header" > $output_file_subbatch
            if [[ $DEBUG_OUTPUT -ne 0 ]]; then
                echo "= = = = = = = = = = = = = = = = = ="
            fi
            #########
            #echo "--------"
            #echo "r3m0 TESTING CURRENT output_file_subbatch=$output_file_subbatch"
            #echo "---"
            #cat -n $output_file_subbatch
            #echo "--------"
            #########
        fi

    done < "$arg_input_manifest"

    #########
    #echo "--------"
    #echo "r3m0 TESTING ^^^^ the above FINAL output_file_subbatch=$output_file_subbatch"
    #echo "---"
    #cat -n $output_file_subbatch
    #echo "--------"
    #########

    #####
    # OPTIMIZATION NOTE: if the above ^^^ "while [[ $num_accessions -le $MAX_ACCESSIONS_LIMIT ]] && ... ;" finishes early,
    # still write out empty sub-match manifests for any remaining sub-batches:
    # NOPE: if [[ $num_accessions -ge $MAX_ACCESSIONS_LIMIT ]]; then
    if [[ $subbatch_num -le $use_N_num_subbatches ]] ; then
        echo "= = = = = = = = = = = = = = = = = ="
        echo "OPTIMIZED container load distribution, to reduce the overall maximum sub-batch size, left a partial sub-batch to close..."
        echo "CLOSING $output_file_subbatch"
        echo "-------"
        #########
        #echo "--------"
        #echo "r3m0 TESTING ^^^^ the above FINAL output_file_subbatch=$output_file_subbatch"
        #echo "---"
        #cat -n $output_file_subbatch
        #echo "--------"
        #########
        (( subbatch_num++ ))
    fi
    while [[ $subbatch_num -le $use_N_num_subbatches ]] ;
    do
        echo "= = = = = = = = = = = = = = = = = ="
        echo "OPTIMIZED container load distribution, to reduce the overall maximum sub-batch size, left an empty sub-batch to create..."
        subbatch_suffix=$(printf '%s_subbatch%02d'  "$arg_suffix" "$subbatch_num")
        output_file_subbatch=$(printf '%s.%s.csv' "$arg_input_manifest" "$subbatch_suffix")
        echo "OPTIMIZATION TOUCHING output_file_subbatch=$output_file_subbatch"
        touch $output_file_subbatch
        ######################################
        echo "OPTIMIZED ADDING HEADER to output_file_subbatch=$output_file_subbatch"
        if [[ $DEBUG_OUTPUT -ne 0 ]];  then
            echo -n "[RE-HEADER sub-batch $subbatch_num sub-line# $subbatch_out_linenum sub-acc# $subbatch_out_accession_num] " > $output_file_subbatch
            echo "$header" >> $output_file_subbatch
        else
            echo "$header" > $output_file_subbatch
        fi
        ######################################
        #echo "$header" > $output_file_subbatch
        if [[ $DEBUG_OUTPUT -ne 0 ]]; then
            echo "= = = = = = = = = = = = = = = = = ="
        fi
        echo "OPTIMIZATION CLOSING $output_file_subbatch"
        (( subbatch_num++ ))
    done
    #####

    if [[ $DEBUG_OUTPUT -ne 0 ]]; then
        echo "= = = = = = = = = = = = = = = = = ="
    fi

    echo "--------------------------------------------------"
    echo "header was: ${header}"

    echo "loop terminating condition variables:"
    echo "[anyline=$any_linenum, num_accessions=$num_accessions, sub-batch $subbatch_num, sub-line# $subbatch_out_linenum, sub-acc# $subbatch_out_accession_num, len=$line_len]: "
    if [[ $subbatch_out_accession_num -lt 1 ]]; then
        echo "WARNING: the last subbatch output appears to be only additional EOF fluff, without any actual accessions; sorry bout that."
        echo "Feel free to delete via: "
        echo -e "${TAB}rm $output_file_subbatch"
        # and again, at the end of the sub-batch file:
        echo "# WARNING: no accessions in this sub-batch; all residual EOF fluff, sorry" >> $output_file_subbatch
        echo "# Feel free to delete via: " >> $output_file_subbatch
        echo -e "#${TAB}rm $output_file_subbatch" >> $output_file_subbatch
        #####
        # NOTE: no longer remove a final empty sub-batch manifest, as it is easier to understand that NO-OP output there
        #####
        # TODO: auto-delete residual last one....UNLESS it is the only, perhaps?
        #if [[ $subbatch_num -gt 1 ]] && [[ $subbatch_out_accession_num -lt 1 ]]; then
        #    echo "In fact, maybe we should just auto-delete it here.... "
        #    echo "DELETING $output_file_subbatch."
        #    rm $output_file_subbatch
        #    (( subbatch_num-- ))
        #fi
    fi
    echo "Done with Part 1a: total sub-manifests created = ${subbatch_num}"
else
    echo "BATCH NOTE: bypassed sub-manifest creation;"
    echo "BATCH NOTE: to instead reference the same input manifest of ${arg_input_manifest} for each sub-batch."
    echo "BATCH NOTE: since Locutus will bypass the sub-manifest anyhow, as via ${PASS_use_batch_DB_bypass_manifest_CSV}"
    echo "--------------------------------------------------"
fi

########################################################
# PRE-Part 2: CHECK for the deployment range across the N sub-batches AND sudo
########################################################

if [[ $arg_do_deploy -lt 1 ]]; then
    echo "Without a -d(eploy) and -r(ange), nothing more to Conduct"
    echo "Regardless of -K(eep), leaving you the sub-batch manifests to peruse prior to deployment...."
    echo "enjoy!"
    exit 0
fi


########################################################
# Part 1b:
# Determine range to deploy, given arg_range_to_deploy=${arg_range_to_deploy}, etc:
echo "--------------------------------------------------"
if [[ "$arg_range_to_deploy" == "" ]]; then
    echo "Determine range to deploy, given NO arg_range_to_deploy, use_N_num_subbatches=${use_N_num_subbatches} & MAX_DEPLOYMENTS_PER_NODE=${MAX_DEPLOYMENTS_PER_NODE}"
    calc_deploy_start=1
    echo "Setting calc_deploy_start to ${calc_deploy_start}"
    calc_deploy_end=${use_N_num_subbatches}
    echo "Setting calc_deploy_end to use_N_num_subbatches=${calc_deploy_end}"
    if [[ calc_deploy_end -gt MAX_DEPLOYMENTS_PER_NODE ]]; then
        echo "BUT this exceeds MAX_DEPLOYMENTS_PER_NODE; truncating to ${MAX_DEPLOYMENTS_PER_NODE}"
        calc_deploy_end=$MAX_DEPLOYMENTS_PER_NODE
        echo "Setting calc_deploy_end to MAX_DEPLOYMENTS_PER_NODE=${calc_deploy_start}"
    fi
    # either way
    echo "ERROR: terminating early to allow a re-Conduct using the required -r option with a recommended range of....."
    echo -e "${TAB}-r ${calc_deploy_start}:${calc_deploy_end}"
    exit -1

else
    # if arg_range_to_deploy is set:
    echo "Determine range to deploy, given arg_range_to_deploy=${arg_range_to_deploy}, use_N_num_subbatches=${use_N_num_subbatches} & MAX_DEPLOYMENTS_PER_NODE=${MAX_DEPLOYMENTS_PER_NODE}"
    # TODO: ensure that there is a : in this at all, but for now, a first pass of:
    calc_deploy_start=`echo ${arg_range_to_deploy} | awk -F ':' '{print $1}'`
    calc_deploy_end=`echo ${arg_range_to_deploy} | awk -F ':' '{print $2}'`
    echo "calc_deploy_start=${calc_deploy_start}, calc_deploy_end=${calc_deploy_end}"
    # TODO: check if range is > MAX_DEPLOYMENTS_PER_NODE
    let range_size=calc_deploy_end-calc_deploy_start+1
    if [[ $range_size -gt $MAX_DEPLOYMENTS_PER_NODE ]]; then
    # Q: ERROR out if so, or limit further?
        echo "WARNING: range_size=${range_size} > MAX_DEPLOYMENTS_PER_NODE=${MAX_DEPLOYMENTS_PER_NODE}.  Truncating...."
        let calc_deploy_end=calc_deploy_start+MAX_DEPLOYMENTS_PER_NODE-1
        let range_size=calc_deploy_end-calc_deploy_start+1
        echo "WARNING: truncated calc_deploy_end=${calc_deploy_end}, w/ updated range_size=${range_size} ?= MAX_DEPLOYMENTS_PER_NODE=${MAX_DEPLOYMENTS_PER_NODE}. Okay."
        echo "ERROR: terminating early to allow a re-Conduct using the new recommended range of....."
        echo -e "${TAB}-r ${calc_deploy_start}:${calc_deploy_end}"
        exit -1
    else
        echo "range_size=${range_size} <= MAX_DEPLOYMENTS_PER_NODE=${MAX_DEPLOYMENTS_PER_NODE}.  Good!"
    fi
fi

deploy_start=$calc_deploy_start
deploy_end=$calc_deploy_end
echo "APPARENTLY proceeding to deployment using range of ${deploy_start}:${deploy_end}"
#exit -1
echo "Here we goooooooooo..........."

echo "--------------------------------------------------"

echo "Given arg_range_to_deploy=${arg_range_to_deploy}, with deploy_start=${deploy_start} & deploy_end=${deploy_end}..."

# TODO: ensure that they are within the range of subbatch_num!!!!!
bail_now=0
if [[ $deploy_start -lt 1 ]]; then
    echo "WARNING: deploy_start (${deploy_start}) < 1; upping to 1"
    deploy_start=1
elif [[ $arg_Size_of_batch_filtered -gt 0 && $deploy_start -gt $arg_Size_of_batch_filtered ]]; then
    # IF $arg_Size_of_batch_filtered even defined (Batches only)
    echo "ERROR: deploy_start (${deploy_start}) > arg_Size_of_batch_filtered (${arg_Size_of_batch_filtered}); bailing."
    bail_now=1
fi

if [[ $deploy_end -lt 1 ]]; then
    echo "WARNING: deploy_end (${deploy_start}) < 1; bailing"
    bail_now=1
elif [[ $arg_Size_of_batch_filtered -gt 0 && $deploy_end -gt $arg_Size_of_batch_filtered ]]; then
    # IF $arg_Size_of_batch_filtered even defined (Batches only)
    echo "WARNING: deploy_end (${deploy_start}) > arg_Size_of_batch_filtered-batch_num (${arg_Size_of_batch_filtered}); downing to ${arg_Size_of_batch_filtered}"
    deploy_end=$arg_Size_of_batch_filtered
fi

if [[ $bail_now -gt 0 ]]; then
    echo "Bailing now.  Let's try again with another deploy_range, shall we?"
    exit -1
else
    echo "Running with deploy_start=${deploy_start} & deploy_end=${deploy_end}..."
fi

########################################################
# Part 1c:
# TODO: consider ensuring that sudo is authenticated,
# such that the downstream $on_branch_deploy_script is
# assured of ability to sudo -E on the actual start script,
# for example:
#   ./scripts/run_docker_onVM_dev_bgd_lab_onprem.sh
# perhaps a mere:
#   sudo ls ${arg_input_manifest}
echo "--------------------------------------------------"
echo "As a pre-deployment sudo test, about to sudo ls the arg_input_manifest..."
echo "Please be advised that this may be asking for your sudo credentials."
sudo ls -al ${arg_input_manifest}
########################################################


########################################################
# Part 2: actually deploy the desired M of N sub-batches
########################################################

echo "= = = = = = = = = = = = = = = = = ="
#echo "r3m0: DEBUG: Batch Fun Bypass arg = $PASS_use_batch_DB_bypass_manifest_CSV"
#echo "r3m0: DEBUG: following deploy_loop SHOULD be in SEQ $deploy_start TO $deploy_end"

deploy_loop=0
for deploy_loop in $(seq $deploy_start $deploy_end); do
    echo "= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="
    echo "SUB-BATCH ${deploy_loop} of ${deploy_start}:${deploy_end} on this node, of ${arg_N_subbatches} in total."
    subbatch_suffix=$(printf '%s_subbatch%02d'  "$arg_suffix" "$deploy_loop")
    #WAS: if [[ "$PASS_use_batch_DB_bypass_manifest_CSV" == "" ]]; then
    if [[ "$use_Batches" = "false" ]]; then
        # BATCH NOTE:
        #echo "NOTE: no Batches being used from DB, shall fully use the -m manifest"
        output_file_subbatch=$(printf '%s.%s.csv' "$arg_input_manifest" "$subbatch_suffix")
        batch_args=""
    else
        # BATCH NOTE: use the same input manifest, just don't delete it after!
        output_file_subbatch="${arg_input_manifest}"
        let this_batch_min=(deploy_loop-1)*batch_size+1
        let this_batch_max=(deploy_loop)*batch_size
        if [[ ${this_batch_max} -le  ${arg_Size_of_batch_filtered} ]]; then
            echo "sub-batch ${deploy_loop} to use FULL sub-batch counter range ${this_batch_min}:${this_batch_max} (towards Batch-filtered Subtotal: ${arg_Size_of_batch_filtered})"
        else
            let this_batch_max=arg_Size_of_batch_filtered
            echo "sub-batch ${deploy_loop} to use TRUNCATED sub-batch counter range ${this_batch_min}:${this_batch_max} (towards Batch-filtered Subtotal: ${arg_Size_of_batch_filtered})"
        fi
        PASS_batch_counter_range="-e locutus_batch_filter_counter_range=${this_batch_min}:${this_batch_max}"
        #OKAY: batch_args_preDashB="\"${PASS_use_batch_DB_bypass_manifest_CSV} ${PASS_batch_name} ${PASS_batch_counter_range} ${PASS_batch_status}\""
        # BUT, without the quotes, since used in the subsequent export:
        batch_args_preDashB="${PASS_use_batch_DB_bypass_manifest_CSV} ${PASS_batch_name} ${PASS_batch_counter_range} ${PASS_batch_status}"
        batch_args="-B ${batch_args_preDashB}"
        export LOCUTUS_CONDUCTOR_BATCH_FILTER_ARGS="${batch_args_preDashB}"
        #echo "JUST exported LOCUTUS_CONDUCTOR_BATCH_FILTER_ARGS == ${LOCUTUS_CONDUCTOR_BATCH_FILTER_ARGS}"
    fi

    # NOTE: build deploy_cmd using: ${on_branch_deploy_script}:
    #NOT: deploy_cmd="./${on_branch_deploy_script} -m ${output_file_subbatch} -s ${subbatch_suffix} -b ${DEPLOYER_CONTAINER_BASENAME} ${batch_args}"
    # NOTE: due to the complexities around passing & parsing multiple args collapsed into a single double-quoted arg,
    # leave ${batch_args} out of the deploy_cmd, and instead...
    # ensure that the downstream on_branch_deploy_script uses the exported LOCUTUS_CONDUCTOR_BATCH_FILTER_ARGS:
    deploy_cmd="./${on_branch_deploy_script} -m ${output_file_subbatch} -s ${subbatch_suffix} -b ${DEPLOYER_CONTAINER_BASENAME}"
    echo "Generating DEPLOY command for sub-batch ${deploy_loop} with sub-manifest of: ${output_file_subbatch} as ...."
    echo -e "${TAB}${deploy_cmd}"
    echo "WITH the optional manifest-once Batch args exported as LOCUTUS_CONDUCTOR_BATCH_FILTER_ARGS:"
    echo -e "${TAB}${LOCUTUS_CONDUCTOR_BATCH_FILTER_ARGS}"


    if [[ $arg_do_deploy -lt 1 ]]; then
        echo "DRY RUN **NOT** actually deploying, thanks to arg_do_deploy: ${arg_do_deploy}"
    else
        container_name=$(printf '%s%s' "$DEPLOYER_CONTAINER_BASENAME" "$subbatch_suffix")
        if [[ $arg_delete_containers_predeploy -ne 0 ]]; then
            echo "LIVE CONDUCTOR RUN, pre-DELETING container thanks to arg_delete_containers_predeploy=${arg_delete_containers_predeploy}; for this sub_batch ${deploy_loop}, container_name=${container_name}"
            docker rm ${container_name}
        else
            echo "LIVE CONDUCTOR RUN, **NOT** pre-DELETING container thanks to arg_delete_containers_predeploy=${arg_delete_containers_predeploy}; for this sub_batch ${deploy_loop}, container_name=${container_name}"
        fi

        echo "LIVE CONDUCTOR RUN, **NOW** deploying this sub_batch ${deploy_loop} ..."
        # TODO: eventually capture a status, etc.,?
        #   or perhaps with something like retval=`$deploy_cmd`?
        $deploy_cmd

        echo "Sleeping ${DEPLOY_SLEEP} seconds before next deploy, to space out the SQL DB connection demands"
        sleep ${DEPLOY_SLEEP}
    fi

    # NOTE: a "sudo -E" in the above $on_branch_deploy_script
    # may be asking the user for their sudo authentication
    # TODO: Q: might there be a way to check/enable this above,
    # along with the VAULT_TOKEN check?
    # (e.g., perhaps a simply "sudo ls")

done

#echo "= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="


# AFTER the entire above loop,
# either cleanup/rm the subbatch_manifests, or keep em:
if [[ $arg_keep_subbatch_manifests -ne 0 ]]; then
    echo "KEEPING the sub-batch manifests, thanks to arg_keep_subbatch_manifests: ${arg_keep_subbatch_manifests}"
else
    #echo "Without -K(EEP_subbatch_manifests) configured, removing this sub-batch manifest: ${output_file_subbatch} ..."
    subbatches_suffix=$(printf '%s_subbatch*'  "$arg_suffix" )
    output_file_subbatches=$(printf '%s.%s.csv' "$arg_input_manifest" "$subbatches_suffix")
    echo "Without -K(EEP_subbatch_manifests) configured, removing the following sub-batch manifests: "
    echo " == ${output_file_subbatches} :"
    # TODO: consider use of ${SUDO_CMD}, or usage of this Conductor via sudo, to ensure permissions to remove:
    # or perhaps with something like retval=`rm ${output_file_subbatch}`
    ls -1 ${output_file_subbatches}
    rm ${output_file_subbatches}
fi

echo "Thanks for Conducting!  Bye for now!"
exit 0
