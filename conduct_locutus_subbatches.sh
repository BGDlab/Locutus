#!/bin/bash
# conduct_locutus_subbatches.sh
# 2024 October 31.... Boo!
# prior last revision of: 20 Nov 2024
# last updated: 05 May 2025, Happy Cinco de Mayo!
# w/ minor optimization update to the use_M_max_subbatch_size computation given use_N_num_subbatches
# -r3m0
#
# Call with:
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
#   w/ default OnPrem De-ID module as default deployed
#
# % sudo -E ./conduct_locutus_subbatches.sh -m manifest_input -s suffix -dD -N 15 -r 1:8 -O
#   deploy OnPrem module to generate -N 15 sub-batches,
#   -d deploying sub-batches for those matching -r range 1 through 8 on this node
#   -D pre-Deleting any same-named containers,
#   (NOT Keeping the sub-batch manifests)
#   -O OnPrem De-ID module explicitly chosen to be deployed (as an example for other such modules)
#
###############################################################################
# NOTE: see below "NOTE: build on_branch_deploy_script using:" for on_branch_deploy_script generation, using:
#   arg_DICOM_DeID_module (onprem, default)
# TODO: incorporate the following, once options other than their defaults are warranted:
#   arg_on (default='VM')
#   arg_branch (default="dev_bgd_lab")
###############################################################################


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
# NOTE: using lowercase `onprem` for the following to DEFAULT_DICOM_DeID_MODULE='OnPrem'
# to align w/ existing deploy_locutus_onVM_dev_bgd_lab_${arg_DICOM_DeID_module}.sh script names.
DICOM_DeID_MODULE_ONPREM='onprem'
DEFAULT_DICOM_DeID_MODULE=${DICOM_DeID_MODULE_ONPREM}
arg_DICOM_DeID_module=${DEFAULT_DICOM_DeID_MODULE}
################################
# NOTE: build on_branch_deploy_script using:
#   TODO: arg_on
#   TODO: arg_branch
on_branch_deploy_script="deploy_locutus_onVM_dev_bgd_lab_"${arg_DICOM_DeID_module}".sh"
# NOTE: eventually to evolve into...
#on_branch_deploy_script="deploy_locutus_on"+${arg_on}+"_"+${arg_branch}+"_"+${arg_DICOM_DeID_module}+".sh"
###############################################################################

# given VM nodes of up to 8 CPUs each, set a maximum default range to deploy:
# 05/04/2025 NOTE: bumping from 8 to 16 deployments per mode,
# in case we want to start testing performance response up to containers per node = 2x # CPUs (8) per node
# Suspicion is that (especially for any cloud-based services), with much asyncronous wait time, multiple containers per CPU might be reasonable.
MAX_DEPLOYMENTS_PER_NODE=16

arg_range_to_deploy=''
###############################################################################
# TODO: extract from arg_range_to_deploy='5:5'
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
   echo "r     FROM:TO range to deploy (min:max = 1:N); REQUIRED if -d(eploying), using default of 1:N,"
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
   echo "O     OnPrem Locutus DeID module (${DICOM_DeID_MODULE_ONPREM}, default)"
   # TODO: finish up the following...
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
OPTSTRING=":hudDKs:m:r:M:N:O"
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
            on_branch_deploy_script="deploy_locutus_onVM_dev_bgd_lab_"${arg_DICOM_DeID_module}".sh";;
        r) # range_to_deploy OPTARG:
            arg_range_to_deploy=${OPTARG};;
        d) # deploy:
            arg_do_deploy=1;;
        D) # delete containers prior to deploy
            arg_delete_containers_predeploy=1;;
        K) # keep manifests
            arg_keep_subbatch_manifests=1;;
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
fi
echo "-------------------------------------------"
echo "Gives:"
echo "use_M_max_subbatch_size=${use_M_max_subbatch_size}"
echo "use_N_num_subbatches=${use_N_num_subbatches}"
echo "extra_beyond_M_max_size_for_subbatch_N=${extra_beyond_M_max_size_for_subbatch_N}"
# pre-compute the extended batch size for subbatch N:
let subbatch_N_size=use_M_max_subbatch_size+extra_beyond_M_max_size_for_subbatch_N
echo "subbatch_N_size=${subbatch_N_size}"


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
    echo "Setting calc_deploy_end to use_N_num_subbatches=${calc_deploy_start}"
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

# regardless, exit early:
# r3m0: WIP in proceeding with deployment
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
elif [[ $deploy_start -gt $subbatch_num ]]; then
    echo "ERROR: deploy_start (${deploy_start}) > max_sub-batch_num (${subbatch_num}); bailing."
    bail_now=1
fi


if [[ $deploy_end -lt 1 ]]; then
    echo "WARNING: deploy_end (${deploy_start}) < 1; bailing"
    bail_now=1
elif [[ $deploy_end -gt $subbatch_num ]]; then
    echo "WARNING: deploy_start (${deploy_start}) > max_sub-batch_num (${subbatch_num}); downing to ${subbatch_num}"
    deploy_end=$subbatch_num
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

deploy_loop=0
for deploy_loop in $(seq $deploy_start $deploy_end); do
    subbatch_suffix=$(printf '%s_subbatch%02d'  "$arg_suffix" "$deploy_loop")
    output_file_subbatch=$(printf '%s.%s.csv' "$arg_input_manifest" "$subbatch_suffix")

    # NOTE: build deploy_cmd using: ${on_branch_deploy_script}:
    deploy_cmd="./${on_branch_deploy_script} -m ${output_file_subbatch} -s ${subbatch_suffix} -b ${DEPLOYER_CONTAINER_BASENAME}"
    echo "Generating DEPLOY command for sub-batch ${deploy_loop} with sub-manifest of: ${output_file_subbatch} as ...."
    echo -e "${TAB}${deploy_cmd}"


    if [[ $arg_do_deploy -lt 1 ]]; then
        echo "DRY RUN **NOT** actually deploying, thanks to arg_do_deploy: ${arg_do_deploy}"
    else
        if [[ $arg_delete_containers_predeploy -ne 0 ]]; then
            container_name=$(printf '%s%s' "$DEPLOYER_CONTAINER_BASENAME" "$subbatch_suffix")
                echo "LIVE CONDUCTOR RUN, pre-DELETING container thanks to arg_delete_containers_predeploy=${arg_delete_containers_predeploy}; for this sub_batch ${deploy_loop}, container_name=${container_name}..."
            docker rm ${container_name}
        else
            echo "LIVE CONDUCTOR RUN, **NOT** pre-DELETING container thanks to arg_delete_containers_predeploy=${arg_delete_containers_predeploy}; for this sub_batch ${deploy_loop}, container_name=${container_name}..."
        fi
        echo "LIVE CONDUCTOR RUN, **NOT YET** deploying this sub_batch ${deploy_loop} now..."
        # TODO: confirm that the following will suffice, (so far so good)
        # OR if we will want to capture a status, etc.,?
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
