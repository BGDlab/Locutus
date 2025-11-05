#!/bin/bash
# deploy_locutus_onVM_dev_bgd_lab_onprem.sh
# for running Locutus to emulate the Jenkins deployment for the dev_bgd_lab BRANCH of the production instance of Orthanc (ORTHANCINTPROD)
#
# usage:
# $ ./deploy_locutus_onVM_dev_bgd_lab_onprem.sh [-m manifest_in] [-s suffix]
# NOTE: no need to call with sudo or sudo -E, just let the sudo -E below ask for user credentials...
#
# 2025 June 13, finally adding FROM deploy_locutus_onVM_dev_bgd_lab_onprem:....
# 2024 Nov 01 updates to support the Locutus Conductor's multi-sub-batch deployment with cmd-line args,
# for example:
# DRY DEPLOY of sub-batch 1 with sub-manifest of: ./test_15batch_4conductor_2024oct31.csv.testfun_subbatch01.csv ....
# ./deploy_locutus_locally_dev_bgd_lab_onprem.sh --manifest ./test_15batch_4conductor_2024oct31.csv.testfun_subbatch01.csv --suffix testfun_subbatch01
# for bash getopts:
# ./deploy_locutus_locally_dev_bgd_lab_onprem.sh -m ./test_15batch_4conductor_2024oct31.csv.testfun_subbatch01.csv -s testfun_subbatch01
#
# Expects the following environment variables:
# * VAULT_ADDR
# * VAULT_NAMESPACE
# * VAULT_TOKEN
# * LOCUTUS_INTERIM_PROCESSING_DIR
# * LOCUTUS_ISILON_OUTPUT_DIR


# NOTE: introducing 'sudo' to all the unlink/ln/rm MANIFEST_LINK[_BACKUP] file manipulations, as needed on VMs
#SUDO_CMD='sudo'
# to test locally without sudo in these new places, use an empty SUDO_CMD, via:
SUDO_CMD=''
# NOTE: 3x or so sudo commands already existing in this for onVM deployment; not changing those at this point.

DRY_RUN=0
# to test locally without any of the actual sudo -E ./scripts/run_docker_onVM_dev_bgd_lab_onprem.sh start script:
#DRY_RUN=1

# default arg_manifest_in:
MANIFEST_LINK='./onprem_dicom_images_manifest.csv'
MANIFEST_LINK_BACKUP='./onprem_dicom_images_manifest.csv.BAK'

# default arg_base:
DEFAULT_LOCUTUS_CONTAINER_BASENAME='locutus_container_'

############################################################
# Help                                                     #
############################################################
show_usage()
{
   # Display Usage/Help
   echo "deploy_locutus_onVM_dev_bgd_lab_onprem.sh:"
   #echo
   echo "syntax: deploy_locutus_onVM_dev_bgd_lab_onprem.sh -m manifest_in -s suffix"
   echo "options:"
   echo "m     manifest input file; else using existing MANIFEST_LINK=${MANIFEST_LINK}"
   echo "s     suffix for Locutus container name upon deployment; else prompting for suffix"
   echo "b     base for Locutus container name upon deployment; else using existing LOCUTUS_CONTAINER_BASENAME=${LOCUTUS_CONTAINER_BASENAME}"
   echo "h     help: print this"
   echo "u     usage: print that"
   #echo
}

echo "Welcome to $0"

if [[ ${DRY_RUN} -eq 0 ]]; then
    echo "HEADS UP! this is LIVE and is *NOT* a test, as DRY_RUN=${DRY_RUN}"
else
    echo "DRY_RUN=${DRY_RUN}: **NOT** actually calling the start script: sudo -E ./scripts/run_docker_onVM_dev_bgd_lab_onprem.sh"
    echo "to allow DEBUG test of just the MANIFEST_LINK_BACKUP/MANIFEST_LINK setup and restoration code... good luck!"
fi

arg_manifest_in=''
arg_suffix=''
# TODO: include an explicit -h(elp)/-u(sage) in the below (and to conduct_locutus_subbatches.sh, to whence it was also shared)
OPTSTRING="m:s:b:"
while getopts ${OPTSTRING} opt
do
    case "${opt}" in
        m) # manifest
            arg_manifest_in=${OPTARG};;
        b) # base container name
            arg_base=${OPTARG};;
        s) # suffix to container name
            arg_suffix=${OPTARG};;
        h | u) # help/usage:
            show_usage
            exit 0;;
        \?) #  (\? or *) == any others, unknown:
            echo "ERROR: unknown flag ${OPTARG}"
            show_usage
            exit -1 ;;
    esac
done

################
# arg_base:
echo "- - - - -"
echo "arg_base=${arg_base}"
if [ -z "${arg_base}" ]; then
    echo "arg_base is unset or set to the empty string"
    echo "NOTE: this means that the Locutus container base will default to: ${LOCUTUS_CONTAINER_BASENAME}"
    export LOCUTUS_CONTAINER_BASENAME=${DEFAULT_LOCUTUS_CONTAINER_BASENAME}
else
    echo "arg_base is set to: ${arg_base}"
    export LOCUTUS_CONTAINER_BASENAME=${arg_base}
fi


################
# arg_suffix:
echo "- - - - -"
echo "arg_suffix=${arg_suffix}"
if [ -z "${arg_suffix}" ]; then
    echo "arg_suffix is unset or set to the empty string"
    echo "WARNING: this means that you will be prompted for the desired Locutus container suffix"
else
    echo "arg_suffix is set to: ${arg_suffix}"
fi


################
# arg_manifest:
################
old_manifest_linked_file=''
manifest_linked=0
manifest_backed_up=0
manifest_backup_linked=0
################
echo "- - - - -"
echo "arg_manifest_in=${arg_manifest_in}"
if [ -z "${arg_manifest_in}" ]; then
    echo "arg_manifest_in is unset or set to the empty string"
    echo "WARNING: this means that the currently following Locutus MANIFEST_LINK will be utilized: ${MANIFEST_LINK}"
    #ls -al ${MANIFEST_LINK}
else
    echo "arg_manifest_in is set to: ${arg_manifest_in}"
fi
# and its effective else, as testing for later utilization with the relinking of MANIFEST_LINK:
if [ -n "${arg_manifest_in}" ]; then
    echo "arg_manifest_in is set to a NON-empty string, checking it..."
    ls -al ${arg_manifest_in}
    if [ -f "${arg_manifest_in}" ]; then
        echo "YAY, arg_manifest_in exists!"
    else
        echo "ERROR: file not found for arg_manifest_in: ${arg_manifest_in}"
        echo "Please ensure that your input manifest filename matches arg_manifest_in then try again"
        echo "Locutus, its Conductor, and this deploy script all thank you!"
        echo "Bailing early."
        exit -1
    fi
    ######## ONLY aim to swap arg_manifest_in if so supplied as a cmd-line arg:
    echo "SINCE arg_manifest_in is set, will aim to swap arg_manifest_in into the following Locutus MANIFEST_LINK: ${MANIFEST_LINK}"

    # MANIFEST_LINK:
    echo "- - - - -"
    echo "Checking the current MANIFEST_LINK = ${MANIFEST_LINK} ..."
    ls -al ${MANIFEST_LINK}
    # okay for MANIFEST_LINK to already exist as a link (nor not at all), but not as a file or as a directory)
    if [ -L "${MANIFEST_LINK}" ]; then
        manifest_linked=1
        echo "YAY, MANIFEST_LINK is currently linked, as typically expected...."
        # Now, get the current MANIFEST_LINK target, to backup:
        old_manifest_linked_file=`ls -l ${MANIFEST_LINK} | awk '{print $NF}'`
        echo "OLD MANIFEST_LINK target file=${old_manifest_linked_file}"
    elif [ -f "${MANIFEST_LINK}" ]; then
        echo "ERROR: MANIFEST_LINK is currently a file (not a link, as typically expected)...."
        echo "Please remove the following file prior to running this deploy script."
        ls -al ${MANIFEST_LINK}
        echo "Locutus, its Conductor, and this deploy script all thank you!"
        echo "Bailing early."
        exit -1
    elif [ -d "${MANIFEST_LINK}" ]; then
        echo "ERROR: MANIFEST_LINK is currently a directory (not a link, as typically expected)...."
        echo "Please remove the following directory prior to running this deploy script."
        ls -aldF ${MANIFEST_LINK}
        echo "Locutus, its Conductor, and this deploy script all thank you!"
        echo "Bailing early."
        exit -1
    else
        echo "WARNING: MANIFEST_LINK is **NOT** currently existing; while unexpected, that is A-Okay."
    fi

    echo "- - - - -"
    if [[ $manifest_linked -eq 0 ]]; then
        # NO previous MANIFEST_LINK linked
        echo "Linking *NEW* MANIFEST_LINK to arg_manifest_in=${arg_manifest_in} ..."
        ${SUDO_CMD} ln -s ${arg_manifest_in} ${MANIFEST_LINK}
        ls -al ${MANIFEST_LINK}
    else
        # WAS: if [[ $manifest_linked -ne 0 ]]; then
        # MANIFEST_LINK currently linked
        # Okay, so we've already got a linked manifest.
        # Is the existing MANIFEST_LINK target the same as our new arg_manifest_in?
        if [ "$arg_manifest_in" = "$old_manifest_linked_file" ]; then
            echo "MATCH FOUND: new arg_manifest_in $arg_manifest_in == existing MANIFEST_LINK target $old_manifest_linked_file"
            echo "NOTE: no need to backup MANIFEST_LINK"
        else
            # existing link differs from arg_manifest_in, so back it up
            echo "NOTE: arg_manifest_in $arg_manifest_in **DIFFERS** from MANIFEST_LINK target $old_manifest_linked_file ; backing up the MANIFEST_LINK ..."
            # MANIFEST_LINK_BACKUP:
            echo "- - - - -"
            echo "Checking the current MANIFEST_LINK_BACKUP = ${MANIFEST_LINK_BACKUP} ..."
            ls -al ${MANIFEST_LINK_BACKUP}
            # okay for MANIFEST_LINK_BACKUP to already exist as a link (nor not at all), but not as a file or as a directory)
            if [ -L "${MANIFEST_LINK_BACKUP}" ]; then
                manifest_backup_linked=1
                echo "YAY, MANIFEST_LINK_BACKUP is currently linked; while unexpected, that is A-Okay. Unlinking now..."
                ${SUDO_CMD} unlink ${MANIFEST_LINK_BACKUP}
            elif [ -f "${MANIFEST_LINK_BACKUP}" ]; then
                echo "ERROR: MANIFEST_LINK_BACKUP is currently a file (not a link, as typically expected)...."
                echo "Please remove the following file prior to running this deploy script."
                ls -al ${MANIFEST_LINK_BACKUP}
                echo "Locutus, its Conductor, and this deploy script all thank you!"
                echo "Bailing early."
                exit -1
            elif [ -d "${MANIFEST_LINK_BACKUP}" ]; then
                echo "ERROR: MANIFEST_LINK_BACKUP is currently a directory (not a link, as typically expected)...."
                echo "Please remove the following directory prior to running this deploy script."
                ls -aldF ${MANIFEST_LINK_BACKUP}
                echo "Locutus, its Conductor, and this deploy script all thank you!"
                echo "Bailing early."
                exit -1
            else
                echo "WARNING: MANIFEST_LINK_BACKUP is **NOT** currently existing; totally good and as expected."
            fi

            echo "- - -"
            echo "Relinking MANIFEST_LINK_BACKUP to the MANIFEST_LINK target=$old_manifest_linked_file ..."
            ${SUDO_CMD} ln -s ${old_manifest_linked_file} ${MANIFEST_LINK_BACKUP}
            ls -al ${MANIFEST_LINK_BACKUP}

            echo "- - -"
            # And, set the arg_manifest_in as the new MANIFEST_LINK target file:
            echo "Unlinking the OLD MANIFEST_LINK..."
            ${SUDO_CMD} unlink ${MANIFEST_LINK}

            echo "- - - - -"
            echo "Re-Linking MANIFEST_LINK to arg_manifest_in=${arg_manifest_in} ..."
            ${SUDO_CMD} ln -s ${arg_manifest_in} ${MANIFEST_LINK}
            ls -al ${MANIFEST_LINK}
            manifest_backed_up=1
        fi
    fi
fi
# end of if [ -n "${arg_manifest_in}" ]; then

# Now that any pre-existing MANIFEST_LINK (if existing at all) has been backed up to MANIFEST_LINK_BACKUP
# Can setup

echo "---------"
echo "All looks good and setup to go.... on to the good ol deploy parts..."


###################################################
# pre-December/November 2024 deploy_locutus_onVM_dev_bgd_lab_onprem.sh follows,
# with only minor mods, namely:
# * if $arg_suffix provided, no need to list the current containers and prompt for a new suffix
# * no need to show the final docker logs, perhaps even if $arg_suffix wasn't supplied anymore?
# * introducing ${DRY_RUN} check before actually calling scripts/run_docker_onVM_dev_bgd_lab_onprem.sh start script:
#
# TODO: consider replacing any other sudo in the old code below with the new ${SUDO_CMD}
###################################################


echo "- - - - -"
if [ -z "${arg_suffix}" ]; then
    # ${arg_suffix} was NOT supplied at the cmd-line
    echo "All previous container names for locutus_containers_ are as follows:"
    ${SUDO_CMD} docker ps -a | grep locutus_container | awk '{print $NF}'

    echo -n "Please enter a LOCUTUS_CONTAINER_SUFFIX to follow ${LOCUTUS_CONTAINER_BASENAME} : "
    read line_in
    echo "Exporting LOCUTUS_CONTAINER_SUFFIX=${line_in} ..."
    export LOCUTUS_CONTAINER_SUFFIX=${line_in}
else
    # ${arg_suffix} WAS supplied at the cmd-line
    echo "Bypassing prompt for LOCUTUS_CONTAINER_SUFFIX, since already provided as cmd-line arg"
    echo "Exporting LOCUTUS_CONTAINER_SUFFIX=arg_suffix=${arg_suffix} ..."
    export LOCUTUS_CONTAINER_SUFFIX=${arg_suffix}
fi
echo "- - - - -"

echo "Exported LOCUTUS_CONTAINER_SUFFIX:"
echo ${LOCUTUS_CONTAINER_SUFFIX}

echo "======================================================="
if [[ ${DRY_RUN} -eq 0 ]]; then
    echo "Hold onto your horses..... about to run the actual start script: ./scripts/run_docker_onVM_dev_bgd_lab_onprem.sh"
    # sudo -E ./scripts/run_docker_locally_as_dev_bgd_lab_onprem.sh
    # NOTE: no need for sudo locally:
    ./scripts/run_docker_locally_as_dev_bgd_lab_onprem.sh
    # NOTE: MOVING the below sleep until AFTER the clues for running docker logs, such that they can be ^C/^V during the sleep :-)
    #echo "5 seconds of zzz before ECHOING how to run docker logs"
    #sleep 5
else
    echo "DRY_RUN: **NOT** actually calling the start script: sudo -E ./scripts/run_docker_onVM_dev_bgd_lab_onprem.sh"
    echo "to allow DEBUG test of just the MANIFEST_LINK_BACKUP/MANIFEST_LINK restoration code, below..."
fi
echo "======================================================="


###########################################
# NOTE: Disable vvvvv logs, with no -d in ./scripts/run_docker_locally_as_dev_bgd_lab_onprem.sh
#
#./scripts/run_docker_onVM_dev_bgd_lab_onprem.sh
#echo "5 seconds of zzz before auto docker logs"
#sleep 5

# NOTE: currently only the ${LOCUTUS_CONTAINER_SUFFIX} env var is read by run_docker_onVM_develop.sh,
# which prepends its own base name of "locutus_container_"
# TODO: consider creating that entire container name here.
# Until then, replicating the base name below in the docker logs

#sudo docker logs -f locutus_container_${LOCUTUS_CONTAINER_SUFFIX}
# no need for sudo locally:
#docker logs -f locutus_container_${LOCUTUS_CONTAINER_SUFFIX}
#
# NOTE: Disable ^^^^^ logs, with no -d in ./scripts/run_docker_locally_as_dev_bgd_lab_onprem.sh
###########################################


# NOTE: return the previous MANIFEST_LINK from that temporarily saved in MANIFEST_LINK_BACKUP
if [[ $manifest_backed_up -ne 0 ]]; then
    echo "Returning MANIFEST_LINK to its previous state ..."

    #echo "1) unlink MANIFEST_LINK"
    echo "* Unlinking MANIFEST_LINK ..."
    ${SUDO_CMD} unlink ${MANIFEST_LINK}

    #echo "2) ln -s current-MANIFEST_LINK_BACKUP MANIFEST_LINK"
    echo "* Re-linking old_manifest_linked_file to MANIFEST_LINK ..."
    ${SUDO_CMD} ln -s ${old_manifest_linked_file} ${MANIFEST_LINK}
    ls -al ${MANIFEST_LINK}

    #echo "3) unlink MANIFEST_LINK_BACKUP (with no need to reinstate its previous BACKUP link)"
    echo "* Un-linking MANIFEST_LINK_BACKUP ..."
    ${SUDO_CMD} unlink ${MANIFEST_LINK_BACKUP}
    ls -al ${MANIFEST_LINK_BACKUP}
else
    echo "NOTE: no MANIFEST_LINK backed up & in need of return to its previous state; leaving MANIFEST_LINK_BACKUP and MANIFEST_LINK as is."
fi

echo "---------"
# NOTE: Disabling the auto docker logs:
# WAS: sudo docker logs -f locutus_container_${LOCUTUS_CONTAINER_SUFFIX}
echo "No longer automatically invoking docker logs, to support the Locutus Conductor and team."
echo "Instead, just sharing the following command-line for you to log it yourself:"
echo "----------------------------------------------------------------------------------"
echo "     ${SUDO_CMD} docker logs -f ${LOCUTUS_CONTAINER_BASENAME}${LOCUTUS_CONTAINER_SUFFIX}"
echo "----------------------------------------------------------------------------------"

echo "Sleeping 5 seconds of zzz before exiting, to allow each container a few moments to start spinning up...."
sleep 5

echo "And Now, back to our regularly scheduled Conductor program."
echo "Goodbye from $0, and thanks for deploying! -Locutus"
