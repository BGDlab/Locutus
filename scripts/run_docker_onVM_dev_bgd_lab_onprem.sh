#!/bin/bash
# run_docker_onVM_dev_bgd_lab_onprem.sh
# with the onprem manifest
# a quick script to simulate a Jenkins launch via general_infra/deploy_etl
# for an image as built via:
# docker build -t locutus_dev_bgd_lab_image:latest .
#
# onprem expects ${LOCUTUS_CONTAINER_SUFFIX} to also be passed in by the environment, as well as:
# * VAULT_ADDR
# * VAULT_NAMESPACE
# * VAULT_TOKEN
# 5/15/2024: now expects ${LOCUTUS_INTERIM_PROCESSING_DIR} for explicitly mounting the LOCUTUS_INTERIM_PROCESSING_DIR...

# NOTE: without the --rm  in the run (such that we can easily check logs)
# need to explicitly remove the previous docker container:
# docker rm locutus_container
# NOTE: now using ${LOCUTUS_CONTAINER_SUFFIX} and removed the automatic "docker rm" so that previous containers are left around for later log retrieval.
export LOCUTUS_CONTAINER_NAME="locutus_container_"${LOCUTUS_CONTAINER_SUFFIX}
echo "DEBUG: will use LOCUTUS_CONTAINER_NAME=[${LOCUTUS_CONTAINER_NAME}].  Thanks!"
#exit -1

echo "DEBUG: will use LOCUTUS_INTERIM_PROCESSING_DIR=[${LOCUTUS_INTERIM_PROCESSING_DIR}]. Please ensure that it is actually -volume mounted and getting data, Thanks!"
#echo "----- ----- ----- ----- -----"
#echo "DEBUG... about to ls LOCUTUS_INTERIM_PROCESSING_DIR..."
#ls -alR ${LOCUTUS_INTERIM_PROCESSING_DIR}
#ls -al ${LOCUTUS_INTERIM_PROCESSING_DIR}/*
#echo "And that is the ls of LOCUTUS_INTERIM_PROCESSING_DIR."
#echo "----- ----- ----- ----- -----"

echo "AND: will use LOCUTUS_ISILON_OUTPUT_DIR=[${LOCUTUS_ISILON_OUTPUT_DIR}]. Please ensure that it is ALSO actually -volume mounted and getting data, Thanks!"
#echo "----- ----- ----- ----- -----"
#echo "DEBUG... about to ls LOCUTUS_ISILON_OUTPUT_DIR..."
#ls -alR ${LOCUTUS_ISILON_OUTPUT_DIR}
#ls -al ${LOCUTUS_ISILON_OUTPUT_DIR}/*
#echo "And that is the ls of LOCUTUS_ISILON_OUTPUT_DIR."
#echo "----- ----- ----- ----- -----"


# set default VAULT_ADDR and VAULT_NAMESPACE


# NOTE: the following docker run currently still has CURR_ENV=develop, as:
# -e CURR_ENV=develop \
# TODO: double check and change to production?

# NOTE: can test the following docker run WITHOUT a -d to keep in ForeGround for initial tests,
# but now trying in the BackGround via a detached docker container:
# NOTE: that other approaches to backgrounding the process could involve:
#  EITHER launching with an & at the end,
#   OR hitting ^Z to stop the process, jobs to see it stopped, and bg to send to the background.
# TODO: determining the best way to put the process into the background AND obtain the logs
# noting that logs could be obtained by appending the following to the deployment: | tee logfile.txt
#######
# WARNING: will need to volume mount  EACH possible input manifest.csv, OR to have a separate link according to which module/cmd running)
# NOTE: currently using -d to detach process into background:
# TESTING summarize_status w/o the -d:
# WAS: -v ${LOCUTUS_INTERIM_PROCESSING_DIR}:${LOCUTUS_INTERIM_PROCESSING_DIR} \
#	but still seems to be a separate volume
# TRY mount, via:
#	   --mount src="${LOCUTUS_INTERIM_PROCESSING_DIR}",target="${LOCUTUS_INTERIM_PROCESSING_DIR}",type=bind \
# Looking like it will need to be back in the Dockerfile for a build, via RUN --mount
# (in which case, only provide the top-level directory, for each VM/env to attend to their specific subfolder of interest.
# NOTE: see the RUN --mount ,z recommendation at:
#    https://github.com/containers/podman/issues/15423
##########

#######
# r3m0: NOTE: adding --rm back into the run -d, so that the same container name can be used for multiple tests:
#docker run -d --rm --name ${LOCUTUS_CONTAINER_NAME} \
# and removing the -rm again to be able to look into the stopped container
# onprem without the following:
#           -v ${GOOGLE_APPLICATION_CREDENTIALS}:/opt/app/GOOGLE_APP_CREDS.json \
#           -e GOOGLE_APPLICATION_CREDENTIALS=/opt/app/GOOGLE_APP_CREDS.json \
docker run -d --name ${LOCUTUS_CONTAINER_NAME} \
           -v /tmp:/tmp  \
           -v `pwd`/config.yaml:/opt/app/config.yaml \
           -v ${LOCUTUS_INTERIM_PROCESSING_DIR}:${LOCUTUS_INTERIM_PROCESSING_DIR}:rw \
           -v ${LOCUTUS_ISILON_OUTPUT_DIR}:${LOCUTUS_ISILON_OUTPUT_DIR}:rw \
           -v `pwd`/dicom_stage_compare_manifest.csv:/opt/app/dicom_stage_compare_manifest.csv \
           -v `pwd`/dicom_summarize_stats_manifest.csv:/opt/app/dicom_summarize_stats_manifest.csv \
           -v `pwd`/dicom_split_accession_manifest.csv:/opt/app/dicom_split_accession_manifest.csv \
           -v `pwd`/onprem_dicom_images_manifest.csv:/opt/app/onprem_dicom_images_manifest.csv \
           -v `pwd`/gcp_dicom_images_manifest.csv:/opt/app/gcp_dicom_images_manifest.csv \
           -e CURR_ENV=develop \
           -e VAULT_ADDR=${VAULT_ADDR} \
           -e VAULT_TOKEN=${VAULT_TOKEN} \
           -e VAULT_NAMESPACE=${VAULT_NAMESPACE} \
           -e DOCKERHOST_HOSTNAME=`hostname` \
           locutus_dev_bgd_lab_image /opt/app/run_main.sh
