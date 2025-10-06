#!/bin/bash
# run_docker_locally_as_bgd_lab_onprem.sh
# a quick script to simulate a Jenkins launch via general_infra/deploy_etl OnPrem
# TODO: ===> confirm removal of internal CHOP details
# for an image as built via:
# docker build -t locutus_local:latest .
# docker build -t locutus_local_dev_bgd_lab:latest .
# or, on the M1:
# docker build -t locutus_dev_bgd_lab_image:latest --platform linux/arm64/v8 .
# docker build -t locutus_dev_bgd_lab_image:latest --platform linux/amd64 .
#############################################################################
# r3m0 NOTE: + added VAULT_NAMESPACE to the docker run
# and changed:
#           -e CURR_ENV=develop \
# to
#           -e CURR_ENV=dev_bgd_lab \
# AND:
# started off as an AWS-based deployment, and adding in the GCP stuff
#
# REMOVED the AWS requirements, (which are currently not being set on my machine as awsDEV is not working)
#           -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
#           -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
#           -e AWS_SECURITY_TOKEN=${AWS_SECURITY_TOKEN} \
# as this will be used to test Google Storage only, at the moment
# Google Store NOTE:
# expects ${GOOGLE_APPLICATION_CREDENTIALS} to already be defined and exist somewhere as a .json
#
# 6/25/2024: now expects ${LOCUTUS_INTERIM_PROCESSING_DIR} for explicitly mounting the LOCUTUS_INTERIM_PROCESSING_DIR...

export LOCUTUS_CONTAINER_NAME="locutus_container_"${LOCUTUS_CONTAINER_SUFFIX}
echo "DEBUG: will use LOCUTUS_CONTAINER_NAME=[${LOCUTUS_CONTAINER_NAME}].  Thanks!"

echo "DEBUG: will use LOCUTUS_INTERIM_PROCESSING_DIR=[${LOCUTUS_INTERIM_PROCESSING_DIR}]. Please ensure that it is actual -volume mounted and getting data, Thanks!"


# NOTE: without the --rm  in the run (such that we can easily check logs)
# need to explicitly remove the previous docker container:
#docker rm locutus_local_develop
#docker rm locutus_local_dev_bgd_lab

# NOTE: no -d, keep in FG:
#docker run --name locutus_local_develop \
docker run --name ${LOCUTUS_CONTAINER_NAME} \
           -v /tmp:/tmp  \
           -v `pwd`/config.yaml:/opt/app/config.yaml \
           -v ${LOCUTUS_INTERIM_PROCESSING_DIR}:${LOCUTUS_INTERIM_PROCESSING_DIR}:rw \
           -v `pwd`/dicom_stage_compare_manifest.csv:/opt/app/dicom_stage_compare_manifest.csv \
           -v `pwd`/dicom_summarize_stats_manifest.csv:/opt/app/dicom_summarize_stats_manifest.csv \
           -v `pwd`/dicom_split_accession_manifest.csv:/opt/app/dicom_split_accession_manifest.csv \
           -v `pwd`/onprem_dicom_images_manifest.csv:/opt/app/onprem_dicom_images_manifest.csv \
           -v `pwd`/gcp_dicom_images_manifest.csv:/opt/app/gcp_dicom_images_manifest.csv \
           -e CURR_ENV=dev_bgd_lab \
           -e VAULT_ADDR=${VAULT_ADDR} \
           -e VAULT_TOKEN=${VAULT_TOKEN} \
           -e VAULT_NAMESPACE=${VAULT_NAMESPACE} \
           -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
           -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
           -e AWS_SECURITY_TOKEN=${AWS_SECURITY_TOKEN} \
           -e DOCKERHOST_HOSTNAME=r3m0sDockerHost \
           locutus_dev_bgd_lab_image /opt/app/run_main.sh
