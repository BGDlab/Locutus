#!/bin/bash
# a quick script to simulate a Jenkins launch via general_infra/deploy_etl
# for an image as built via:
# docker build -t locutus_local:latest .
# with CURR_ENV=develop

# NOTES on the s3 & GS targets:
# AWS s3: expects ${AWS_SECRET_ACCESS_KEY}, ${AWS_ACCESS_KEY_ID}, and ${AWS_SECURITY_TOKEN} to already be set & active via local `awsDEV` function
# GCP GS: expects ${GOOGLE_APPLICATION_CREDENTIALS} to already be defined and exist somewhere as a .json

# WARNING: the gcp_dicom_images_manifest.csv & onprem_dicom_images_manifest.csv
# are currently just currently built right on into the Docker image....
# TODO: may want to eventually also link it in with a -v:

# NOTE: without the --rm  in the run (such that we can easily check logs)
# need to explicitly remove the previous docker container:
docker rm locutus_local_develop

# NOTE: no -d, keep in FG:
docker run --name locutus_local_develop \
           -v /tmp:/tmp  \
           -v `pwd`/config.yaml:/opt/app/config.yaml \
           -v /tmp:/tmp  \
           -v ${LOCUTUS_INTERIM_PROCESSING_DIR}:${LOCUTUS_INTERIM_PROCESSING_DIR}:rw \
           -v `pwd`/dicom_stage_compare_manifest.csv:/opt/app/dicom_stage_compare_manifest.csv \
           -v `pwd`/dicom_summarize_stats_manifest.csv:/opt/app/dicom_summarize_stats_manifest.csv \
           -v `pwd`/dicom_split_accession_manifest.csv:/opt/app/dicom_split_accession_manifest.csv \
           -v `pwd`/onprem_dicom_images_manifest.csv:/opt/app/onprem_dicom_images_manifest.csv \
           -v `pwd`/gcp_dicom_images_manifest.csv:/opt/app/gcp_dicom_images_manifest.csv \
           -v ${GOOGLE_APPLICATION_CREDENTIALS}:/opt/app/GOOGLE_APP_CREDS.json \
           -e GOOGLE_APPLICATION_CREDENTIALS=/opt/app/GOOGLE_APP_CREDS.json \
           -e CURR_ENV=develop \
           -e VAULT_ADDR=${VAULT_ADDR} \
           -e VAULT_TOKEN=${VAULT_TOKEN} \
           -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
           -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
           -e AWS_SECURITY_TOKEN=${AWS_SECURITY_TOKEN} \
           -e DOCKERHOST_HOSTNAME=r3m0sDockerHost \
           locutus_local /opt/app/run_main.sh
