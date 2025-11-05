#!/bin/bash
# deploy_etl.sh: generalized Execute shell command for Jenkins deploy jobs specific to ETL:
# incorporating new elements for the July/August 2020 switch to RIS's vault server and corresponding minor restructuring
#
# EXPECTS the following environmental variables to be injected:
# HOSTNAME (as usually provided by the shell, same as hostname command from /etc/hostname)
#	sets DOCKERHOST_HOSTNAME within the container if originating hostname is needed.
# AND: VAULT_EIG_LOCATION & ENVIRONMENT for: ${VAULT_NAMESPACE}/${VAULT_EIG_LOCATION}/${ENVIRONMENT}
# AND: VAULT_ADDR, VAULT_TOKEN, VAULT_NAMESPACE
# AND: XTRA_DOCKER_RUN_FLAGS to support optional detached daemon launching with a -d, etc.
# AND: a separate JOB_DESCRIPTION (via Jenkins {job_description}) and INPUT_MANIFEST_NAME (via Jenkins {safe_input_manifest_name_csv})
#     so that they can be more easily quoted here, since these fields may have spaces
# AND: CONCURRENT_CONTAINER_NAME_SUFFIX as optional with Jenkins BUILD_ID var or other distinct value,
#	    for concurrent deployments of a same container name (in the same or other Jenkins job)
# PLUS: CONCURRENT_TRY_DOCKER_RMI to otherwise attempt to remove docker image,
#     when it won't normally be done so for such concurrent deployments.
# AND: GCP_CREDS_VAULT_PATH for any GCP-specific jobs
#     to create the corresponding ${GOOGLE_APPLICATION_CREDENTIALS} within the docker container, and its full Vault path, as:
#     ${GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH} = ${VAULT_NAMESPACE}/${VAULT_EIG_LOCATION}/${GCP_CREDS_VAULT_PATH}
#
# Defaulting docker run name to {$VAULT_EIG_LOCATION}_${ENVIRONMENT}${CONCURRENT_CONTAINER_NAME_SUFFIX}


# NOTE: safe_input might not be available for all deployments,
# locutus-gcpdicom-deploy does seem to have:
# export safe_input_manifest_name_csv=`echo ${gcp_dicom_images_manifest} | sed 's/ /_/g'`
# but the dicom-staging-deploy script does not.

echo "Hello from the generalized Jenkins deploy_etl script!"
echo "deploy_etl: Starting at: `date`"
# 8/06/2024: in searching for the source of an unexpected call.... found!
#####
# NOTE TO FUTURE SELF, do NOT use single quotes in a Jenkins Build Execute Shell's informative echoes, such as the following:
#   echo "Jenkins Build Step, `Execute shell` about to call `${WORKSPACE}/general_infra/deploy_etl.sh`"
# WARNING: The single-quoted deploy script reference above ^^^
# was inadvertently executed, causing a doubled up run.
# The below fixes these, and merely echoes:
#   echo "Jenkins Build Step, `Execute shell` about to call ${WORKSPACE}/general_infra/deploy_etl.sh"
#####
#echo "r3m0 DEBUG: Dunno why you got called yet, because the Jenkins Build Execute Shell has y'all kindsa commented out"
#echo "r3m0 DEBUG: BAILING NOW, That's All Folx!"
#exit -1
#####

echo "Jenkins-supplied job_description = \"${job_description}\""
echo "Jenkins-supplied image_tag = \"${image_tag}\""
echo "Jenkins-supplied ENVIRONMENT = \"${ENVIRONMENT}\""
echo "Jenkins-supplied GCP_CREDS_VAULT_PATH = \"${GCP_CREDS_VAULT_PATH}\""
echo "Jenkins-supplied deployment_node = \"${deployment_node}\""
echo "Jenkins-supplied safe_input_manifest_name_csv = \"${safe_input_manifest_name_csv}\""
echo "Jenkins-supplied XTRA_DOCKER_RUN_FLAGS = ${XTRA_DOCKER_RUN_FLAGS}"

# Append the optional CONTAINER_NAME_SUFFIX to the CONTAINER_NAME, in case deploying concurrently,
# and utilize the empty valued default (with the :=) just in case not actually defined:
# (but first, replace any `/` with `_` for the docker container names)
SAFE_ENVIRONMENT=`echo ${ENVIRONMENT} | sed 's/\//_/'`
echo "DEBUG: creating a SAFE_ENVIRONMENT=${SAFE_ENVIRONMENT} from the raw ENVIRONMENT=${ENVIRONMENT}"
export CONTAINER_NAME=${VAULT_EIG_LOCATION}_${SAFE_ENVIRONMENT}${CONCURRENT_CONTAINER_NAME_SUFFIX:=}
echo "DEBUG: Using CONTAINER_NAME = ${CONTAINER_NAME}"

# Setup VAULT_TOKEN & VAULT_FULL_PATH_ETL:
echo "sourcing ${WORKSPACE}/general_infra/deploy_setup_vars.sh ..."
echo "================================================================="
source ${WORKSPACE}/general_infra/deploy_setup_vars.sh
echo "================================================================="

if [ ${CONCURRENT_CONTAINER_NAME_SUFFIX:="UNDEF"} == "UNDEF" ] || [ ${CONCURRENT_TRY_DOCKER_RMI:="UNDEF"} == "true" ] ; then
  # NOTE: CONCURRENT_CONTAINER_NAME_SUFFIX is currently only used for concurrent jobs,
  # and if a Jenkins job is configured to run concurrently, the following Docker
  # image removal can pose problems such as "unable to remove repository reference"
  # so ONLY attempt such last_local image cleanup if CONCURRENT_CONTAINER_NAME_SUFFIX is NOT defined,
  # OR if an explicit CONCURRENT_TRY_DOCKER_RMI override included, as might be set on the
  # first of a new concurrent batch after a newly built image becomes available.
  echo "WARNING: CONCURRENT_CONTAINER_NAME_SUFFIX requires update to this deploy_etl script's HARBOR_REPO stufffffff."
  ####
else
   echo "WARNING: Not removing last_local docker image since CONCURRENT_CONTAINER_NAME_SUFFIX indicates possible concurrency, and CONCURRENT_TRY_DOCKER_RMI not true."
fi

# Create config file:
./vault read -field=value ${VAULT_FULL_PATH_ETL} > ${WORKSPACE}/config.yaml

# If GCP_CREDS_VAULT_PATH defined, setup GOOGLE_APPLICATION_CREDENTIALS
# Using the already defined VAULT_EIG_LOCATION just append GCP_CREDS_VAULT_PATH:
if [ ${GCP_CREDS_VAULT_PATH:="UNDEF"} == "UNDEF" ] ; then
    echo "No GCP_CREDS_VAULT_PATH defined, continuing without."
    # NOTE: but still at least touch such a file for mounting into the Docker container:
    touch ${WORKSPACE}/google_service_account.json
else
    # PRE 2024:
    # ###############
    # # NOTE: No longer creating GOOGLE_APPLICATION_CREDENTIALS from the VAULT_FULL_PATH_ETL (which includes with environment!),
    # # Instead, allow the environment to be optionally included, but do not require it (in fact, allow a single default Locutus-level GCP_CREDS_VAULT_PATH).
    # echo "Creating GOOGLE_APPLICATION_CREDENTIALS from the Locutus root VAULT_FULL_PATH_SANS_ENVIRONMENT and GCP_CREDS_VAULT_PATH..."
    # export GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH=${VAULT_FULL_PATH_SANS_ENVIRONMENT}/${GCP_CREDS_VAULT_PATH}
    # ./vault read -field=value ${GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH} > ${WORKSPACE}/google_service_account.json
    # # NOTE: create the environment variable as under /opt/app, its mount point in the container
    # export GOOGLE_APPLICATION_CREDENTIALS=/opt/app/google_service_account.json
    # ###############
    #
    # 2024 update: full path to a dedicated spot
    echo "2024: Creating GOOGLE_APPLICATION_CREDENTIALS directly from GCP_CREDS_VAULT_PATH..."
    export GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH=${GCP_CREDS_VAULT_PATH}
    ./vault read -field=value ${GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH} > ${WORKSPACE}/google_service_account.json
    # NOTE: create the environment variable as under /opt/app, its mount point in the container
    export GOOGLE_APPLICATION_CREDENTIALS=/opt/app/google_service_account.json

fi


##############################
# For those jobs launched with ${XTRA_DOCKER_RUN_FLAGS} including -d for detached daemon,
# force an attempted stop of the container.
#
# First, just print out a debugging message if already running:
#
####################
# DEBUG:
echo "r3m0 DEBUG: In initially checking for CONTAINER_NAME, here is the full non-quiet docker docker ps on this node:"
docker ps
echo "r3m0 DEBUG: about to filter the above ^^^ docker ps for CONTAINER_NAME with \"name=${CONTAINER_NAME}\"..."
####################
docker ps -q --filter "name=${CONTAINER_NAME}" | grep -q . && echo "Container ${CONTAINER_NAME} already running, will first stop it..." || echo "Container ${CONTAINER_NAME} not previously running, can just start it up"

# NOTE: if the container wasn't launched with a --rm, may want to also include the rm here:
# docker ps -q --filter "name=${CONTAINER_NAME}" | grep -q . && docker stop ${CONTAINER_NAME} && docker rm -fv ${CONTAINER_NAME}
# BUT, these ARE launched with --rm, so can just do the stop:
#
####################
# DEBUG:
echo "r3m0 DEBUG: In AGAIN checking for CONTAINER_NAME for a DOCKER STOP, here is the full non-quiet docker docker ps on this node:"
docker ps
echo "r3m0 DEBUG: about to AGAIN filter the above ^^^ docker ps for CONTAINER_NAME with \"name=${CONTAINER_NAME}\" for a DOCKER STOP..."
####################
docker ps -q --filter "name=${CONTAINER_NAME}" | grep -q . && docker stop ${CONTAINER_NAME}

# and just in case the above is stopping, wait a moment (or five) for it to terminate:
sleep 5


echo "NOT YET, wait for it, wait for it...NOT  about to docker rmi ${HARBOR_REPO_NAME}:${image_tag} to help bring the latest Harbor-built image..."
#docker rmi ${HARBOR_REPO_NAME}:${image_tag}
#but in case the image is NOT on this node:
#docker images|sed "1 d"|grep ${HARBOR_REPO_NAME}\:${image_tag} | awk '{print $3}'|sort|uniq|xargs docker rmi

#echo "================================================================="
#echo "about to docker pull ${HARBOR_REPO}:${image_tag} prior to docker run, to more consistently get the latest"
# NOTE: not yet doing the docker pull....
# FIRST, trying the addition of ==pull=always to the below docker run:

echo "================================================================="
echo "about to docker run --name ${CONTAINER_NAME} with --pull=always [...]"
# NOTE: https://stackoverflow.com/questions/56286929/docker-pull-wouldnt-pull-latest-image-from-remote#comment134648904_56287421
echo "================================================================="
##############################
# Run Docker-based ETL (with forced-download of the image_tag from (previously EC2, now:) Harbor):
docker run --pull=always --name ${CONTAINER_NAME} ${XTRA_DOCKER_RUN_FLAGS}\
    --rm -v ${WORKSPACE}/config.yaml:/opt/app/config.yaml\
    -v ${WORKSPACE}/google_service_account.json:/opt/app/google_service_account.json\
    -e CURR_ENV=${ENVIRONMENT}\
    -e VAULT_ADDR=${VAULT_ADDR}\
    -e VAULT_TOKEN=${VAULT_TOKEN}\
    -e VAULT_NAMESPACE=${VAULT_NAMESPACE}\
    -e DOCKERHOST_HOSTNAME=${HOSTNAME}\
    -e DOCKERHOST_CONTAINER_NAME=${CONTAINER_NAME}\
    -e DOCKERHOST_IMAGE_TAG=${image_tag}\
    -e GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}\
    -e GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH=${GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH}\
    -e JOB_NAME=${JOB_NAME}\
    -e BUILD_NUMBER=${BUILD_NUMBER}\
    -e JOB_DESCRIPTION="${job_description}"\
    -e INPUT_MANIFEST_NAME="${safe_input_manifest_name_csv}"\
    ${HARBOR_REPO}:${image_tag} /opt/app/run_main.sh
echo "================================================================="

# CLEANUP:
# WARNING: the last_local images might not yet exist at first run; just leave them around for reference.

echo "deploy_etl: Ending at: `date`"
echo "Goodbye from the generalized Jenkins deploy_etl script!"
