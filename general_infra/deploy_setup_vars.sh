#!/bin/bash
# deploy_setup_vars.sh: generalized shell command to help Jenkins deploy jobs,
# incorporating new elements for the July/August 2020 switch to RIS's vault server and corresponding minor restructuring
#
# namely by building up the following environment variables:
# * HARBOR_URL
# * VAULT_TOKEN (no longer from the expected tok.txt artifact to be imported, but now retrieved via generate_post_data())
# * VAULT_ADDR as well, rather than hardcoding as below
# * VAULT_NAMESPACE for the RIS Vault server
# * VAULT_FULL_PATH_ETL & VAULT_FULL_PATH_HARVEST
#       from VAULT_EIG_LOCATION & ENVIRONMENT
#
# EXPECTS the following environmental variables to be injected:
# HARBOR_REPO_NAME to append to our general ECR REPO dir
# AND: VAULT_EIG_LOCATION & ENVIRONMENT for: secret/dbhi/eig/${VAULT_EIG_LOCATION}/${ENVIRONMENT}
#
# To be used with by:
#   general_infra/deploy_etl.sh
# And any other custom deployments which might not align with the above two.
#
# NOTE: new approach to setting up the Vault token requires that jq be installed.

generate_post_data()
{
    cat <<EOF
{
"role_id":"$TRIG_VAULT_ROLE_ID",
"secret_id":"$TRIG_VAULT_SECRET_ID"
}
EOF
}

echo "Hello from the generalized Jenkins deploy_setup_vars helper script."
echo "deploy_setup_vars: Starting at: `date`"

if [ ${VAULT_EIG_LOCATION:="UNDEF"} == "UNDEF" ] || [ ${ENVIRONMENT:="UNDEF"} == "UNDEF" ] ; then
    echo "WARNING: expected environment variables of VAULT_EIG_LOCATION and/or ENVIRONMENT are missing; continuing in case this is just a build job..."
fi

# Save the rain forests!
# NOTE: the {image_tag} (e.g., ":latest", etc) shall be supplied to the calling deploy_etl.sh script via Jenkins, etc.
export HARBOR_REPO="${HARBOR_URL}/${HARBOR_REPO_NAME}"

# NOTE: override any VAULT_ADDR in the Jenkins parameters
# WAS: export VAULT_ADDR="https://risvault.research.chop.edu"
echo "deploy_setup_vars.sh: Using VAULT_ADDR=\"${VAULT_ADDR}\", and VAULT_NAMESPACE=\"${VAULT_NAMESPACE}\"..."
echo "deploy_setup_vars.sh: Looking for the TRIG_VAULT vars or then tok.txt for VAULT_ADDR=\"${VAULT_ADDR}\"..."

echo "Welcome to 2024+, inheriting VAULT_ADDRESS & VAULT_TOKEN from Jenkins"
######
# DEPRECATED in the RESURRECTION? (since VAULT_ADDRESS & VAULT_TOKEN inherited now via Jenkins?)
## NOTE: the previously imported tok.txt artifact from Jenkins job vault-token-refresh has been deprecated.
## Support its existence in the transition to a new AppRole as follows:
#if [ ${TRIG_VAULT_ROLE_ID:="UNDEF"} == "UNDEF" ] || [ ${TRIG_VAULT_SECRET_ID:="UNDEF"} == "UNDEF" ] ; then
#    echo "WARNING: no TRIG_VAULT_ROLE_ID or TRIG_VAULT_SECRET_ID supplied (expected both); please update to the latest TrigApps AppRole"
#    # NOTE: deprecated tok.txt becomes the fall-back, even if not valid (delete from any such workspaces)
#    echo "Falling back to look for (now deprecated) tok.txt...."
#    export DEPRECATED_TOKEN_ARTIFACT="${WORKSPACE}/tok.txt"
#    if [ -f ${DEPRECATED_TOKEN_ARTIFACT} ]; then
#        echo "Deprecated Vault token file ${DEPRECATED_TOKEN_ARTIFACT} still exists, trying to use it as VAULT_TOKEN"
#        # Use the artifact token from Jenkins job vault-token-refresh:
#        export VAULT_TOKEN=`cat ${DEPRECATED_TOKEN_ARTIFACT}`
#        echo "WARNING: just because tok.txt has been found and loaded does NOT mean that it is still valid (delete any old ones from workspaces)..."
#    else
#        echo "WARNING: Deprecated Vault token file $DEPRECATED_TOKEN_ARTIFACT does NOT exist and nothing to fall back on; continuing in case this is just a build job..."
#    fi
#else
#    echo "Using the supplied TRIG_VAULT_ROLE_ID and TRIG_VAULT_SECRET_ID to retrieve a RIS-Vault token"
#    export VAULT_TOKEN=$(curl --request POST --data "$(generate_post_data)" ${VAULT_ADDR}/v1/trig/auth/approle/login | jq -r .auth.client_token)
#fi

# Setup Vault to Grab Configuration
if [ ! -f vault_0.6.5_linux_amd64.zip ]; then
    wget https://releases.hashicorp.com/vault/0.6.5/vault_0.6.5_linux_amd64.zip
	unzip vault_0.6.5_linux_amd64.zip
fi

# Calculate the possible VAULT_FULL_PATH_* for both ETL and HARVEST (with /app):
# TODO: consider eventually moving all HARVEST /app secrets to same as ETL.
# Until then, here is where they are each currently configured within Vault:
# NOTE: the ${VAULT_NAMESPACE} might eventually not be needed, but for now, must explicitly include:
export VAULT_FULL_PATH_ETL="${VAULT_NAMESPACE}/kv1/etl/${VAULT_EIG_LOCATION}/${ENVIRONMENT}"
export VAULT_FULL_PATH_HARVEST="${VAULT_NAMESPACE}/kv1/app/${VAULT_EIG_LOCATION}/${ENVIRONMENT}/app"
# and one more for use with downstream modules (e.g., deploy_etl.sh) to use as needed (e.g., to create GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH)
export VAULT_FULL_PATH_SANS_ENVIRONMENT="${VAULT_NAMESPACE}/kv1/etl/${VAULT_EIG_LOCATION}"

echo "deploy_setup_vars: Ending at: `date`"
echo "Goodbye from the generalized Jenkins deploy_setup_vars helper script."
