#!/usr/bin/env python3
# main_locutus.py:
# Perform Locutus processing on all applicable upstream modules, e.g.:
# 	* OnPrem  Dicom Stage
#	* OnPrem Aperio Stage
#	* etc.
#
# Development NOTES (some from trig-dicom-stage which still apply here):
#
# * python3 required due to issues with python2 and httplib2
#
# * known issues with httplib2 and request w/ local MacOSX modules cause
#   occasional intermittent hangs, and must be run in a docker container.
#   Sources:
#       https://github.com/requests/requests/issues/171
#           "socket.error: [Errno 54] Connection reset by peer"
#       https://github.com/requests/requests/issues/3883
#           "This code generates errors only on Mac OSX"
#
# * python3's flush=True ensures prints are flushed from docker container.
#
# * upstream modules such as orthanc-dicom-stage may be configured
#	to poll and stage every 30 seconds or so,
# 	but Locutus could be setup to run on different intervals as needed
#	(e.g., nightly, hourly, or whatever) by way of its Jenkins job.
#
# * configuration for upstream modules may be referenced here as part of Locutus
#	such that their configurations aren't repeated in multiple places.
#	For example, since the ONPREM Orthanc DICOM server will still need to
#	be accessed to retrieve the actal DICOM images referenced in the
#	orthanc-dicom-stage staged database, a reference to the same config creds
#	as used for orthanc-dicom-stage can be reused here to access Orthanc.
#   NOTE that the ${ENVIRONMENT} could be neeed to access the particular cfg,
#   but these are now just hardcoded into the particular sub-module config path.
#
#   For example:
# TODO: rename Vault paths from trig_dicom_stage_config_vault_path to: orthanc_dicom_stage_config_vault_path
# TODO == orthanc_dicom_stage_config_vault_path: secret/dbhi/eig/orthanc-dicom-staging/develop
#   Locutus's Develop config might define orthanc_dicom_stage_config_vault_path
#   Orthanc Dicom's sub-module vault path to its config, with the develop env, as:
#   orthanc_dicom_stage_config_vault_path: secret/dbhi/eig/orthanc-dicom-staging/develop
#
# * Continuous-polling daemon possible with RUN_MODE='continuous' configuration,
# to allow the option for a Jenkins job to launch it but once (like Harvest).
# Alternately, can run more like a one-off ETL with RUN_MODE='single', but with
# a periodic Jenkins build-trigger of every 5 minutes or so.

# for target GCP GS, but must appear at top:
from __future__ import absolute_import

import argparse
import httplib2
from trigsecrets import TrigSecrets
import hvac
import os
from sqlalchemy import create_engine
import sys
import time
import traceback
import yaml
# for target AWS s3:
# r3m0: NOTE: disabling AWS/boto until needed again, pending resolution of:
#   https://github.research.chop.edu/dbhi/Locutus/issues/412
#   "upgrade aws cli to v2 (from pip to curl&unzip install)"
#import boto3
#from botocore.client import Config as Boto3Config

# for target GCP GS:
from google.cloud import storage as GS_storage


# NOTE: only one set of high-level general Locutus settings
from src_modules.settings import Settings

# NOTE: each Locutus module's settings taken care of in their own Setup():
from src_modules.module_gcp_dicom import GCPDicom
from src_modules.module_onprem_dicom import OnPrem_Dicom
from src_modules.module_onprem_aperio import OnPrem_Aperio
###########################################################################
# NOTE: teach Locutus command's settings (supporting the above modules, but not full processing modules in and of themselves)
# NOTE: each Locutus cmd's settings taken care of in their own Setup():
from src_modules.cmd_dicom_summarize_status import DICOMSummarizeStats
from src_modules.cmd_dicom_stage_compare import DICOMStageCompare
from src_modules.cmd_dicom_QC_for_BGDlab_HMsubjects_age_accession_increases import DICOM_QCforBGDlabWorkspace
#####
# DEPRECATED DICOMSplitAccession 22 August 2024 (see DEPRECATED_cmd_dicom_split_accessio.py header for further details)
#from src_modules.cmd_dicom_split_accession import DICOMSplitAccession
#####

# and for the general settings in Locutus's settings.py:
import src_modules.settings

# for set/get_Locutus_system_status() to allow even this main module its own status
# (though effectively equivalent to the overall Locutus status):
SYS_STAT_MODULENAME = 'main_Locutus'

# CFG_OUT:
# NOTE: support quick stdout log crumb to easily create a CFG output CSV via: grep CFG_OUT
# Listing here the most recent header values from src_modules.settings.py (& not yet in its Settings class)
# For here, though, only the CFG_OUT_PREFIX is actually needed, since the headers are starting in Settings:
####################################################
# src_modules.settings.CFG_OUT_PREFIX == "CFG_OUT:"
# src_modules.settings.CFG_OUT_HEADER_KEY_TYPE == 'KEY_TYPE'  # e.g., cfg, env, or hardcoded, etc.
# src_modules.settings.CFG_OUT_HEADER_KEY == 'KEY'
# src_modules.settings.CFG_OUT_HEADER_VAL = 'VALUE'

##
## Main loop that sets up and processes all things Locutus.
##
def main(args):

    print("Welcome to Locutus!", flush=True)

    ###################################
    # setup Vault and the Locutus database credentials:
    #
    # NOTE: currently the following Vault and Locutus DB info is ONLY needed
    # if actually processing DICOM images, or processing Aperio slides.
    # Since used by either of these, modules, keeping here rather than
    # under the subsequent SetupAll section,
    # and in case other modules such as the various reports (e.g., previous MRI, Op, and Path reports)
    # end up w/ their own corresponding MANIFEST_STATUS tables in this
    # Locutus DB space.
    if not (Settings.PROCESS_GCP_DICOM_IMAGES or Settings.PROCESS_ONPREM_DICOM_IMAGES) \
        and not (Settings.PROCESS_ONPREM_APERIO_SLIDES or Settings.PROCESS_ONPREM_APERIO_SLIDES_MANIFEST_SKIM) \
        and not (Settings.PROCESS_DICOM_STAGE_COMPARE or Settings.PROCESS_DICOM_SPLIT_ACCESSION or Settings.PROCESS_DICOM_SUMMARIZE_STATS) \
        and not Settings.PROCESS_DICOM_QC_FOR_BGDLAB_WORKSPACE \
        and not Settings.PROCESS_LOCUTUS_SYSTEM_STATUS:
        if Settings.LOCUTUS_VERBOSE:
            print('Locutus is not to PROCESS_[GCP/ONPREM]_DICOM_IMAGES nor to PROCESS_ONPREM_APERIO_SLIDES[_MANIFEST_SKIM] (nor SUMMARIZE, QC4BGD, or PROCESS_LOCUTUS_SYSTEM_STATUS), and is therefore not connecting to Vault or configuring DB...', flush=True)
    else:

        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},locutus-env,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX, "VAULT_ADDR", os.environ.get('VAULT_ADDR')), flush=True)
        print('{0},locutus-env,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX, "VAULT_NAMESPACE", os.environ.get('VAULT_NAMESPACE')), flush=True)

        if Settings.LOCUTUS_VERBOSE:
            print('Loading TrigSecrets with Vault server: {0}'.format(os.environ['VAULT_ADDR']), flush=True)
            # WAS: print('Using Vault server: {0}'.format(os.environ['VAULT_ADDR']), flush=True)
        # WAS: hvac_client = hvac.Client(
        #    url=os.environ['VAULT_ADDR'],
        #    token=os.environ['VAULT_TOKEN'])
        # Replacing internally generated hvac_client, as above, with that from TrigSecrets, below:
        # NOTE: TrigSecrets' expected environmental variables are VAULT_TOKEN and VAULT_ADDR, including explicitly:
        trig_secrets = TrigSecrets(vault_token=os.environ['VAULT_TOKEN'],
                                    vault_address=os.environ['VAULT_ADDR'],
                                    vault_namespace=os.environ['VAULT_NAMESPACE'])
        hvac_client = trig_secrets.get_hvac_client()

        ###################################
        # Locutus Database credentials for TRiG Data Warehouse via Vault:
        locutus_target_db_host = hvac_client.read(
                    Settings.LOCUTUS_DB_VAULT_PATH+'/db_host')['data']['value']
        # TODO: eventually replace the existing Vault db_host keys from FQDN into simply "production" & "dev",
        # but until then....
        # NOTE: convert the above FQDN db_host into a format that TrigSecrets can understand, namely:
        trig_secrets_dw_host = 'UNKNOWN'
        if locutus_target_db_host == 'eigdw.research.chop.edu':
            trig_secrets_dw_host = 'production'
        elif locutus_target_db_host == 'eigdwdev.research.chop.edu':
            trig_secrets_dw_host = 'dev'
        else:
            # TODO: consider eventually switching the Vault-based db_hosts directly into production & dev, but for now:
            print('Locutus: ERROR: LOCUTUS_DB_VAULT_PATH of {0} contains an unknown db_host ({1}) and currently expecting only [eigdw|eigdwdev].research.chop.edu ; exiting.'.format(
                            Settings.LOCUTUS_DB_VAULT_PATH, locutus_target_db_name), flush=True)
            # TODO: consider an alternate exception, etc:
            exit(-1)
        """
        # NOTE: TrigSecrets now takes care of the following:
        locutus_target_db_user = hvac_client.read(
                    Settings.LOCUTUS_DB_VAULT_PATH+'/user')['data']['value']
        locutus_target_db_pw = hvac_client.read(
                    Settings.LOCUTUS_DB_VAULT_PATH+'/password')['data']['value']
        """
        # But do still load the actual DB name for logging:
        locutus_target_db_name = hvac_client.read(
                    Settings.LOCUTUS_DB_VAULT_PATH+'/db_name')['data']['value']

        # and, if so defined, also append the optional dev_suffix to the db_name:
        # Support EITHER boolean (unquoted) OR string (quoted) representations of True for *_use_dev_suffix:
        db_use_dev_suffix_raw = Settings.LOCUTUS_DB_USE_DEV_SUFFIX
        db_use_dev_suffix = False
        if db_use_dev_suffix_raw and str(db_use_dev_suffix_raw).upper() == "TRUE":
            db_use_dev_suffix = True
            # WARNING: assumes that the TRiGSecrets implementation of 'DB_use_dev_suffix'
            # is the same as our (now purely optional) 'DB_dev_suffix',
            # which is typically a `_dev` suffix:
            locutus_target_db_name += Settings.LOCUTUS_DB_DEV_SUFFIX
        # finally generate the entire target DB connection string:
        """
        # NOTE: TrigSecrets now takes care of the following:
        locutus_target_db = 'postgresql://{0}:{1}@{2}:5432/{3}'.format(
                                    locutus_target_db_user,
                                    locutus_target_db_pw,
                                    locutus_target_db_host,
                                    locutus_target_db_name)
        """
        # NOTE: Now using TrigSecrets to generate the DB connection string:
        # TODO: eventually replace the existing config VAULT_PATH keys to no longer have the 'secret/dbhi/eig/databases' prefix:
        # but for now, merely check and remove up to the last '/', if included:
        trig_secrets_db_vault_path =  Settings.LOCUTUS_DB_VAULT_PATH
        if trig_secrets_db_vault_path.find('/') > -1:
            trig_secrets_db_vault_path = trig_secrets_db_vault_path[trig_secrets_db_vault_path.rfind('/')+1:]
        if Settings.LOCUTUS_VERBOSE:
            print("DEBUG: About to generate DB credentials from TrigSecrets using vault_db_name={0}, dw_host={1}, use_dev_suffix={2}".format(
                                        trig_secrets_db_vault_path,
                                        trig_secrets_dw_host,
                                        db_use_dev_suffix),
                                        flush=True)
        locutus_target_db = trig_secrets.get_db_credentials(vault_db_name=trig_secrets_db_vault_path,
                                                            dw_host=trig_secrets_dw_host,
                                                            dev=db_use_dev_suffix)
        # to facilitate other cmds/modules in also calling get_db_credentials():
        Settings.trig_secrets = trig_secrets
        Settings.trig_secrets_dw_host = trig_secrets_dw_host
        #print('r3m0 DEBUG: Locutus just got TrigSecrets creds for locutus_target_db="{0}"'.format(locutus_target_db))
        #
        ###################################
        Settings.locutus_target_db_name = locutus_target_db_name
        Settings.locutus_target_db = locutus_target_db
        if Settings.LOCUTUS_VERBOSE:
            print("DEBUG: Set Locutus Settings.target_db_name = {0}!".format(
                            Settings.locutus_target_db_name),
                            flush=True)
        print("Locutus: Using Locutus target_db_name = {0}!".format(
                            locutus_target_db_name), flush=True)

        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},locutus-via-vault,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX, "locutus_target_db_host", locutus_target_db_host), flush=True)
        print('{0},locutus-via-vault,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX, "locutus_target_db_name", locutus_target_db_name), flush=True)

        ######################################################################
        # Locutus DB..... System Status section:
        #
        ###########
        # no DBconnSession within main_locutus.py, so get_Locutus_system_status() will generate one from the locutus_target_db:
        no_sysDBconnSession = None
        # TODO: consider creating and adding a call to a new Settings.create_Locutus_system_status_table(Settings.LOCUTUS_SYS_STATUS_TABLE, no_sysDBconnSession, locutus_target_db)
        # that itself shall do a CREATE TABLE if not exists

        ###########
        # first, for CFG_OUT, check the Locutus overall status:
        no_sysDBconnSession = None
        check_Docker_node=False
        alt_node_name=None
        check_module=False
        module_name=SYS_STAT_MODULENAME
        (curr_sys_stat_overall, sys_msg_overall, sys_node_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
        print('{0},{1},overall,{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_overall, sys_msg_overall), flush=True)

        # next, also check the node-level status that we'll ultimately use
        # (even if the above overall active status is False, for completeness):
        check_Docker_node=True
        # setting alt_node_name heer to DOCKERHOST is equivalent to leaving it None here, but go ahead and explicitly set it:
        alt_node_name = os.environ.get('DOCKERHOST_HOSTNAME')
        (curr_sys_stat_node, sys_msg_node, sys_node_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
        print('{0},{1},node={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_node, sys_msg_node), flush=True)

        # NOTE: no need to check module-specific yet, since this is overall main_locutus, effectively same as overall,
        # but doing so anyway:
        # and then, also check the module-specific status that we'll also ultimately use
        # (still checking even if either of the above overall & node-level active status are False, for completeness):
        check_Docker_node=False
        check_module=True
        (curr_sys_stat_module, sys_msg_module, sys_module_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
        print('{0},{1},module={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_module_name, curr_sys_stat_module, sys_msg_module), flush=True)

        # NOTE: the above module-specific checks are here in main_Locutus BEFORE calling any applicable module that is configured for processing,
        # as well as (at least, eventually) WITHIN each applicable module (esp those with particularly long run times such as *_DICOM and *_Aperio)

        # NOTE: to HALT the HALT if.... and only if.... this run of Locutus is purely for Settings.PROCESS_LOCUTUS_SYSTEM_STATUS
        ############################################### ###############################################
        if Settings.PROCESS_LOCUTUS_SYSTEM_STATUS:
            # NOTE: for this PROCESS_LOCUTUS_SYSTEM_STATUS CFG_OUT, build up the overall -vs- node=NODENAME for the node_field:
            sys_stat_cmd_msg = "SET" if Settings.LOCUTUS_SET_SYSTEM_STATUS else "GET"
            node_field_msg = "node={0}".format(Settings.LOCUTUS_USE_SYSTEM_STATUS_NODE_NAME) if Settings.LOCUTUS_USE_SYSTEM_STATUS_NODE  else "overall"
            module_field_msg = "module={0}".format(Settings.LOCUTUS_USE_SYSTEM_STATUS_MODULE_NAME) if Settings.LOCUTUS_USE_SYSTEM_STATUS_MODULE  else "overall"
            node_set_msg = " to {0}".format(Settings.LOCUTUS_SET_SYSTEM_STATUS_TO_VALUE) if Settings.LOCUTUS_SET_SYSTEM_STATUS else ""
            combined_field_msg = module_field_msg if Settings.LOCUTUS_USE_SYSTEM_STATUS_MODULE else  node_field_msg if Settings.LOCUTUS_USE_SYSTEM_STATUS_NODE else "overall"
            print('====> PROCESS_LOCUTUS_SYSTEM_STATUS about to {0} {1} / {2}..... {3}:'.format(sys_stat_cmd_msg, node_field_msg, module_field_msg, node_set_msg), flush=True)
            if Settings.LOCUTUS_SET_SYSTEM_STATUS:

                # TODO: enhance this SET SYS STATUS functionality to include the specified module

                #print('PROCESS_LOCUTUS_SYSTEM_STATUS about to SET {0} to {1}...'.format(node_field_out, Settings.LOCUTUS_SET_SYSTEM_STATUS_TO_VALUE))
                # Regardless of the values for the above curr_sys_stat_overall & curr_sys_stat_node,
                # if this is to set the system status, be sure to reevaluate curr_sys_stat_node afterwards (extra safety check in case the set did not fully)
                # although the returned curr_sys_stat_node, though not fully decoupled with a subsequent get_Locutus_system_status(), should suffice:
                (curr_sys_stat_node_set, sys_msg_node, sys_node_name) = Settings.set_Locutus_system_status(Settings, Settings.LOCUTUS_SET_SYSTEM_STATUS_TO_VALUE,
                                                                                            Settings.LOCUTUS_USE_SYSTEM_STATUS_NODE, Settings.LOCUTUS_USE_SYSTEM_STATUS_NODE_NAME,
                                                                                            Settings.LOCUTUS_USE_SYSTEM_STATUS_MODULE, Settings.LOCUTUS_USE_SYSTEM_STATUS_MODULE_NAME,
                                                                                            Settings.LOCUTUS_SYSTEM_STATUS_ENABLE_DB_UPDATES, no_sysDBconnSession, locutus_target_db)
                #print('r3m0 DEBUG: straight outta set_Locutus_system_status() stub; got curr_sys_stat_node={0}, sys_msg_node={1}'.format(
                #        curr_sys_stat_node_set, sys_msg_node), flush=True)
                print('{0},{1},{2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "SET_LOCUTUS_SYS_STATUS", node_field_msg, curr_sys_stat_node_set, sys_msg_node), flush=True)

            # either way a GET, either following the above SET, or as a stand-alone GET:
            check_Docker_node=False
            check_module=False
            if Settings.LOCUTUS_USE_SYSTEM_STATUS_NODE:
                check_Docker_node=True
            if Settings.LOCUTUS_USE_SYSTEM_STATUS_MODULE:
                check_module=True
            # for GET_SYSTEM_STATUS, if a node was specified (whether or not different than the default node), do one more get:
            (curr_sys_stat_nodemodule, sys_msg_nodemodule, sys_nodemodule_name) = Settings.get_Locutus_system_status(Settings, \
                                                                    check_Docker_node, Settings.LOCUTUS_USE_SYSTEM_STATUS_NODE_NAME, \
                                                                    check_module, Settings.LOCUTUS_USE_SYSTEM_STATUS_MODULE_NAME, \
                                                                    no_sysDBconnSession, locutus_target_db)
            print('{0},{1},{2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "GET_LOCUTUS_SYS_STATUS", combined_field_msg, curr_sys_stat_nodemodule, sys_msg_nodemodule), flush=True)

                # TODO: enhance this SET SYS STATUS functionality to include a GET of the module, if so specified.

            print('PROCESS_LOCUTUS_SYSTEM_STATUS complete, see ^^^^^^^^^^ ^^^^^^^^^^ ^^^^^^^^^^')
            exit(0)
        ############################################### ###############################################

        # WAS: only need to evaluate curr_sys_stat_node, since it takes into account curr_sys_stat_overall during its calculation
        # NOTE: but now, looking at node and module, they are each independently queried via get_Locutus_system_status(),
        # so go ahead and explicitly check for full activity across all three:
        if curr_sys_stat_overall and curr_sys_stat_node and curr_sys_stat_module:
            print('INFO: main_locutus sees Locutus system is active overall and for this node & module.... carry on!')
        else:
            # NOTE: could set run_loop = False, but..... let's just bail here:
            print('FATAL ERROR: main_locutus sees Locutus system is NOT currently active overall, or for this node &/or module.... halting.')
            exit(-1)
            #####
            #print('r3m0 DEBUG: NOT YET BAILING, see if the GCP module also catches this???? YES, it does, YAY!!!')
        ###########

    ######################################################################
    # Locutus.SetupAll() section:

    if ( (Settings.PROCESS_GCP_DICOM_IMAGES or Settings.PROCESS_ONPREM_DICOM_IMAGES) \
        and not (Settings.LOCUTUS_TARGET_USE_S3 \
                or Settings.LOCUTUS_TARGET_USE_GS \
                or Settings.LOCUTUS_TARGET_USE_ISILON) ):
            # NOTE: this is OK if running the Summarizer, but not for any DeID modules
            print('Locutus: ERROR: A LOCUTUS_TARGET_USE_* must be set for either DeID module, PROCESS_GCP_DICOM_IMAGES or PROCESS_ONPREM_DICOM_IMAGES; exiting.', flush=True)
            # TODO: consider an alternate exception, etc:
            exit(-1)

    # Setup s3 connection resource just once here, for any & all sub-modules:
    # r3m0 NOTE: commenting out AWS s3 references until new approach to loading the CLI and boto* into the env:
    #if Settings.LOCUTUS_TARGET_USE_S3:
    #    if Settings.LOCUTUS_VERBOSE:
    #        print('Locutus: Setting up AWS s3 boto3 client & resource object...', flush=True)
    #    Settings.locutus_target_s3client = boto3.client('s3', config=Boto3Config(signature_version='s3v4'))
    #    Settings.locutus_target_s3resource = boto3.resource('s3', config=Boto3Config(signature_version='s3v4'))
    if Settings.LOCUTUS_TARGET_USE_S3:
        print('Locutus: WARNING: LOCUTUS_TARGET_USE_S3 is set, but AWS s3 boto3 client & resource object are currently unused until new CLI load implemented...', flush=True)

    # Setup GS connection resource just once here, for any & all sub-modules:
    if Settings.LOCUTUS_TARGET_USE_GS:
        if Settings.LOCUTUS_VERBOSE:
            print('Locutus: Setting up GCP GS client object...', flush=True)
        Settings.locutus_target_GSclient = GS_storage.Client()
        # and setup access to our desired bucket, just once here in the setup:
        Settings.locutus_target_GSbucket = None
        caught_exception = None
        try:
            if Settings.LOCUTUS_VERBOSE:
                print('Locutus: Setting up locutus_target_GSbucket.get_bucket(\'{0}\')...'.format(
                    Settings.LOCUTUS_TARGET_GS_BUCKET),
                    flush=True)
            Settings.locutus_target_GSbucket = Settings.locutus_target_GSclient.get_bucket(
                                    Settings.LOCUTUS_TARGET_GS_BUCKET)
        except Exception as locally_caught_exception:
            caught_exception = locally_caught_exception
            # Q: log it anyhow, just in case a problem?
            print('Locutus: ERROR: locutus_target_GSclient.get_bucket() '\
                    '(\'{0}\') seems to have '\
                    'thrown the following exception: \'{1}\'; exiting.'.format(
                    Settings.LOCUTUS_TARGET_GS_BUCKET,
                    caught_exception),
                    flush=True)
            # NOTE: NO NEED FOR: errors_encountered += 1
            # since this will be incremented in a below "if gs_err or caught_exception:"
            # TODO: consider re-throwing the caught exception, etc:

            # Add more GCP cred rotation clues to the caller if a GCP_INVALID_CREDS_ERR
            if Settings.GCP_INVALID_CREDS_ERR in str(caught_exception):
                print('Locutus: ERROR: GCP_INVALID_CREDS_ERR is makin some noize!', flush=True)
                print('Locutus: The time has come for you to retreive our rotated GCP creds from Vault, '\
                    'and update our dicom-alpha-current.json links for all such Locutus deployment nodes.', flush=True)
                print('Locutus thanks you kindly, and wishes you good fortune on your quest.', flush=True)
                #and TEST locally w/ an old set of creds

            # NOTE: to facilate temporary DEBUG test here with GCP_INVALID_CREDS_ERR by NOT bailing,
            # uncomment the next line, and comment out the exit(-1):
            #print('r3m0 DEBUG: bailing on the following main_locutus exit from FATAL Settings GCP_INVALID_CREDS_ERR, to see if we can encounter it later')
            exit(-1)

    if Settings.PROCESS_GCP_DICOM_IMAGES and not Settings.LOCUTUS_TARGET_USE_GS:
            print('Locutus: ERROR: LOCUTUS_TARGET_USE_GS must be set for PROCESS_GCP_DICOM_IMAGES; exiting.', flush=True)
            # TODO: consider an alternate exception, etc:
            exit(-1)

    if Settings.LOCUTUS_TARGET_USE_GS or Settings.PROCESS_GCP_DICOM_IMAGES:
        if not Settings.LOCUTUS_GOOGLE_APPLICATION_CREDENTIALS:
            print('Locutus: ERROR: GOOGLE_APPLICATION_CREDENTIALS must be set when using either PROCESS_GCP_DICOM_IMAGES or LOCUTUS_TARGET_USE_GS; exiting.', flush=True)
            # TODO: consider an alternate exception, etc:
            exit(-1)

    num_modules_and_commands_to_process = 0

    # Setup for Command: DICOM-STAGE-COMPARE:
    if not Settings.PROCESS_DICOM_STAGE_COMPARE:
        if Settings.LOCUTUS_VERBOSE:
            print("Locutus: NOT setting up DICOMStageCompare....", flush=True)
    else:
        num_modules_and_commands_to_process += 1
        print("Locutus: setting up DICOMStageCompare....", flush=True)
        dicom_stage_compare = DICOMStageCompare(Settings)
        dicom_stage_compare_config = dicom_stage_compare.Setup(trig_secrets, Settings.LOCUTUS_DICOM_STAGE_VAULT_PATH)

    # Setup for Command: DICOM-SPLIT-ACCESSION:
    # DEPRECATED DICOMSplitAccession 22 August 2024 (see DEPRECATED_cmd_dicom_split_accessio.py header for further details)
    #if not Settings.PROCESS_DICOM_SPLIT_ACCESSION:
    #    if Settings.LOCUTUS_VERBOSE:
    #        print("Locutus: NOT setting up DICOMSplitAccession....", flush=True)
    #else:
    #    num_modules_and_commands_to_process += 1
    #    print("Locutus: setting up DICOMStageCompare....", flush=True)
    #    dicom_split_accession = DICOMSplitAccession(Settings)
    #    dicom_split_accession_config = dicom_split_accession.Setup()

    # Setup for Command: DICOM-SUMMARIZE-STATS:
    if not Settings.PROCESS_DICOM_SUMMARIZE_STATS:
        if Settings.LOCUTUS_VERBOSE:
            print("Locutus: NOT setting up DICOMSummarizeStats....", flush=True)
    else:
        num_modules_and_commands_to_process += 1
        print("Locutus: setting up DICOMSummarizeStats....", flush=True)
        dicom_summarize_stats = DICOMSummarizeStats(Settings)
        dicom_summarize_stats_config = dicom_summarize_stats.Setup(trig_secrets, Settings.LOCUTUS_DICOM_SUMMARIZE_STATS_STAGE_VAULT_PATH)

    # Setup for Command: DICOM-QC-FOR-BGDLAB-WORKSPACE
    if not Settings.PROCESS_DICOM_QC_FOR_BGDLAB_WORKSPACE:
        if Settings.LOCUTUS_VERBOSE:
            print("Locutus: NOT setting up DICOMSummarizeStats....", flush=True)
    else:
        num_modules_and_commands_to_process += 1
        print("Locutus: setting up DICOM_QCforBGDlabWorkspace....", flush=True)
        dicom_QCforBGlab = DICOM_QCforBGDlabWorkspace(Settings)
        dicom_QCforBGlab_config = dicom_QCforBGlab.Setup(trig_secrets)

    # Setup for Module: GCP-DICOM:
    if not Settings.PROCESS_GCP_DICOM_IMAGES:
        if Settings.LOCUTUS_VERBOSE:
            print("Locutus: NOT setting up GCPDicom....", flush=True)
    else:
        num_modules_and_commands_to_process += 1
        print("Locutus: setting up GCPDicom....", flush=True)
        gcp_dicom = GCPDicom(Settings)
        gcp_dicom_config = gcp_dicom.Setup(trig_secrets, Settings.GCP_DICOM_STAGE_VAULT_PATH)

    # Setup for Module: OnPrem-DICOM:
    if not Settings.PROCESS_ONPREM_DICOM_IMAGES:
        if Settings.LOCUTUS_VERBOSE:
            print("Locutus: NOT setting up OnPrem_Dicom....", flush=True)
    else:
        num_modules_and_commands_to_process += 1
        print("Locutus: setting up OnPrem_Dicom....", flush=True)
        onprem_dicom = OnPrem_Dicom(Settings)
        (onprem_dicom_src_orthanc_config, onprem_dicom_qc_orthanc_config) = onprem_dicom.Setup(trig_secrets, Settings.ONPREM_DICOM_STAGE_VAULT_PATH)

    # Setup for Module: OnPrem-APERIO (either for normal processing or manifest skim processing):
    if not Settings.PROCESS_ONPREM_APERIO_SLIDES and not Settings.PROCESS_ONPREM_APERIO_SLIDES_MANIFEST_SKIM:
        if Settings.LOCUTUS_VERBOSE:
            print("Locutus: NOT setting up OnPrem_Aperio....", flush=True)
    else:
        num_modules_and_commands_to_process += 1
        print("Locutus: setting up OnPrem_Aperio....", flush=True)
        onprem_aperio = OnPrem_Aperio(Settings)
        onprem_aperio_config = onprem_aperio.Setup(trig_secrets, Settings.ONPREM_APERIO_STAGE_VAULT_PATH)

    # Setup for Locutus System Processing
    if not Settings.PROCESS_LOCUTUS_SYSTEM_STATUS:
        if Settings.LOCUTUS_VERBOSE:
            print("Locutus: NOT setting up PROCESS_LOCUTUS_SYSTEM_STATUS....", flush=True)
    else:
        num_modules_and_commands_to_process += 1
        print("Locutus: setting up PROCESS_LOCUTUS_SYSTEM_STATUS....", flush=True)
        # NOTE: nothing really to setup, LOL, since this is all being done through Settings


    ######################################################################
    # Locutus.ProcessAll() section:
    run_loop = True
    run_iteration_num = 0
    # total across all run iterations:
    num_errs_total = 0
    num_dicom_processed_total = 0
    # and our current [n] run iteration data:
    num_errs_this_run = 0
    num_dicom_processed_this_run = 0
    # and our [n-1] last_run iteration data:
    num_errs_last_run = 0
    num_dicom_processed_last_run = 0
    # and our [n-2] last_last_run iteration data:
    num_errs_last_last_run = 0
    num_dicom_processed_last_last_run = 0
    # TODO: yes, may want to swap these into lists/vectors/arrays/etc ;-)
    # any fatal errors in this current iteration?:
    this_run_has_fatal_errors = False


    if num_modules_and_commands_to_process == 0:
        print('Locutus: WARNING: No known modules or commands configured for processing; exiting.', flush=True)
        # TODO: consider an alternate exception, etc:
        exit(-1)

    # provide an initial pre-Processing message BEFORE the processing loop,
    # such that messages aren't printed each time if not LOCUTUS_VERBOSE
    if Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE:
        # NOTE: although some convergence framework has been build around each of the modules,
        # convergence only really applies to the actual DICOM DeID modules (as opposed to the Summarizer, Stage Compare, etc).
        # As such, go and and disable LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE
        # if any other than GCP or OnPrem are also selected:
        if Settings.PROCESS_DICOM_STAGE_COMPARE \
        or Settings.PROCESS_DICOM_SUMMARIZE_STATS \
        or Settings.PROCESS_ONPREM_APERIO_SLIDES_MANIFEST_SKIM \
        or Settings.PROCESS_ONPREM_APERIO_SLIDES:
            # NOTE: YES, even OnPrem currently disables, until convergence has been built in there:
            print('Locutus: Overriding LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE=={0} to False '\
                '(leaving LOCUTUS_RUN_MODE=={1}) '\
                'because sub-modules other than OnPrem or GCP are activated...'.format(
                    Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE,
                    Settings.LOCUTUS_RUN_MODE), flush=True)
            Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE = False
    elif Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE:
        print('Locutus: Overriding LOCUTUS_RUN_MODE=={0} with LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE '\
            'to begin processing loop of all sub-modules until a manifest convergence of processing...'.format(
                Settings.LOCUTUS_RUN_MODE), flush=True)
    elif Settings.LOCUTUS_RUN_MODE == "continuous":
        print('Locutus: LOCUTUS_RUN_MODE== continuous; About to begin continuous processing loop of all sub-modules ...', flush=True)
    else:
        print('Locutus: LOCUTUS_RUN_MODE== single; About to begin a single processing loop of all sub-modules ...', flush=True)
    ##############
    # else:  TODO: introduce a default else for any other unrecognized RUN_MODEs, terminating
    ##############


    while run_loop:
        # NOTE: given no convergence yet, shift this_run counters to last_run,
        # reset our current [n] run iteration data, and leaving our totals untouched:
        # AND our [n-2] last_last_run iteration data:
        num_dicom_processed_last_last_run = num_dicom_processed_last_run
        num_dicom_processed_last_run = num_dicom_processed_this_run
        num_dicom_processed_this_run = 0
        num_errs_last_last_run = num_errs_last_run
        num_errs_last_run = num_errs_this_run
        num_errs_this_run = 0

        # TODO: consider introducing try/catch blocks around these calls to Process():
        # especially now with LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE

        # Process for Command: DICOM-STAGE-COMPARE:
        # (non-DICOM-DeID to process for the first run only)
        if run_iteration_num == 0:
            if Settings.PROCESS_DICOM_STAGE_COMPARE:
                if Settings.LOCUTUS_VERBOSE:
                    print("Locutus DEBUG: About to... DICOMStageCompare.Process()", flush=True)
                this_stagecmp_errs = 0
                this_stagecmp_accessions_processed_successfully = 0
                try:
                    this_stagecmp_errs += dicom_stage_compare.Process()
                    # r3m0: TODO: ^^^^ update each Process() to return (num_errs_this_run, num_processed)
                except Exception as escaped_exception:
                    print("MAIN_Locutus ERROR DEBUG: carrying on after catching the following StageCompare.Process() escaped exception: {0}".format(
                                escaped_exception), flush=True)
                    this_stagecmp_errs = 1
                num_errs_this_run += this_stagecmp_errs
                # NOTE: num_dicom_processed_this_run is not applicable

        # Process for Command: DICOM-SPLIT-ACCESSION:
        # DEPRECATED DICOMSplitAccession 22 August 2024 (see DEPRECATED_cmd_dicom_split_accessio.py header for further details)
        # (non-DICOM-DeID to process for the first run only)
        #if not run_iteration_num == 0:
        #   if Settings.PROCESS_DICOM_SPLIT_ACCESSION:
        #       if Settings.LOCUTUS_VERBOSE:
        #           print("Locutus DEBUG: About to... DICOMSplitAccession.Process()", flush=True)
        #       num_errs_this_run += dicom_split_accession.Process()

        # Process for Command: DICOM-SUMMARIZE_STATS:
        # (non-DICOM-DeID to process for the first run only)
        if run_iteration_num == 0:
            if Settings.PROCESS_DICOM_SUMMARIZE_STATS:
                if Settings.LOCUTUS_VERBOSE:
                    print("Locutus DEBUG: About to... DICOMSummarizeStats.Process()", flush=True)
                this_summarizer_errs = 0
                this_summarizer_accessions_processed_successfully = 0
                try:
                    this_summarizer_errs += dicom_summarize_stats.Process()
                    # r3m0: TODO: ^^^^ update each Process() to return (num_errs, num_processed)
                except Exception as escaped_exception:
                    # NORMALLY:
                    ##### ##### #####
                    #print("MAIN_Locutus ERROR DEBUG: carrying on after catching the following Summarizer.Process() escaped exception: {0}".format(
                    #            escaped_exception), flush=True)
                    ##### ##### #####
                    # TODO: consider making these more flexible to the FORCE_SUCCESS flag,
                    # as that, when it and convergence are False,
                    # should allow the except to re-raise, yeah?
                    ##########
                    # For now, comment the above print, and uncomment the below section
                    # when wanting to see the full traceback:
                    ##### ##### #####
                    print("MAIN_Locutus ERROR DEBUG: TEMPORARILY re-raising exception to see traceback of the following Summarizer.Process() escaped exception: {0}".format(
                                escaped_exception), flush=True)
                    raise
                    ##### ##### #####
                    this_summarizer_errs = 1
                num_errs_this_run += this_summarizer_errs
                # NOTE: num_dicom_processed_this_run is not applicable

        # Process for Command: DICOM-SUMMARIZE_STATS:
        # (non-DICOM-DeID to process for the first run only)
        if run_iteration_num == 0:
            if Settings.PROCESS_DICOM_QC_FOR_BGDLAB_WORKSPACE:
                if Settings.LOCUTUS_VERBOSE:
                    print("Locutus DEBUG: About to... DICOM_QCforBGDlabWorkspace.Process()", flush=True)
                this_QCer_errs = 0
                this_QCer_accessions_processed_successfully = 0
                try:
                    this_QCer_errs += dicom_QCforBGlab.Process()
                    # r3m0: TODO: ^^^^ update each Process() to return (num_errs, num_processed)
                except Exception as escaped_exception:
                    # NORMALLY:
                    ##### ##### #####
                    #print("MAIN_Locutus ERROR DEBUG: carrying on after catching the following Summarizer.Process() escaped exception: {0}".format(
                    #            escaped_exception), flush=True)
                    ##### ##### #####
                    # TODO: consider making these more flexible to the FORCE_SUCCESS flag,
                    # as that, when it and convergence are False,
                    # should allow the except to re-raise, yeah?
                    ##########
                    # For now, comment the above print, and uncomment the below section
                    # when wanting to see the full traceback:
                    ##### ##### #####
                    print("MAIN_Locutus ERROR DEBUG: TEMPORARILY re-raising exception to see traceback of the following dicom_QCforBGlab.Process() escaped exception: {0}".format(
                                escaped_exception), flush=True)
                    raise
                    ##### ##### #####
                    this_QCer_errs = 1
                num_errs_this_run += this_QCer_errs
                # NOTE: num_dicom_processed_this_run is not applicable

        # Process for Module: GCP-DICOM:
        # DICOM-DeID to continue processsing to RUN_MODE,
        # and potential LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE...
        if Settings.PROCESS_GCP_DICOM_IMAGES:
            if Settings.LOCUTUS_VERBOSE:
                print("Locutus DEBUG: About to... GCPDicom.Process()", flush=True)
            this_gcp_errs = 0
            this_gcp_has_fatal_errors = False
            this_gcp_accessions_processed_successfully = 0
            try:
                (this_gcp_has_fatal_errors, this_gcp_errs, this_gcp_accessions_processed_successfully) = gcp_dicom.Process()
            except Exception as escaped_exception:
                print("MAIN_Locutus ERROR DEBUG: caught the following GCPDicom.Process() escaped exception: {0}".format(
                            escaped_exception), flush=True)
                #############################
                traceback_clues = traceback.format_exc()
                print("MAIN_Locutus ERROR DEBUG: suppressed traceback MAY look like: {0}".format(
                            traceback_clues), flush=True)
                #############################
                # nothing else to log with it at this point, since all GCPDicom.Process() should have updated
                # can likely infer that none where processed successfully, so:
                this_gcp_errs = 1
                # NOTE: check if the safe escaped_exception string contains GCP_INVALID_CREDS_ERR
                # if so, likewise set this_gcp_has_fatal_errors=True:
                safe_escaped_exception_msg = '{0}'.format(escaped_exception)
                if this_gcp_has_fatal_errors \
                or Settings.GCP_INVALID_CREDS_ERR_NAME in safe_escaped_exception_msg \
                or Settings.GCP_INVALID_CREDS_ERR in safe_escaped_exception_msg:
                    this_gcp_has_fatal_errors = True
                if this_gcp_has_fatal_errors:
                    print("MAIN_Locutus ERROR DEBUG: winding down after this FATAL escaped exception: {0}".format(
                            escaped_exception), flush=True)
                else:
                    print("MAIN_Locutus ERROR DEBUG: carrying on after the following NON-FATAL escaped exception: {0}".format(
                            escaped_exception), flush=True)

            num_errs_this_run += this_gcp_errs
            num_dicom_processed_this_run += this_gcp_accessions_processed_successfully
            if this_gcp_has_fatal_errors:
                this_run_has_fatal_errors = True
            #####

        # Process for Module: OnPrem-DICOM:
        # DICOM-DeID to continue processsing to RUN_MODE,
        # and potential LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE...
        if Settings.PROCESS_ONPREM_DICOM_IMAGES:
            if Settings.LOCUTUS_VERBOSE:
                print("Locutus DEBUG: About to... OnPrem_Dicom.Process()", flush=True)
            this_onprem_errs = 0
            this_onprem_has_fatal_errors = False
            this_onprem_accessions_processed_successfully = 0
            try:
                (this_onprem_has_fatal_errors, this_onprem_errs, this_onprem_accessions_processed_successfully) = onprem_dicom.Process()
            except Exception as escaped_exception:
                print("MAIN_Locutus ERROR DEBUG: carrying on after catching the following OnPremDicom.Process() escaped exception: {0}".format(
                            escaped_exception), flush=True)
                #############################
                traceback_clues = traceback.format_exc()
                print("MAIN_Locutus ERROR DEBUG: suppressed traceback MAY look like: {0}".format(
                            traceback_clues), flush=True)
                #############################
                # nothing else to log with it at this point, since all OnPremDicom.Process() should have updated
                # can likely infer that none where processed successfully, so:
                this_onprem_errs = 1
            num_errs_this_run += this_onprem_errs
            num_dicom_processed_this_run += this_onprem_accessions_processed_successfully
            if this_onprem_has_fatal_errors:
                this_run_has_fatal_errors = True
            #####

        # Process for Module: ONPREM-APERIO (Manifest Skimming only):
        # (non-DICOM-DeID to process for the first run only)
        if run_iteration_num == 0:
            if Settings.PROCESS_ONPREM_APERIO_SLIDES_MANIFEST_SKIM:
                if Settings.LOCUTUS_VERBOSE:
                    print("Locutus DEBUG: About to... OnPrem_Aperio.ProcessManifestSkim()", flush=True)
                this_aperioskim_errs = 0
                this_aperioskim_accessions_processed_successfully = 0
                try:
                    this_aperioskim_errs += onprem_aperio.ProcessManifestSkim()
                    # r3m0: TODO: ^^^^ update each Process() to return (num_errs, num_processed)
                except Exception as escaped_exception:
                    print("MAIN_Locutus ERROR DEBUG: carrying on after catching the following OnPremAperio.ProcessManifestSkim() escaped exception: {0}".format(
                                escaped_exception), flush=True)
                    this_aperioskim_errs = 1
                num_errs_this_run += this_aperioskim_errs
                # NOTE: num_dicom_processed_this_run is not applicable

        # Process for Module: ONPREM-APERIO (normal processing):
        # (non-DICOM-DeID to process for the first run only)
        if run_iteration_num == 0:
            if Settings.PROCESS_ONPREM_APERIO_SLIDES:
                if Settings.LOCUTUS_VERBOSE:
                    print("Locutus DEBUG: About to... OnPrem_Aperio.Process()", flush=True)
                this_aperio_errs = 0
                this_aperio_accessions_processed_successfully = 0
                try:
                    this_aperio_errs += onprem_aperio.Process()
                    # r3m0: TODO: ^^^^ update each Process() to return (num_errs, num_processed)
                except Exception as escaped_exception:
                    print("MAIN_Locutus ERROR DEBUG: carrying on after catching the following OnPremAperio.Process() escaped exception: {0}".format(
                                escaped_exception), flush=True)
                    this_aperio_errs = 1
                num_errs_this_run += this_aperio_errs
                # NOTE: num_dicom_processed_this_run is not applicable


        # Summarize/Continue any processing re: run mode and num_errs_this_run from above:
        run_iteration_num += 1

        if Settings.LOCUTUS_RUN_MODE == 'continuous' \
        or Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE:
            ######################################################################
            # BEFORE the next convergence conditional, check Locutus DB..... System Status section:
            no_sysDBconnSession = None

            ###########
            # first, for CFG_OUT, check the Locutus overall status:
            no_sysDBconnSession = None
            check_Docker_node=False
            alt_node_name=None
            check_module=False
            module_name=SYS_STAT_MODULENAME
            (curr_sys_stat_overall, sys_msg_overall, sys_node_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
            print('{0},{1},overall,{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_overall, sys_msg_overall), flush=True)

            # next, also check the node-level status that we'll ultimately use
            # (even if the above overall active status is False, for completeness):
            check_Docker_node=True
            # setting alt_node_name heer to DOCKERHOST is equivalent to leaving it None here, but go ahead and explicitly set it:
            alt_node_name = os.environ.get('DOCKERHOST_HOSTNAME')
            (curr_sys_stat_node, sys_msg_node, sys_node_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
            print('{0},{1},node={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_node, sys_msg_node), flush=True)

            # NOTE: no need to check module-specific yet, since this is overall main_locutus, effectively same as overall,
            # but doing so anyway:
            # and then, also check the module-specific status that we'll also ultimately use
            # (still checking even if either of the above overall & node-level active status are False, for completeness):
            check_Docker_node=False
            check_module=True
            (curr_sys_stat_module, sys_msg_module, sys_module_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
            print('{0},{1},module={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_module_name, curr_sys_stat_module, sys_msg_module), flush=True)

            # WAS: only need to evaluate curr_sys_stat_node, since it takes into account curr_sys_stat_overall during its calculation
            # NOTE: but now, looking at node and module, they are each independently queried via get_Locutus_system_status(),
            # so go ahead and explicitly check for full activity across all three:
            if curr_sys_stat_overall and curr_sys_stat_node and curr_sys_stat_module:
                print('r3m0 DEBUG: main_locutus sees Locutus system is active overall and for this node & module.... carry on!')
            else:
                run_loop = False
                print('FATAL ERROR: main_locutus sees Locutus system is NOT currently active overall, or for this node &/or module.... setting this_run_has_fatal_errors to trigger LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE.')
                # no need to exit(-1), since it will fall out of the convergence loops:
                # likewise, no need for: Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE = False
                # so long as this_run_has_fatal_errors:
                this_run_has_fatal_errors = True
            ###########

        if Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE:
            # Essentially an expanded RUN_MODE == 'single'
            # (effectively, a limited RUN_MODE == 'continuous' as well, applying to either)
            # As a qualifier to the RUN_MODE until perceived MANIFEST CONVERGENCE, namely:

            # After the 1st iteration, disable any force_reprocess, predelete, or preretire
            #   that MIGHT have been enabled on the first pass;
            #   otherwise we will end up infinitely attempting to reprocess the same again and again

            # disable FORCE_REPROCESS_ACCESSION_STATUS:
            if Settings.LOCUTUS_GCP_DICOM_FORCE_REPROCESS_ACCESSION_STATUS:
                print('LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE disabling LOCUTUS_GCP_DICOM_FORCE_REPROCESS_ACCESSION_STATUS for subsequent iterations')
                Settings.LOCUTUS_GCP_DICOM_FORCE_REPROCESS_ACCESSION_STATUS = False
            if Settings.LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS:
                print('LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE disabling LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS for subsequent iterations')
                Settings.LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS = False

            # disable PREDELETE_ACCESSION_STATUS:
            if Settings.LOCUTUS_GCP_DICOM_PREDELETE_ACCESSION_STATUS:
                print('Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE disabling LOCUTUS_GCP_DICOM_PREDELETE_ACCESSION_STATUS for subsequent iterations')
                Settings.LOCUTUS_GCP_DICOM_PREDELETE_ACCESSION_STATUS = False
            if Settings.LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS:
                print('Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE disabling LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS for subsequent iterations')
                Settings.LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS = False

            # disable PRERETIRE_ACCESSION_STATUS:
            if Settings.LOCUTUS_GCP_DICOM_PRERETIRE_ACCESSION_STATUS:
                print('Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE disabling LOCUTUS_GCP_DICOM_PRERETIRE_ACCESSION_STATUS for subsequent iterations')
                Settings.LOCUTUS_GCP_DICOM_PRERETIRE_ACCESSION_STATUS = False
            if Settings.LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS:
                print('Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE disabling LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS for subsequent iterations')
                Settings.LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS = False
            ######
            # TODO: look out for fatal issues such as GCP Credential Rotations
            # NOTE: even without them yet explicitly checked, in theory the below convergence detection should catch it,
            # because no more accessions will be processing until resolved.
            ######
            num_errs_total += num_errs_this_run
            num_dicom_processed_total += num_dicom_processed_this_run
            # NOTE: leave an ending space in the convergence_msg prefix
            # (NOTE: otherwise empty for this general DEBUG print)
            convergence_msg = ' '

            ##################################
            # convergence detection criteria:
            if ( this_run_has_fatal_errors ) \
            or ( (num_errs_this_run) == 0 and (num_dicom_processed_this_run == 0)) \
            or ( (run_iteration_num > 0) \
                and (num_errs_this_run == num_errs_last_run) and (num_dicom_processed_this_run == num_dicom_processed_last_run) \
                and (num_errs_last_run == num_errs_last_last_run) and (num_dicom_processed_last_run == num_dicom_processed_last_last_run) ):
                # NOTE: the above could increase (run_iteration_num > 1) to ensure that we've got last and last_last, but with those as 0s, we might just call it early.
                # Either way.....
                # convergence criteria conditions, as hopefully within the alloted MAX_ITERATIONS
                run_loop = False
                # NOTE: leave an ending space in the convergence_msg prefix
                convergence_msg = 'calling CONVERGENCE following '
                if ( this_run_has_fatal_errors ):
                    # FATAL convergence criteria condition met
                    convergence_msg = 'calling FATAL CONVERGENCE following FATAL '
                # TODO: merely set run_loop & build up the convergence_msg within this if/elif/else,
                # but move the below print() further down below, to beyond the if/elif/else ladder:
                # NOTE: no longer DEBUG SIMULATION ERRORS:
                print('Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE: {0}run iteration # {1} (of {2} MAX), sharing Process()-encountered '\
                        'num_errs_this/last/lastlast_run={3}/{4}/{5}, num_errs_total={6} & num_dicom_processed_this/last/lastlast_run={7}/{8}/{9}, num_dicom_processed_total={10}, '\
                        'AND this_run_has_fatal_errors={11}; '\
                        'ending run loop now.'.format(
                        convergence_msg,
                        run_iteration_num, Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE_MAX_ITERATIONS,
                        num_errs_this_run, num_errs_last_run, num_errs_last_last_run, num_errs_total,
                        num_dicom_processed_this_run, num_dicom_processed_last_run, num_dicom_processed_last_last_run, num_dicom_processed_total,
                        this_run_has_fatal_errors), flush=True)
                # TODO: and THEN, do so for all others as well, giving the this/last/lastlast
            elif (run_iteration_num >= Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE_MAX_ITERATIONS):
                # TODO: these 3 (above and below 2) are very similar, and eventually themselves converge.
                # For now, though, do de-couple his MAX_ITERATIONS condition from the above, and both from the conver
                # MAX_ITERATIONS terminating condition if not yet converged:
                run_loop = False
                # NOTE: leave an ending space in the convergence_msg prefix
                convergence_msg = 'TERMINATING thanks to MAX following '
                # TODO: merely set run_loop & build up the convergence_msg within this if/elif/else,
                # but move the below print() further down below, to beyond the if/elif/else ladder:
                print('Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE: {0}run iteration # {1} (as AT the MAX of {2} MAX), this last module Process() encountered: '\
                        'num_errs_this/last/lastlast_run={3}/{4}/{5}, num_errs_total={6} & num_dicom_processed_this/last/lastlast_run={7}/{8}/{9}, num_dicom_processed_total={10}, '\
                        'AND this_run_has_fatal_errors={11}; '\
                        'ending run loops now.  Thanks, MAX!'.format(
                        convergence_msg,
                        run_iteration_num, Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE_MAX_ITERATIONS,
                        num_errs_this_run, num_errs_last_run, num_errs_last_last_run, num_errs_total,
                        num_dicom_processed_this_run, num_dicom_processed_last_run, num_dicom_processed_last_last_run, num_dicom_processed_total,
                        this_run_has_fatal_errors), flush=True)
                # TODO: and THEN, do so for all others as well, giving the this/last/lastlast
            else:
                # leaving run_loop = True
                # WAS: if Settings.LOCUTUS_VERBOSE:
                # Share LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE iteration calculations, regardless of VERBOSE:
                # NOTE: leave an ending space in the convergence_msg prefix
                convergence_msg = 'NOT YET converging following '
                # TODO: merely set run_loop (or not, in this case) & build up the convergence_msg within this if/elif/else,
                # but move the below print() further down below, to beyond the if/elif/else ladder...
                # ALTHOUGH, this non-convegence iteration continuation section does need to Setup_Input_Manifest() again,
                # and perhaps its print can indeed follow, as well as the sleep (pending a flag):
                print('Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE: {0}run iteration # {1} (of {2} MAX): this module Process() encountered: '\
                        'num_errs_this/last/lastlast_run={3}/{4}/{5}, num_errs_total={6} & num_dicom_processed_this/last/lastlast_run={7}/{8}/{9}, num_dicom_processed_total={10}, '\
                        'AND this_run_has_fatal_errors={11}; '\
                        'about to loop through another run...'.format(
                        convergence_msg,
                        run_iteration_num, Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE_MAX_ITERATIONS,
                        num_errs_this_run, num_errs_last_run, num_errs_last_last_run, num_errs_total,
                        num_dicom_processed_this_run, num_dicom_processed_last_run, num_dicom_processed_last_last_run, num_dicom_processed_total,
                        this_run_has_fatal_errors), flush=True)
                # TODO: and THEN, do so for all others as well, giving the this/last/lastlast

                # with current run_iteration_num to be emitted in the commented header:
                if Settings.PROCESS_GCP_DICOM_IMAGES:
                    # re-open the GCP Input Manifest
                    # WAS: if Settings.LOCUTUS_VERBOSE:
                    # Share LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE iteration calculations, regardless of VERBOSE:
                    print("Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE: About to... GCPDicom.Setup_Input_Manifest()", flush=True)
                    # NOTE: may ALSO need to re-open the DB connection, if it was shut down with all raise()/ValueError()s?
                    gcp_dicom.Setup_Input_Manifest(run_iteration_num)
                #####
                if Settings.PROCESS_ONPREM_DICOM_IMAGES:
                    # re-open the OnPrem Input Manifest
                    # WAS: if Settings.LOCUTUS_VERBOSE:
                    # Share LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE iteration calculations, regardless of VERBOSE:
                    print("Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE: About to... OnPrem_Dicom.Setup_Input_Manifest()", flush=True)
                    onprem_dicom.Setup_Input_Manifest(run_iteration_num)
                #####
                print('Locutus LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE: '\
                        'Waiting the LOCUTUS_CONTINUOUS_WAIT_SECS of {0} seconds...'.format(
                        Settings.LOCUTUS_CONTINUOUS_WAIT_SECS),
                        flush=True)
                time.sleep(Settings.LOCUTUS_CONTINUOUS_WAIT_SECS)
                ######################################################################
                # Following a sleep, and IMMEDIATELY BEFORE the next convergence iteration,
                # one more check of the Locutus DB..... System Status section:
                no_sysDBconnSession = None
                check_Docker_node=True

                no_sysDBconnSession = None
                check_Docker_node=False
                alt_node_name=None
                check_module=False
                module_name=SYS_STAT_MODULENAME
                (curr_sys_stat_overall, sys_msg_overall, sys_node_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
                print('{0},{1},overall,{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_overall, sys_msg_overall), flush=True)

                # next, also check the node-level status that we'll ultimately use
                # (even if the above overall active status is False, for completeness):
                check_Docker_node=True
                # setting alt_node_name heer to DOCKERHOST is equivalent to leaving it None here, but go ahead and explicitly set it:
                alt_node_name = os.environ.get('DOCKERHOST_HOSTNAME')
                (curr_sys_stat_node, sys_msg_node, sys_node_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
                print('{0},{1},node={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_node, sys_msg_node), flush=True)

                # NOTE: no need to check module-specific yet, since this is overall main_locutus, effectively same as overall,
                # but doing so anyway:
                # and then, also check the module-specific status that we'll also ultimately use
                # (still checking even if either of the above overall & node-level active status are False, for completeness):
                check_Docker_node=False
                check_module=True
                (curr_sys_stat_module, sys_msg_module, sys_module_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
                print('{0},{1},module={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_module_name, curr_sys_stat_module, sys_msg_module), flush=True)

                # WAS: only need to evaluate curr_sys_stat_node, since it takes into account curr_sys_stat_overall during its calculation
                # NOTE: but now, looking at node and module, they are each independently queried via get_Locutus_system_status(),
                # so go ahead and explicitly check for full activity across all three:
                if curr_sys_stat_overall and curr_sys_stat_node and curr_sys_stat_module:
                    print('r3m0 DEBUG: main_locutus sees Locutus system is active overall and for this node & module.... carry on!')
                else:
                    run_loop = False
                    print('FATAL ERROR: main_locutus sees Locutus system is NOT currently active overall or for this node / module.... setting this_run_has_fatal_errors to trigger LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE.')
                    # no need to exit(-1), since it will fall out of the convergence loops:
                    # likewise, no need for: Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE = False
                    # so long as this_run_has_fatal_errors:
                    this_run_has_fatal_errors = True
                    #####
                    #print('r3m0 DEBUG: NOT YET BAILING, see if GCP can catch this????')
                    #run_loop = True
                    #this_run_has_fatal_errors = False
                ###########


            # looking to assist in the pattern of: deploy full batch, note errors, re-deploy errored subset, note remaining errors, re-deploy til done
            # as a middle ground between  single_shot and fully continuous
            # while any non-PENDING/non-MULTI remain unPROCESSED
            # to be careful of force_reprocess, predelete or preretire, auto-disabling those after the 1st pass.

            # NOTE: regardless of above convergence,
            # *_this_run will be reset automatically at the top of the loop;
            # TODO: consider doing that for the *_last_runs as well.

            # 1) compare tallies of num_processed > 0
            # as well as of the previous iteration
            # 2) ensure no FATAL ERRORS (e.g., GCP_CREDS)
        elif Settings.LOCUTUS_RUN_MODE == 'continuous':
            # RUN_MODE == 'continuous':
            # and not Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE:
            if num_errs_this_run > 0:
                print('Locutus CONTINUOUS MODE: ERROR: encountered {0} errors during Processing; '\
                            'ending continous mode now.'.format(num_errs_this_run), flush=True)
                run_loop = False
            else:
                if Settings.LOCUTUS_VERBOSE:
                    print('Locutus CONTINUOUS MODE: Everything has been Processed without error: '\
                            'Waiting and checking every {0} seconds...'.format(
                            Settings.LOCUTUS_CONTINUOUS_WAIT_SECS),
                            flush=True)
                time.sleep(Settings.LOCUTUS_CONTINUOUS_WAIT_SECS)
                ######################################################################
                # Following a sleep, and IMMEDIATELY BEFORE the next convergence iteration,
                # one more set of checks across the Locutus DB..... System Status section:


                ###########
                # first, for CFG_OUT, check the Locutus overall status:
                no_sysDBconnSession = None
                check_Docker_node=False
                alt_node_name=None
                check_module=False
                module_name=SYS_STAT_MODULENAME
                (curr_sys_stat_overall, sys_msg_overall, sys_node_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
                print('{0},{1},overall,{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_overall, sys_msg_overall), flush=True)

                # next, also check the node-level status that we'll ultimately use
                # (even if the above overall active status is False, for completeness):
                check_Docker_node=True
                # setting alt_node_name heer to DOCKERHOST is equivalent to leaving it None here, but go ahead and explicitly set it:
                alt_node_name = os.environ.get('DOCKERHOST_HOSTNAME')
                (curr_sys_stat_node, sys_msg_node, sys_node_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
                print('{0},{1},node={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_node, sys_msg_node), flush=True)

                # NOTE: no need to check module-specific yet, since this is overall main_locutus, effectively same as overall,
                # but doing so anyway:
                # and then, also check the module-specific status that we'll also ultimately use
                # (still checking even if either of the above overall & node-level active status are False, for completeness):
                check_Docker_node=False
                check_module=True
                (curr_sys_stat_module, sys_msg_module, sys_module_name) = Settings.get_Locutus_system_status(Settings, check_Docker_node, alt_node_name, check_module, module_name, no_sysDBconnSession, locutus_target_db)
                print('{0},{1},module={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "main_GET_LOCUTUS_SYS_STATUS", sys_module_name, curr_sys_stat_module, sys_msg_module), flush=True)

                # WAS: only need to evaluate curr_sys_stat_node, since it takes into account curr_sys_stat_overall during its calculation
                # NOTE: but now, looking at node and module, they are each independently queried via get_Locutus_system_status(),
                # so go ahead and explicitly check for full activity across all three:
                if curr_sys_stat_overall and curr_sys_stat_node and curr_sys_stat_module:
                    print('r3m0 DEBUG: main_locutus sees Locutus system is active overall and for this node & module.... carry on!')
                else:
                    run_loop = False
                    print('FATAL ERROR: main_locutus sees Locutus system is NOT currently active overall, or for this node &/or module.... setting this_run_has_fatal_errors to trigger end of continuous mode.')
                    # no need to exit(-1), since it will fall out of the convergence loops:
                    # likewise, no need for: Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE = False
                    # so long as this_run_has_fatal_errors:
                    this_run_has_fatal_errors = True
                    #####
                    #print('r3m0 DEBUG: NOT YET BAILING, see if GCP can catch this????')
                    #run_loop = True
                    #this_run_has_fatal_errors = False
                ###########
        elif Settings.LOCUTUS_RUN_MODE == 'single':
            # RUN_MODE == 'single':
            # and not Settings.LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE:
            if num_errs_this_run > 0:
                print('Locutus SINGLE MODE: ERROR: encountered {0} errors during Processing; '\
                            'ending now.'.format(
                            num_errs_this_run),
                            flush=True)
            else:
                print('Locutus SINGLE MODE: Everything has been processed; ending this '\
                            'single-shot mode.',
                            flush=True)
            run_loop = False
        ##############
        # else:  TODO: introduce a default else for any other unrecognized RUN_MODEs, terminating
        ##############


    print("JUST ABOUT to say Goodbye from Locutus, but first, waiting thirty seconds to test log flushing...", flush=True)
    # print a message every 10 seconds, so consider 12 such iterations:
    # reducing to just 30 seconds:
    for i in range(0, 3):
        print("sleeping 10 seconds, iteration {0} of 3...".format(i), flush=True)
        time.sleep(10)
    print("Goodbye from Locutus!", flush=True)
    sys.exit(num_errs_total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Locutus processing as configured via its config.yaml.')
    args = parser.parse_args()
    main(args)
