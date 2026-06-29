#!/usr/bin/env python3
# cmd_dicom_summarize_status.py:
#
# a Locutus/DICOM helper script that
# expects an input CSV
# confirms the header is as expected "accession_num" as the first (and only read) column
# runs through each input line
# 	and prints out a summarized view of their status/statuses/stati

####################
# DEV NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
# Base Summarizer features have been tested on a Re-Migrated alpha-numeric workspace.
# TODO: sidecar features still needing testing include:
#   * Preloader
#   * Show Multi-UUIDs
#   * Multi-UUID Resolver
# as well as some final polishing of the WIP code :-)
####################



import csv
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime
import re,pdb
from decimal import *
import time
import math # for math.floor() :-)
# for new ORTHANC_OUT_PREFIX clues with show_multiuuids & expand_multiuuids_to_manifest_output()
import src_3rdParty.OrthancRestToolbox as RestToolbox
############
# and for the general settings in Locutus's settings.py:
import src_modules.settings
###########
# NOTE: each Locutus module w/ potential helpers for the Preloader & its CREATE TABLES:
from src_modules.module_onprem_dicom import OnPrem_Dicom    # & its create_db_tables() for the Preloader

DEFAULT_CHANGE_TYPE_STUDY = 'StableStudy'
DEFAULT_CHANGE_TYPE_PATIENT = 'StablePatient'


# NOTE: the following are LOCUTUS-specific constants for this ONPREM_DICOM module:
# NOTE: Moved to Settings: DICOM_MODULE_ONPREM = "ONPREM"
# TODO: consider moving some of the rest of these there as well.
DEF_LOCUTUS_ONPREM_DICOM_STATUS_TABLE = 'onprem_dicom_status'
LOCUTUS_ONPREM_DICOM_STATUS_TARGET_FIELD = 'deidentified_targets'
LOCUTUS_ONPREM_DICOM_STATUS_DEID_QC_STATUS_FIELD = 'deid_qc_status'
LOCUTUS_ONPREM_DICOM_STATUS_DEID_QC_EXPLORER_FIELD = 'deid_qc_explorer_study_url'
DEF_LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE = 'onprem_dicom_manifest'
# TODO: DEF_LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE = 'onprem_dicom_int_cfgs'
#
# w/ the above as initial default values to the following, to be used in the code:
LOCUTUS_ONPREM_DICOM_STATUS_TABLE = DEF_LOCUTUS_ONPREM_DICOM_STATUS_TABLE
LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE = DEF_LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
# TODO: LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE = DEF_LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE
###############################
# NOTE: the above table pairs can be overridden in Setup() w/ a Workspace configuration
# to set the WS_WORKSPACE_NAME, initially as WS_WORKSPACE_DEFAULT_NAME.
# for example, given a WS_WORKSPACE_NAME='bgd2025on', the table pair becomes:
# LOCUTUS_ONPREM_DICOM_STATUS_TABLE = 'onprem_dicom_ws_bgd2025on_status'
# LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE = 'onprem_dicom_ws_bgd2025on_manifest'
# LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE = 'onprem_dicom_ws_bgd2025on_int_cfgs'
###############################
WS_LOCUTUS_ONPREM_TABLE_PREFIX = 'onprem_dicom_ws_'
WS_WORKSPACE_DEFAULT_NAME='DEFAULT'
WS_LOCUTUS_ONPREM_STATUS_TABLE_SUFFIX = '_status'
WS_LOCUTUS_ONPREM_MANIFEST_TABLE_SUFFIX = '_manifest'
WS_LOCUTUS_ONPREM_INTCFGS_TABLE_SUFFIX = '_int_cfgs'
###############################
# NOTE: leaving the disable of Workspaces to default back to DEF_LOCUTUS_ONPREM_DICOM_*_TABLE
###############################

# Staging DB: (for LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS)
STAGING_DICOM_STABLE_STUDIES_TABLE = 'dicom_stablestudies'

# expected RESOLVE_VIA command modes:
#######################################
# NOTE: each of these modes operate on the whole accession level,
# and apply to its entire set of multi-uuids,
#
# MERGE_RADIOLOGY: AUTOMATIC DB removal all uuids of accession, for re-request of Radiology-merged accession:
RESOLVE_VIA_MERGE_RADIOLOGY = 'MERGE_at_RADIOLOGY'
# and an ALIAS to the above, with the same internal clearing, but simply for a RE-SEND from RADIOLOGY:
RESOLVE_VIA_RESEND_RADIOLOGY = 'RESEND_RADIOLOGY'
#
# CONSOLIDATE_LOCALLY: AUTOMATIC DB removal of all but the uuid for the max/latest change
RESOLVE_VIA_CONSOLIDATE_LOCALLY = 'CONSOLIDATE_LOCALLY'
#
# CHOOSE_LOCALLY: a more directed semi-automatic DB removal of all but the uuid specified following the uuid= prefix:
RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX = 'CHOOSE_LOCALLY:uuid='
#
# DELETE_SCAN_LOCALLY: AUTOMATIC DB removal of THIS accession's uuids
RESOLVE_VIA_DELETE_LOCALLY = 'DELETE_SCAN_LOCALLY'
# NOTING that the above ^^^ will delete from MANIFEST, STATUS, and STAGING tables.
# TODO: ensure that each of the MANIFEST and STATUS are also robust enough
# to carry on should there not be any applicable records there?
# Just in case not yet (and/or, for clarity when resolving only the stage),
# also provide DELETE_SCAN_FROM_STAGE_ONLY, next.
#
# DELETE_SCAN_FROM_STAGE_ONLY: AUTOMATIC DB removal of this STAGED-ONLY accession (not in the Locutus data space)
RESOLVE_VIA_DELETE_STAGE_ONLY = 'DELETE_SCAN_FROM_STAGE_ONLY'
# This is a subset of DELETE_SCAN_LOCALLY, and might not even be needed
# if..... the preceding superset DELETE_SCAN_LOCALLY subcmd is robust enough to manage
# such staged, but not yet in the Locutus DB, accessions (TBD)
##########################################
#########
# TODO: all of the above need additional capabilities to:
# * DONE: delete the uuids from Orthanc :-D
# * remove any applicable deid_targets from GCP's GS, isilon, etc.
#########
ERROR_RESOLVING_PREFIX='ERROR_RESOLVING_MULTIUUID_CANDIDATE'
# tentative RESOLVING_MULTIUUID manifest_status clue for when NOT enable_db:
#SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX='RESOLVING_MULTIUUID_CANDIDATE'
# let the actual OUTPUT_STATUS say something about RESOLVED,
# leaving the SAFEMODE prexix to simply DRYRUN,
# with its tailing colon generating an extra colon in the resolve_*() methods,
# which makes a nice double delimiter in viewing the MANIFEST_OUTPUT:
SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX='DRYRUN:'
#
# NOTE: the corresponding outputs for the above RESOLVE_VIA are as follows,
# and have been moved to Settings.py for consistency with the other MANIFEST_OUTPUT_STATUS_*
"""
# and additional post-resolution STATUS possibilities:
MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_MERGE = 'PENDING_CHANGE_RADIOLOGY_MERGE'
MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_RESEND = 'PENDING_CHANGE_RADIOLOGY_RESEND'
MANIFEST_OUTPUT_STATUS_PENDING_AFTER_LOCAL_CONSOLIDATION = 'PENDING_CHANGE_RESOLVED_LOCALLY'
MANIFEST_OUTPUT_STATUS_OTHER_AFTER_LOCAL_CONSOLIDATION = 'RESOLVED_MULTIUUIDS:CONSOLIDATED_LOCALLY'
MANIFEST_OUTPUT_STATUS_AFTER_LOCAL_CHOICE = 'RESOLVED_MULTIUUIDS:CHOSEN_LOCALLY'
MANIFEST_OUTPUT_STATUS_AFTER_LOCAL_DELETE = 'RESOLVED_MULTIUUIDS:DELETED_LOCALLY'
"""

# NOTE: these MIN/MAX_PROCESSING_PHASE(s) are needed here in the Summarizer for the Preloader and resolve_multiuuids
# and for the resolve_multiuuids, as from OnPrem_dicom, completion of Phase 3 would indicate partial processing progress:
MIN_PROCESSING_PHASE = 2
MAX_PROCESSING_PHASE = 5
# TODO: eventually expand to per-module capability (selecting the MIN/MAX of OnPrem, as current module)
# for all methods that query by or update to such a MIN/MAX_PROCESSING_PHASE:
# self.locutus_settings.DICOM_ONPREM_MIN_PROCESSING_PHASE
# self.locutus_settings.DICOM_ONPREM_MAX_PROCESSING_PHASE

#####
# self.locutus_settings.SQL_OUT_PREFIX: An additional preface field for generating a SQL_OUT: in stdout (e.g., from DRY RUNs to manually perform the SQL), as via:
#    grep SQL_OUT [log] | sed 's/SQL_OUT:,//` > sql_out.csv
# NOTE: now to reference as: self.locutus_settings.SQL_OUT_PREFIX
#####


# An additional preface field for generating ORTHANC_OUT: in stdout (e.g., from DRY RUNs to manually perform the ORTHANC curl), as via:
#    grep ORTHANC_OUT [log] | sed 's/ORTHANC_OUT:,//` > orthanc_out.csv
ORTHANC_OUT_PREFIX = "ORTHANC_OUT:"

# And, additional fields for generating a MANIFEST_OUTPUT in stdout via:
#    grep MANIFEST_OUTPUT [log] | sed 's/MANIFEST_OUTPUT:,//` > manifest_out.csv
#
MANIFEST_OUTPUT_PREFIX = src_modules.settings.MANIFEST_OUTPUT_PREFIX
#
MANIFEST_HEADER_STATS_OUT = "#STATUS_OUT:"
MANIFEST_HEADER_COUNT = "COUNT"
MANIFEST_HEADER_STATUS = "STATUS"
#####
MANIFEST_HEADER_ACCESSIONS_TOTAL_MANIFEST = "TOTAL_ACCESSIONS_VIA_MANIFEST"

#WAS: MANIFEST_HEADER_ACCESSIONS_TOTAL_BATCH = "TOTAL_ACCESSIONS_VIA_DB_BATCH"
# changing "VIA" to "IN", as a subtle highlight that even if Bypassing the Manifest,
# a non-empty BATCH_NAME will still apply from the CSV manifest, as in....
# the corresponding batch-specific MANIFEST records will be used.
MANIFEST_HEADER_ACCESSIONS_TOTAL_BATCH = src_modules.settings.MANIFEST_HEADER_ACCESSIONS_TOTAL_BATCH
MANIFEST_HEADER_ACCESSIONS_FILTERED_BATCH = src_modules.settings.MANIFEST_HEADER_ACCESSIONS_FILTERED_BATCH
MANIFEST_HEADER_ACCESSIONS_COUNTED_FILTERED_BATCH = src_modules.settings.MANIFEST_HEADER_ACCESSIONS_COUNTED_FILTERED_BATCH
MANIFEST_HEADER_BATCH_FILTER = src_modules.settings.MANIFEST_HEADER_BATCH_FILTER
MANIFEST_HEADER_BATCH_COUNTER_RANGE = src_modules.settings.MANIFEST_HEADER_BATCH_COUNTER_RANGE
#####
MANIFEST_HEADER_ACCESSION_DUPLICATES = "DUPLICATE_ACCESSIONS_IN_MANIFEST"
MANIFEST_HEADER_DEID_DATETIME = "LAST_DATETIME_PROCESSED"
MANIFEST_HEADER_DEID_TARGET = "TARGET"
MANIFEST_HEADER_SUMMARIZED_FROM = "VERSION_SUMMARIZED"
MANIFEST_HEADER_AS_OF_DATETIME = "SUMMARIZED_AS_OF:"

"""
# NOTE: moving these into Settings for more access to the Batch & CVS Manifest methods, etc:
# An internal configuration to allow the printing of commented lines into the MANIFEST_OUTPUT.
# Consider, for example, input manifests which might still have a pre-split accession number listed,
# but commented out, and is followed by its respective split accession nunmbers.
# We will want this included in the MANIFEST_OUTPUT such that all rows still align:
MANIFEST_OUTPUT_INCLUDE_COMMENT_LINES = True
"""

###################### ###################### ######################
# NOTE: all status #defines available via Settings:
###################### ###################### ######################

# Additional MANIFEST_OUTPUT columns specific to ONPREM:
MANIFEST_OUTPUT_ONPREM_EXTRAS = 'ONPREM-XTRAS:'
MANIFEST_HEADER_OUTPUT_ONPREM_FILENAME_FOR_LOCAL = 'filename4local'
MANIFEST_HEADER_OUTPUT_ONPREM_FW_GROUP = 'flywheel_group'
MANIFEST_HEADER_OUTPUT_ONPREM_FW_PROJECT = 'flywheel_project'
MANIFEST_HEADER_OUTPUT_ONPREM_FW_IMPORT_SUBJECT_ARG = 'flywheel_subject_import_arg'
MANIFEST_HEADER_OUTPUT_ONPREM_FW_IMPORT_SESSION_ARG = 'flywheel_session_import_arg'
# TODO: eventually adjust all scripts/post-onprem-dicom-summarize-helper_* to support a move UP of the following:
MANIFEST_HEADER_OUTPUT_ONPREM_DEID_QC_STATUS = 'DEID_QC_STATUS'
MANIFEST_HEADER_OUTPUT_ONPREM_DEID_QC_STUDY_EXPLORER_URL= 'DEID_QC_STUDY_URL'
# NOTE: src_modules/module_onprem_dicom.py MANIFEST_OUPUT emits the above...
# * DEID_QC_STATUS *before* ONPREM_MANIFEST_VER_COLUMN_OFFSET
# * DEID_STUDY_EXPLORER_URL *immediately* after ONPREM_MANIFEST_VER_COLUMN_OFFSET
# but, putting them here at the end of the list (for now),
# so as to not disrupt the downstream post-onprem-dicom-summarize-helper_* scripts... for now.


ACCESSION_DUPLICATED = "WARNING:duplicates"

CLASS_PRINTNAME = 'DICOMSummarizeStats'

class DICOMSummarizeStats:

    def __init__(self, locutus_settings):
        self.locutus_settings = locutus_settings
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE: {0}.init() says Hi'.format(CLASS_PRINTNAME), flush=True)
        self.this_hostname = ''

        self.status_table = None
        self.status_target_field = None
        self.manifest_table = None
        self.stage_table = None
        # to facilitate resolve_multiuuids():
        self.resolved_multiuuid_whole_accessions = []


    def Setup(self, trig_secrets, stager_vault_config_path):
        print('{0}.Setup() initializing...'.format(CLASS_PRINTNAME), flush=True)

        global LOCUTUS_ONPREM_DICOM_STATUS_TABLE
        global LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
        # TODO: global LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE


        ###################################
        # validate input parameter for: LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS:
            if (not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS \
                or self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS):
                # requires LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and NOT LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS
                resolve_parms_msg = '{0}.Setup(): ERROR: LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS(={1}) requires that '\
                                'LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS(={2}) must be True, '\
                                'and LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSION(={3}) must be False '.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS,
                                self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS,
                                self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS)
                print(resolve_parms_msg, flush=True)
                raise ValueError(resolve_parms_msg)
            elif self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
                # requires NOT LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB
                # i.e., if self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
                # BATCHES NOTE: Setup() requiremes manifest (even if a manifest-once Batch) when Resolving
                resolve_parms_msg = '{0}.Setup(): ERROR: Resolver must use a CSV manifest with RESOLVE_CMDs. '\
                    'LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB is currently \'{1}\'; '\
                    'please disable to utilize Resolver w/ CSV'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB)
                print(resolve_parms_msg, flush=True)
                raise ValueError(resolve_parms_msg)

        if (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM):
            # introducing... Locutus Workspaces, for DICOM_MODULE_ONPREM:
            if self.locutus_settings.LOCUTUS_WORKSPACES_ENABLE:
                if not self.locutus_settings.LOCUTUS_WORKSPACE_NAME \
                or not len(self.locutus_settings.LOCUTUS_WORKSPACE_NAME):
                    # Q: do we want to this ERROR out, or fall back to the LOCUTUS_WORKSPACE_NAME of DEFAULT, or just disable Workspaces?
                    print('{0}.Setup(): ERROR: LOCUTUS_WORKSPACES_ENABLE set, but LOCUTUS_WORKSPACE_NAME *not* configured (and non-empty)'.format(
                                            CLASS_PRINTNAME), flush=True)
                    raise ValueError('Locutus {0}.Setup() ERROR: LOCUTUS_WORKSPACES_ENABLE set, but LOCUTUS_WORKSPACE_NAME *not* configured (and non-empty)'.format(
                                        CLASS_PRINTNAME))
                else:
                    # Workspace enabled & defined; update the 3x ONPREM_DICOM_*_TABLEs:
                    #LOCUTUS_ONPREM_DICOM_STATUS_TABLE
                    #LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
                    #LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE
                    workspace_prefix = '{0}{1}'.format(WS_LOCUTUS_ONPREM_TABLE_PREFIX, self.locutus_settings.LOCUTUS_WORKSPACE_NAME)

                    print('{0}.Setup(): LOCUTUS_WORKSPACES_ENABLE enabled; updating ONPREM_DICOM tables to use '\
                                        'prefix of \'{1}\'...'.format(
                                            CLASS_PRINTNAME,
                                            workspace_prefix), flush=True)

                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE   = '{0}{1}'.format(workspace_prefix, WS_LOCUTUS_ONPREM_STATUS_TABLE_SUFFIX)
                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE = '{0}{1}'.format(workspace_prefix, WS_LOCUTUS_ONPREM_MANIFEST_TABLE_SUFFIX)
                    LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE = '{0}{1}'.format(workspace_prefix, WS_LOCUTUS_ONPREM_INTCFGS_TABLE_SUFFIX)

                    # and, emit the generated workspace table names to the CFG_OUT:
                    print('{0},summarizer-onprem-dicom-workspace,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_STATUS_TABLE", LOCUTUS_ONPREM_DICOM_STATUS_TABLE), flush=True)
                    print('{0},summarizer-onprem-dicom-workspace,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE", LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE), flush=True)
                    print('{0},summarizer-onprem-dicom-workspace,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE", LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE), flush=True)
            self.status_table = LOCUTUS_ONPREM_DICOM_STATUS_TABLE
            self.status_target_field = LOCUTUS_ONPREM_DICOM_STATUS_TARGET_FIELD
            self.manifest_table = LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
            self.status_deid_qc_status_field = LOCUTUS_ONPREM_DICOM_STATUS_DEID_QC_STATUS_FIELD
            self.status_deid_qc_explorer_field = LOCUTUS_ONPREM_DICOM_STATUS_DEID_QC_EXPLORER_FIELD
            print('Setting up to summarize status table `{0}` for module `{1}`... '.format(
                    self.status_table, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE), flush=True)
        else:
            print('{0}.Setup(): ERROR: LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE setting value of (\'{1}\') '\
                                'does NOT match any of the expected (\'{2}\')!'.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE,
                                src_modules.settings.DICOM_MODULE_ONPREM),
                                flush=True)
            raise ValueError('Locutus {0}.Setup() LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE setting value of (\'{1}\') '\
                                'does NOT match any of the expected (\'{2}\')!'.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE,
                                src_modules.settings.DICOM_MODULE_ONPREM))

        if (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS \
            or self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS):

            # Need to pull up the stager_vault_config_path
            # to find orthanc_hostname, orthanc_portnum
            self.trig_secrets = trig_secrets
            self.hvac_client = trig_secrets.get_hvac_client()

            stager_input_yaml_str = self.hvac_client.read(stager_vault_config_path)['data']['value']
            self.stager_local_dicom_config = yaml.safe_load(stager_input_yaml_str)
            self.orthanc_hostname = self.stager_local_dicom_config['orthanc_hostname']
            self.orthanc_portnum = self.stager_local_dicom_config['orthanc_portnum']

            stager_db_use_dev_suffix = self.stager_local_dicom_config['dicom_stage_DB_use_dev_suffix']
            stager_db_vault_path = self.stager_local_dicom_config['dicom_stage_DB_vault_path']

            self.orthanc_url = 'http://%s:%d' % (self.orthanc_hostname,
                                    int(self.orthanc_portnum))

            # TODO: REFACTOR all this into a core Locutus Library
            # Dicom Target Stage Database credentials for EIG Data Warehouse via Vault:

            stager_db_vault_path_db_name = stager_db_vault_path + '/db_name'
            stager_db_name = self.hvac_client.read(stager_db_vault_path_db_name)['data']['value']

            stager_db_vault_path_db_host = stager_db_vault_path + '/db_host'
            stager_db_host = self.hvac_client.read(stager_db_vault_path_db_host)['data']['value']

            # NOTE: convert the above FQDN db_host into a format that TrigSecrets can understand, namely:
            trig_secrets_dw_host = 'UNKNOWN'
            if stager_db_host == 'eigdw.research.chop.edu':
                trig_secrets_dw_host = 'production'
            elif stager_db_host == 'eigdwdev.research.chop.edu':
                trig_secrets_dw_host = 'dev'
            # TODO: add else: exception

            """
            # NOTE: TrigSecrets now takes care of the following:
            stager_db_vault_path_db_user = stager_db_vault_path + '/user'
            stager_db_user = self.hvac_client.read(stager_db_vault_path_db_name)['data']['value']
            stager_db_vault_path_db_password = stager_db_vault_path + '/password'
            stager_db_password = self.hvac_client.read(stager_db_vault_path_db_name)['data']['value']
            """

            # Furthermore, for Orthanc access,
            # when either queried by LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS
            # or when Deleting by LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS:
            ############################################
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS \
                or self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.Setup(): authenticating with Summarizer\'s local Orthanc server to show/resolve_multiuuids...'.format(CLASS_PRINTNAME), flush=True)
                # NOTE: no need to retain orthanc_user & orthanc_password, so just use them directly:
                RestToolbox.SetCredentials(self.stager_local_dicom_config['orthanc_user'],
                                        self.stager_local_dicom_config['orthanc_password'])

            # Furthermore, for the Stager DB,
            # when LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS:
            ############################################
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS:
                self.stage_table = STAGING_DICOM_STABLE_STUDIES_TABLE
                print('{0}.Setup(): Adding stage_table={0} to support LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS, generate DB credentials from TrigSecrets using [TBD...]'.format(self.stage_table), flush=True)

                print('{0}.Setup(): About to generate DB credentials from TrigSecrets using vault_db_name={1}, dw_host={2}, use_dev_suffix={3}'.format(
                                    CLASS_PRINTNAME,
                                    stager_db_name,
                                    self.locutus_settings.trig_secrets_dw_host,
                                    stager_db_use_dev_suffix),
                                    flush=True)
                self.stager_target_db = self.locutus_settings.trig_secrets.get_db_credentials(
                                                        vault_db_name=stager_db_name,
                                                        dw_host=self.locutus_settings.trig_secrets_dw_host,
                                                        dev=stager_db_use_dev_suffix)

                # Setup the connection engine for OnPrem's local DICOM staging DB connection string
                StagerDBengine = create_engine(self.stager_target_db)
                self.StagerDBconn = StagerDBengine.connect()
                StagerSession = sessionmaker(bind=self.StagerDBconn, autocommit=True)
                self.StagerDBconnSession = StagerSession()

        # NOTE: DB connection now needs to be set PRIOR to the BATCH/CSV opening section.
        # Setup the connection engine for the Locutus DB connection string
        #####
        #print('DEBUG: about to create SQL engine for "{0}"'.format(self.locutus_settings.locutus_target_db), flush=True)
        print('DEBUG: about to create SQL engine', flush=True)
        #####
        LocutusDBengine = create_engine(self.locutus_settings.locutus_target_db)
        self.LocutusDBconn = LocutusDBengine.connect()
        Session = sessionmaker(bind=self.LocutusDBconn, autocommit=True)
        self.LocutusDBconnSession = Session()
        # NOTE: TODO: if the above new LocutusDBconnSession approach works
        # for preventing unexpected timeouts, then introduce this to
        # the other Locutus modules as well, complete with all the .close() calls.
        ############################################


        # BATCHES NOTE: the Preloader can still use BATCH_NAME to load up a batch (even NULL!),
        # even w/o LOCUTUS_BATCHES_ENABLE (now LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB):
        if not self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
            ###################################
            # START OF: setup DICOM Summarize Status input manifest CSV:
            print('{0}.Setup(): Opening Locutus DICOM Summarizer CSV = {1}!'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_MANIFEST_CSV),
                                    flush=True)
            # NOTE:MANIFEST_READER=part0ab-ALT-CSV:
            self.locutus_settings.open_manifest_CSV(self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_MANIFEST_CSV)

            # ensure that the input CSV headers are as expected:
            # NOTE:MANIFEST_READER=part1a-ALT-CSV-headers:
            csv_headings_row = self.locutus_settings.get_next_nontrivial_row_via_manifest_CSV()

            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Setup(): parsing input manifest header line of "{1}"'.format(CLASS_PRINTNAME, csv_headings_row), flush=True)

            #########################
            # TODO: rather than hardcoding the following CSV field #s, consider creating a dictionary:
            # TODO: consider eventually introducing and checking for its own expected header of MANIFEST_HEADER_MANIFEST_VER
            # NOTE: but for now, allow an easier copy of just the ACCESSION_NUM from another working manifest.
            #########################
            # NOTE: as part of being able to generate curr_fw_session_import_arg from a manifest,
            # do need to begin introducing optional manifest input reading elements,
            # with a bit more robust handling of the expected input columns:
            ###################################
            # Default case for accession_num only, as the first column:
            csv_header0 = csv_headings_row[0].strip().upper() if (len(csv_headings_row) >= 1) else ''
            ###################################
            # NOTE: keeping the potential version header field as lower case, to highlight it as a data-less version header
            # First, setup for the appropriate module-specific manifest format (decoupled from the test to later reference, if not used):
            expected_header_ver = ''
            expected_resolve_header_ver = ''
            if (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM):
                # look for the ONPREM_MANIFEST_HEADER_MANIFEST_VER at ONPREM_MANIFEST_VER_COLUMN_OFFSET:
                expected_header_ver = src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER
                expected_resolve_header_ver = src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER_to_RESOLVE_MULTIUUIDS
                if len(csv_headings_row) < src_modules.settings.ONPREM_MANIFEST_NUM_HEADERS:
                    # Not enough columns for this module's expected format, setting to the simple version:
                    self.locutus_settings.manifest_header_ver = src_modules.settings.SIMPLIFIED_MANIFEST_HEADER_MANIFEST_VER
                    print('{0}.Setup(): WARNING: input manifest for Locutus module {1} format \'{2}\' expects {3} columns, but this manifest only has {4}; treating as {5}!'.format(
                                    CLASS_PRINTNAME,
                                    src_modules.settings.DICOM_MODULE_ONPREM,
                                    expected_header_ver,
                                    src_modules.settings.ONPREM_MANIFEST_NUM_HEADERS,
                                    len(csv_headings_row),
                                    src_modules.settings.MANIFEST_TYPE_DEFAULT_SIMPLIFIED),
                                    flush=True)
                else:
                    self.locutus_settings.manifest_header_ver = csv_headings_row[src_modules.settings.ONPREM_MANIFEST_VER_COLUMN_OFFSET].strip().lower() if (len(csv_headings_row) >= src_modules.settings.ONPREM_MANIFEST_VER_COLUMN_OFFSET) else ''
                    print('{0}.Setup(): Looking for Locutus module {1} input Manifest format = \'{2}\', column {3} has value = \'{4}\'!'.format(
                                    CLASS_PRINTNAME,
                                    src_modules.settings.DICOM_MODULE_ONPREM,
                                    expected_header_ver,
                                    src_modules.settings.ONPREM_MANIFEST_VER_COLUMN_OFFSET,
                                    self.locutus_settings.input_manifest_version),
                                    flush=True)

            print('**DEBUG**: Just loaded headers for MODULE=\'{0}\' (Q: ~=DICOM_MODULE_ONPREM == {1}), '\
                'seeing: manifest_header_ver=\'{2}\' (Q: ~=ONPREM_MANIFEST_HEADER_MANIFEST_VER == {3} )...'.format(
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper(),
                    (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM),
                    self.locutus_settings.manifest_header_ver,
                    (self.locutus_settings.manifest_header_ver == src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER)),
                    flush=True)

            # Next, check against the optional module-specific manifest format:
            if (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM \
                and self.locutus_settings.manifest_header_ver == src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER):
                # DICOM_MODULE_ONPREM manifest format:
                print('{0}.Setup(): Using Locutus module {1} input Manifest format = {2}!'.format(
                                    CLASS_PRINTNAME,
                                    src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER,
                                    src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER),
                                    flush=True)
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS:
                    # NOTE: throw an ERROR if resolve_multiuuids was set, but a non-Resolver manifest was included:
                    print('{0}.Setup(): ERROR: LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS set, but '\
                                    'Locutus input Manifest version \'{1}\' supplied; '\
                                    '\n==========> TIP: Please use Locutus multi-UUID Resolver input Manifest version \'{2}\''.format(
                                    CLASS_PRINTNAME,
                                    src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER,
                                    src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER_to_RESOLVE_MULTIUUIDS),
                                    flush=True)
                    raise ValueError('LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS set, but '\
                                    'Locutus input Manifest version \'{1}\' supplied; '\
                                    '\n==========> TIP: Please use Locutus multi-UUID Resolver input Manifest version \'{2}\''.format(
                                    CLASS_PRINTNAME,
                                    src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER,
                                    src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER_to_RESOLVE_MULTIUUIDS) )
            elif (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM \
                and self.locutus_settings.manifest_header_ver == src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER_to_RESOLVE_MULTIUUIDS):
                # DICOM_MODULE_ONPREM manifest format:
                print('{0}.Setup(): Using Locutus module {1} multi-UUID Resolver input Manifest format = {2}!'.format(
                                    CLASS_PRINTNAME,
                                    src_modules.settings.DICOM_MODULE_ONPREM,
                                    src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER),
                                    flush=True)
            #
            ###################################
            # NOTE: ensure that resolve_multiuuids ALSO lists the resolve_uuids manifest_ver!
            # with support for OnPrem:
            #
            elif ((len(csv_headings_row) < src_modules.settings.MANIFEST_NUM_HEADERS) or
                (csv_header0 != src_modules.settings.MANIFEST_HEADER_ACCESSION_NUM)):
                print('{0}.Setup(): ERROR: Locutus input Manifest CSV headers (\'{1}\') '\
                                    'for {2} do NOT match those expected (\'{3}\') for the \'{4}\' format, '\
                                    'nor for the {5} module-specific versions (\'{6}\' or \'{7}\').'\
                                    '\n==========> TIP: Please double-check your manifest format '\
                                    'AND module selection Jenkins parameter'.format(
                                    CLASS_PRINTNAME,
                                    csv_headings_row[0].strip().upper(),
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_MANIFEST_CSV,
                                    src_modules.settings.MANIFEST_HEADER_ACCESSION_NUM,
                                    src_modules.settings.MANIFEST_TYPE_DEFAULT_SIMPLIFIED,
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper(),
                                    expected_header_ver,
                                    expected_resolve_header_ver),
                                    flush=True)
                raise ValueError('Locutus {0}.Setup() input Manifest CSV headers (\'{1}\') '\
                                    'for {2} do NOT match those expected (\'{3}\') for the \'{4}\' format, '\
                                    'nor for the {5} module-specific version (\'{6}\' or \'{7}\').'\
                                    '\n==========> TIP: Please double-check your manifest format '\
                                    'AND module selection Jenkins parameter'.format(
                                    CLASS_PRINTNAME,
                                    csv_headings_row[0].strip().upper(),
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_MANIFEST_CSV,
                                    src_modules.settings.MANIFEST_HEADER_ACCESSION_NUM,
                                    src_modules.settings.MANIFEST_TYPE_DEFAULT_SIMPLIFIED,
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper(),
                                    expected_header_ver,
                                    expected_resolve_header_ver) )
            print('{0}.Setup(): Using Locutus input MRI Manifest CSV = {1}!'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_MANIFEST_CSV),
                                    flush=True)

            # NOTE: await the manifest reading loop in Process() to write out the MANIFEST_OUTPUT_PREFIX headers
            # END of: setup DICOM Summarize Status input manifest CSV:
            ###################################
        else:
            # i.e., if self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
            # BATCHES NOTE: alternate Setup() approach for manifest-once when NOT Preloading
            # ===> update the above if not LOCUTUS_BATCHES_ENABLE/LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB to ALSO ensure NOT Preloading.

            print('{0}.Setup(): Opening Locutus DICOM Summarizer using DB BATCH \'{1}\' rather than a CSV!'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_BATCH_NAME),
                                    flush=True)

            # BATCHES NOTE: for manifest-once Batch query in the Workspace via SQL:
            # TODO: eventually add filtering to open_batch_cursor(), but first pass is for the entire batch:
            # NOTE:MANIFEST_READER=part0ab-ALT-DB:
            self.locutus_settings.open_batch_cursor(self.LocutusDBconnSession, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE, self.manifest_table, self.locutus_settings.LOCUTUS_BATCH_NAME)

        # NOTE: allow the Preloader to create the applicable module's DB tables, if enabled for DB updates:
        print("{0}.Setup(): pre-Preloader Setup check for Enable DB Updates".format(
                CLASS_PRINTNAME), flush=True)
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST \
        and self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            print("{0}.Setup(): pre-Preloader Setup FOUND Enable DB Updates : True".format(
                    CLASS_PRINTNAME), flush=True)
            if (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM):
                onprem_dicom = OnPrem_Dicom(self.locutus_settings)
                # NOT: (onprem_dicom_src_orthanc_config, onprem_dicom_qc_orthanc_config) = onprem_dicom.Setup(trig_secrets, self.locutus_settings.ONPREM_DICOM_STAGE_VAULT_PATH)
                # INSTEAD, pass on:
                onprem_dicom.create_db_tables(self.LocutusDBconnSession, LOCUTUS_ONPREM_DICOM_STATUS_TABLE, LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE, LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE)

        # no config to return:
        return ""
        # end of Setup()


    def update_accession_manifest_status_across_all_workspaces(self, accession_num, new_manifest_status):
        # NOTE: see also SAFETY CHECK in the Settings Setup(),
        #   to restrict process_Summarizer+Resolver to bypass_CSV False
        #   since need for RESOLVE_CMDS limits Resolver to a Resolver CSV.
        #####
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # # # # # # # # # # # # # # # # # # # # # #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.update_accession_manifest_status_across_all_workspaces(accession=\'{2}\', new_manifest_status=\'{3}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num, new_manifest_status), flush=True)
            # SQL blank line before all this method's umbrella SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)
        return_success = True

        #####
        # open cursor to AAA_LOCUTUS_BATCHES:
        #   == SELECT DISTINCT(module, workspace) FROM aaa_locutus_batches WHERE active ORDER BY module, workspace
        #####
        # BATCHES NOTE: querying all modules and workspaces for this accession; requires that
        # all formerly loaded MANIFEST records be added to LOCUTUS_ALL_BATCHES_TABLE retroactively:
        acc_module_workspaces_sql_select='SELECT DISTINCT(module, workspace) as modspace '\
                                'FROM {0} '\
                                'WHERE accession_num=\'{1}\' '\
                                'AND active '\
                                'ORDER BY (module, workspace) ;'.format(
                                self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE,
                                accession_num)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, acc_module_workspaces_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that prepdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        acc_module_workspaces_results =  self.LocutusDBconnSession.execute(acc_module_workspaces_sql_select)
        for acc_module_workspaces_row in acc_module_workspaces_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, acc_module_workspaces_row), flush=True)
                # - - - - - - -START of each module+workspace - - - - - - -
                print('{0}-- - - - - - - - - - - - - - - - - - - - - - -'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            ###################
            curr_acc_module = ''
            curr_acc_workspace = ''
            # NOTE: SHOULD USE a macro for conversion to WORKSPACE tables, but here we are:
            curr_acc_module_workspace_manifest_table = ''

            if acc_module_workspaces_row:
                #NOT: curr_acc_module = acc_module_workspaces_row['module']
                #NOT: curr_acc_workspace = acc_module_workspaces_row['workspace']
                #####
                # the above two ^^ are combined with the above DISTINCT(module, workspace):
                curr_acc_module_moduleworkspace = acc_module_workspaces_row['modspace']
                # and extract em from "(module,workspace)", replacing both '(' and ')' with '':
                curr_acc_module = curr_acc_module_moduleworkspace.split(',')[0].replace('(','')
                curr_acc_workspace = curr_acc_module_moduleworkspace.split(',')[1].replace(')','')

                # set default table start as ONPREM's default (no workspace):
                curr_acc_module_workspace_manifest_table = DEF_LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
                # NOTE: module will have been set by Preloader/etc as .upper():
                # NOTE: currently running with only ONPREM modules:
                if curr_acc_module == 'ONPREM' and curr_acc_workspace and len(curr_acc_workspace):
                    # non-empty workspace, go ahead and build up its manifest table name:
                    curr_acc_module_workspace_manifest_table = WS_LOCUTUS_ONPREM_TABLE_PREFIX + \
                                                    curr_acc_workspace + WS_LOCUTUS_ONPREM_MANIFEST_TABLE_SUFFIX
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.update_accession_manifest_status_across_all_workspaces(): '\
                                    'Looking at accession=`{1}`s next Module/Workspace: '\
                                    'DICOM module=`{2}`, workspace=`{3}`, table=`{4}`'.format(
                                    CLASS_PRINTNAME,
                                    accession_num,
                                    curr_acc_module,
                                    curr_acc_workspace,
                                    curr_acc_module_workspace_manifest_table),
                                    flush=True)
                # NOTE: TODO: check on DRYRUN before any UPDATES
                #####
                # PART 01 of UPDATE: the accession's rows in its home MODULE + WORKSPACE MANIFEST table:
                #   print UPDATING TABLE
                #   UPDATE TABLE SET manifest_status=${MANIFEST_STATUS}, \
                #           last_datetime_processed=now()
                #       WHERE accession_num=${ACCESSION_NUM} AND active
                #####
                # FIRST, a commented SELECT version of the actual UPDATE command:
                # BATCHES NOTE: retiring for this specific batch_clause:
                preupdate_sql_select='SELECT accession_num, batch_name, '\
                                                'manifest_status, \'{0}\' as proposed_manifest_status, '\
                                                'now() as proposed_last_datetime_processed, '\
                                                'active '\
                                                'FROM {1} WHERE {2} '\
                                                'AND active ORDER BY batch_name ;'.format(
                                                new_manifest_status,
                                                curr_acc_module_workspace_manifest_table,
                                                accession_range_where)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
                    print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
                ###################
                # DO IT! Go ahead and actually execute that preupdate_sql_select
                # in order to preserve those values in the output log (as well as in the updated accession records)
                preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
                for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
                ###################
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
                # and NOW, the actual UPDATE:
                update_sql='UPDATE {0} '\
                                                'SET manifest_status=\'{1}\', '\
                                                'last_datetime_processed=now() '\
                                                'WHERE {2} '\
                                                'AND active ;'.format(
                                                curr_acc_module_workspace_manifest_table,
                                                new_manifest_status,
                                                accession_range_where)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
                #
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.update_accession_manifest_status_across_all_workspaces(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                    self.LocutusDBconnSession.execute(update_sql)
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.update_accession_manifest_status_across_all_workspaces(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    # SQL blank line at end of all this method's SQL:
                    print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
                #

                #####
                # PART 02 of UPDATE: the accession's rows in the AAA_LOCUTUS_BATCHES table:
                #   print UPDATING aaa_locutus_batches
                #   UPDATE aaa_locutus_batches SET manifest_status=${MANIFEST_STATUS}, \
                #           last_datetime_processed=now()
                #       WHERE accession_num=${ACCESSION_NUM} AND active
                #####
                # FIRST, a commented SELECT version of the actual UPDATE command:
                # BATCHES NOTE: retiring for this specific batch_clause:
                preupdate_sql_select='SELECT accession_num, batch_name, '\
                                                'manifest_status, \'{0}\' as proposed_manifest_status, '\
                                                'now() as proposed_last_datetime_processed, '\
                                                'active '\
                                                'FROM {1} WHERE {2} '\
                                                'AND module=\'{3}\' AND workspace=\'{4}\' '\
                                                'AND active ORDER BY batch_name;'.format(
                                                new_manifest_status,
                                                self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE,
                                                accession_range_where,
                                                curr_acc_module,
                                                curr_acc_workspace)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
                    print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
                ###################
                # DO IT! Go ahead and actually execute that preupdate_sql_select
                # in order to preserve those values in the output log (as well as in the updated accession records)
                preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
                for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
                ###################
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
                # and NOW, the actual UPDATE:
                update_sql='UPDATE {0} '\
                                                'SET manifest_status=\'{1}\', '\
                                                'last_datetime_processed=now() '\
                                                'WHERE {2} '\
                                                'AND module=\'{3}\' AND workspace=\'{4}\' '\
                                                'AND active ;'.format(
                                                self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE,
                                                new_manifest_status,
                                                accession_range_where,
                                                curr_acc_module,
                                                curr_acc_workspace)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
                #
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.update_accession_manifest_status_across_all_workspaces(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                    self.LocutusDBconnSession.execute(update_sql)
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.update_accession_manifest_status_across_all_workspaces(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                #
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    # - - - - - - -END of each module+workspace - - - - - - -
                    print('{0}-- - - - - - - - - - - - - - - - - - - - - - -'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    # SQL blank line at end of all this method's SQL:
                    print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)

        #####
        # That's all, folx!
        #####
        if self.locutus_settings.LOCUTUS_VERBOSE:
            # end of all this method's umbrella SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
            print('{0}-- # END of {1}.update_accession_manifest_status_across_all_workspaces(accession=\'{2}\', new_manifest_status=\'{3}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num, new_manifest_status), flush=True)
            print('{0}-- # # # # # # # # # # # # # # # # # # # # # #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        return return_success
        # end of update_accession_manifest_status_across_all_workspaces()


    def expand_multiuuids_to_manifest_output(self, multiuuid_accessions_shown, accession_num, accession_duplicated, manifest_status, batch_clause='', commented=True, orthanc_comment=''):
        comment = ''
        if commented:
            comment = '#'
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: expand_multiuuids_to_manifest_output(): YAYYY Hello!!! Considering accession=\'{0}\''.format(accession_num), flush=True)

        # 1. Check: if accession_num not already in self.multiuuid_accessions_shown[]
        # 2. SELECT * from MANIFEST & STATUS where accession_num={accession_num}

        ####################
        # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
        # NOTE: Q: NO need to even cast these into floats anymore, riiiiight? right.
        ####################
        # Q: will we still want to include any sub/whole statistics?
        # especially w/ the few remaining SPLIT accessions no longer as status quo
        # NOTE: for now, see how the above fix rolls on through to the stats.
        ####################

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE: expand_multiuuids_to_manifest_output() about check if accession_num(=\'{0}\') in multiuuid_accessions_shown'.format(
                    accession_num), flush=True)
            if multiuuid_accessions_shown is not None:
                print('VERBOSE: expand_multiuuids_to_manifest_output() len(multiuuid_accessions_shown)={0}'.format(
                        len(multiuuid_accessions_shown)), flush=True)

        if (multiuuid_accessions_shown is None) or (accession_num  not in multiuuid_accessions_shown):
            # NOTE: emit the MANIFEST_OUTPUT ##### header BEFORE and AFTER the multi-uuids:
            # NOTE: ALSO have the caller emit another of the same row of hashes to FOLLOW the main accession, if showing multiuuids:
            print('{0},#################################################'.format(MANIFEST_OUTPUT_PREFIX), flush=True)
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0},# LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS expanding accession=\'{1}\' with manifest_status={2} expanding:'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    accession_num,
                    manifest_status), flush=True)
            # not yet in multiuuid_accessions_shown, add it now (seemingly working even though a new multiuuid_accessions_shown=[] evaluates as None)
            multiuuid_accessions_shown.append(accession_num)
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.expand_multiuuids_to_manifest_output(): Looking for MANIFEST w/ accession=`{1}` within DICOM module `{2}` in table `{3}`'.format(
                                    CLASS_PRINTNAME,
                                    accession_num,
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE,
                                    self.manifest_table),
                                    flush=True)

            # SELECT * to support the differing fields of different manifest tables:
            # BATCHES NOTE: now includes batch_clause w/ potential batch_name in the following SELECT for expand_multiuuids_to_manifest_output()
            manifest_status_result = self.LocutusDBconnSession.execute('SELECT * '\
                                    'FROM {0} '\
                                    'WHERE accession_num=\'{1}\' '\
                                    'AND active {2} '\
                                    'ORDER BY accession_num ;'.format(
                                    self.manifest_table,
                                    accession_num,
                                    batch_clause))
            for manifest_status_row in manifest_status_result.fetchall():
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.expand_multiuuids_to_manifest_output(): Looking at next MANIFEST record w/ accession=`{1}` within DICOM module `{2}` in table `{3}`'.format(
                                    CLASS_PRINTNAME,
                                    accession_num,
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE,
                                    self.manifest_table),
                                    flush=True)
                    print('{0}-- # Locutus MANIFEST TABLE:{1}'.format(self.locutus_settings.SQL_OUT_PREFIX, manifest_status_row), flush=True)
                    print('VERBOSE: expand_multiuuids_to_manifest_output(accession_num=\'{0}\') found manifest_status_row={1}'.format(
                            accession_num, manifest_status_row), flush=True)
                manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND

                status_targets = ''
                status_target = ''
                status_deid_datetime = ''
                status_deid_qc_status = ''
                status_deid_qc_explorer = ''
                if manifest_status_row:
                    manifest_row_accession = manifest_status_row['accession_num']
                    manifest_status = manifest_status_row['manifest_status']
                    status_deid_datetime = manifest_status_row['last_datetime_processed']
                    # and for this non-empty manifest row, find its corresponding status row's output target:
                    module_xtra_fields_msg = ''
                    module_xtra_fields_cols = ''
                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM:
                        # DICOM_MODULE_ONPREM::
                        # NOTE: preface w/ commas for since these are optional strings:
                        module_xtra_fields_msg = ', deid_qc_status field \'{0}\', and deid_qc_explorer field \'{1}\''.format(
                                                    self.status_deid_qc_status_field,
                                                    self.status_deid_qc_explorer_field)
                        module_xtra_fields_cols = ', {0}, {1}'.format(
                                                    self.status_deid_qc_status_field,
                                                    self.status_deid_qc_explorer_field)

                    # NOTE: assemble the extra deid_qc_* fields ONLY as an option for module ONPREM
                    # NOTE: no column before {5} in case its optional fields are empty:
                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.expand_multiuuids_to_manifest_output(): Looking for STATUS of accession=={1} within DICOM module `{2}` in table `{3}` '\
                                'for output target field `{4}`{5}'.format(
                                            CLASS_PRINTNAME,
                                            accession_num,
                                            self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE,
                                            self.status_table,
                                            self.status_target_field,
                                            module_xtra_fields_msg),
                                            flush=True)

                    # BATCHES NOTE: now includes batch_clause w/ potential batch_name in the following SELECT for expand_multiuuids_to_manifest_output()
                    status_target_result = self.LocutusDBconnSession.execute('SELECT uuid,{0}{1} '\
                                            'FROM {2} '\
                                            'WHERE accession_num=\'{3}\' AND (change_seq_id>0) '\
                                            'AND active {4} ;'.format(
                                            self.status_target_field,
                                            module_xtra_fields_cols,
                                            self.status_table,
                                            manifest_row_accession,
                                            batch_clause))
                    # NOTE: the specific status for this manifest of a fetchall
                    # NOW honoring the possibility of a MULTIUUID accession, namely 1x manifest record w/ >=2x status records:

                    this_orthanc_uuid_num = 0
                    status_target_results = status_target_result.fetchall()
                    num_orthanc_uuids = len(status_target_results)
                    for status_target_row in status_target_results:
                        if status_target_row:
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('{0}-- # Locutus STATUS TABLE: {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, status_target_row), flush=True)
                                print('VERBOSE: expand_multiuuids_to_manifest_output(accession_num=\'{0}\') found status_target_row={1}'.format(
                                    accession_num, status_target_row), flush=True)

                            status_targets = status_target_row[self.status_target_field]
                            status_uuid = status_target_row['uuid']
                            this_orthanc_uuid_num += 1
                            # retrieve the most recent target, last, of any comma-delimited targets:
                            if status_targets:
                                status_target = status_targets.split(',')[-1]
                            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM:
                                status_deid_qc_status = status_target_row[self.status_deid_qc_status_field]
                                status_deid_qc_explorer = status_target_row[self.status_deid_qc_explorer_field]

                            ###################################################################################
                            # Add a MANIFEST_OUTPUT for each of these related multi-uuid whole/sub-accessions:
                            # Recycle former MANIFEST_OUTPUT emitting code, looking for all comments such as:
                            ## TODO: create a method for generating such module_specific_manifest_columns for MANIFEST_TYPE_DEFAULT_SIMPLIFIED.
                            ## especially now that expand_multiuuids_to_manifest_output() is repurposing them:
                            #
                            ## TODO: create a method for generating such module_specific_manifest_columns for ONPREM_MANIFEST_TYPE.
                            ## especially now that expand_multiuuids_to_manifest_output() is repurposing them:
                            ###################################################################################
                            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS:
                                preshow_comment='#'
                            # TODO: create a method for the ^C/^V module_specific_manifest_columns (&, eventually module_specific_xtra_columns) bits:
                            module_specific_manifest_columns = ''

                            # BATCHES NOTE: even w/ LOCUTUS_BATCHES_ENABLE/LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB, input_manifest_type has been set for this output by Setup():
                            if self.locutus_settings.input_manifest_type == src_modules.settings.MANIFEST_TYPE_DEFAULT_SIMPLIFIED:
                                # SIMPLIFIED:
                                # MANIFEST_HEADER_ACCESSION_NUM, SIMPLIFIED_MANIFEST_HEADER_MANIFEST_VER)
                                module_specific_manifest_columns = '{0},{1}'.format(
                                                            manifest_row_accession,
                                                            '')
                            elif self.locutus_settings.input_manifest_type == src_modules.settings.ONPREM_MANIFEST_TYPE:
                                # ONPREM-specific versioned manifest:
                                # ONPREM_MANIFEST_HEADER_SUBJECT_ID, ONPREM_MANIFEST_HEADER_OBJECT_INFO_01, ONPREM_MANIFEST_HEADER_OBJECT_INFO_02, ONPREM_MANIFEST_HEADER_OBJECT_INFO_03,
                                #   + ONPREM_MANIFEST_HEADER_ACCESSION_NUM, ONPREM_MANIFEST_HEADER_MANIFEST_VER)
                                ##################
                                # BATCH NOTE / DEV NOTE:
                                # DEV TODO: Q: should the following be replaced by their DB names, e.g., ONPREM_BATCH_VIA_DB_HEADER_OBJECT_INFO_01?
                                #   =====> OR left entirely as is?  Since the UPPERCASE ones were used for the query, will it impact these, if so used?
                                module_specific_manifest_columns = '{0},{1},{2},{3},{4},{5}'.format(
                                                            manifest_status_row['subject_id'],
                                                            manifest_status_row['object_info_01'],
                                                            manifest_status_row['object_info_02'],
                                                            manifest_status_row['object_info_03'],
                                                            manifest_row_accession,
                                                            '')
                            ######################
                            # and immediately BEFORE the below vvv MANIFEST_OUTPUT for each multi-uuid,
                            # query & include some ORTHANC_OUT clues for a subsequent Orthanc query tool,
                            # much like those clues emitted by retire_accession_stager_only() to DELETE from ORTHANC:
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print(f'{ORTHANC_OUT_PREFIX} ####################', flush=True)
                                # TODO: add subject_id, object_info01, object_info02, object_info03 (OnPrem) to this:
                                print(f'{ORTHANC_OUT_PREFIX} # INSPECTING in ORTHANC {orthanc_comment}: accession_num=\'{accession_num}\':', flush=True)
                            #########
                            this_orthanc_uuid = status_uuid
                            ################################################
                            # TODO: adding ORTHANCINTPROD, via the following...
                            # NOTE: https://orthanc.uclouvain.be/book/users/rest.html#browsing-from-the-patient-down-to-the-instance
                            # $ curl http://localhost:8042/studies/9ad2b0da-[EXAMPLE-UUID]-78d12c15
                            ################################################
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>', flush=True)
                                print(f'{ORTHANC_OUT_PREFIX} echo "INSPECT in ORTHANC {orthanc_comment}: accession_num=\'{accession_num}\', uuid {this_orthanc_uuid_num} of {num_orthanc_uuids}..."', flush=True)
                            VAR_ORTHANC_URL = "${ORTHANC_URL}"
                            VAR_ORTHANC_USER = "${ORTHANC_USER}"
                            VAR_ORTHANC_PASS = "${ORTHANC_PASS}"
                            for_shell_staged_study_url =  f'{VAR_ORTHANC_URL}/studies/{this_orthanc_uuid}'
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print(f'{ORTHANC_OUT_PREFIX} curl -v -u {VAR_ORTHANC_USER}:{VAR_ORTHANC_PASS}  {for_shell_staged_study_url}', flush=True)
                            # NOTE: and also internal_staged_study_url for reference, but not really needed in the output,
                            internal_staged_study_url = f'{self.orthanc_url}/studies/{this_orthanc_uuid}'
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>', flush=True)
                                print(f'{ORTHANC_OUT_PREFIX} ####################', flush=True)
                            # ONLY emit the MANIFEST_OUTPUT ##### header BEFORE and AFTER the multi-uuids:
                            ######################
                            # TODO: define a new method/subroutine for the following, as now also mod'd for retire_accession_stager_only():
                            orthanc_study_clues = ''
                            orthanc_study_data = RestToolbox.DoGet(internal_staged_study_url)

                            orthanc_study_maintags = None
                            orthanc_study_maintags = orthanc_study_data.get('MainDicomTags')
                            # NOTE: REDACT for production logging:
                            #print('DEBUG: study MainDicomTags looks like.... {0}'.format(orthanc_study_maintags), flush=True)

                            # Put the StudyDate first, as it is when all was acquired:
                            # InstitutionName:
                            orthanc_study_clues += ',StudyDate:,'
                            if orthanc_study_maintags:
                                orthanc_study_date = orthanc_study_maintags.get('StudyDate')
                                if orthanc_study_date:
                                    orthanc_study_clues += orthanc_study_date

                            # LastUpdate:
                            orthanc_study_clues += ',LastUpdate:,'
                            orthanc_study_lastupdate = orthanc_study_data.get('LastUpdate')
                            if orthanc_study_lastupdate:
                                orthanc_study_clues += orthanc_study_lastupdate

                            # InstitutionName:
                            orthanc_study_clues += ',InstitutionName:,'
                            if orthanc_study_maintags:
                                orthanc_study_institution = orthanc_study_maintags.get('InstitutionName')
                                if orthanc_study_institution:
                                    orthanc_study_clues += orthanc_study_institution

                            # AccessionNumber:
                            orthanc_study_clues += ',AccessionNumber:,'
                            if orthanc_study_maintags:
                                orthanc_study_accnum = orthanc_study_maintags.get('AccessionNumber')
                                if orthanc_study_accnum:
                                    orthanc_study_clues += orthanc_study_accnum
                            # NOTE: save the other orthanc_study_maintags for AFTER the NumSeries,
                            # since RequestedProcedureDescription is not always present
                            # (and the StudyDescription itself is of variable length,
                            # so it might format better folling the 2 concise numbers, AccNum & NumSeries)

                            # accumulate count for Series
                            orthanc_study_num_series = 0
                            # calculate Num Series:
                            orthanc_study_series_uuids = orthanc_study_data.get('Series')
                            if orthanc_study_series_uuids:
                                orthanc_study_num_series = len(orthanc_study_series_uuids)
                            else:
                                orthanc_study_num_series = 0
                            orthanc_study_clues += f',NumSeries:,{orthanc_study_num_series}'

                            # TODO: consider checking each of the following attribute values for consistency across each of the series in this study.
                            # If they are consistent, then perhaps also show these values in the Study's MANIFEST_OUTPUT line, even if more than 1 series.
                            # Else, can just use a value such as '(many)' or '(mixed)' or....???
                            orthanc_series_description = ''
                            orthanc_series_modality = ''
                            orthanc_series_manufacturer = ''
                            orthanc_series_bodypart = ''
                            orthanc_series_protocol = ''

                            # calculate Num Images:
                            orthanc_study_num_images = 0
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('{0} # STARTING STUDY # images = {1}'.format(
                                        ORTHANC_OUT_PREFIX,
                                        orthanc_study_num_images), flush=True)
                            this_single_instance_uuid = None
                            for this_series_uuid in orthanc_study_series_uuids:
                                internal_staged_series_url = f'http://{self.orthanc_hostname}:{self.orthanc_portnum}/series/{this_series_uuid}'
                                orthanc_series_data = RestToolbox.DoGet(internal_staged_series_url)
                                # consider the series MainDicomTags, including:
                                #   .BodyPartExamined, .ProtocolName, .Manufacturer,
                                #   .PerformedProcedureStepDescription, .SeriesDescription, etc.
                                # accumulate each the series' Instances:
                                if orthanc_series_data:
                                    orthanc_series_num_images = len(orthanc_series_data.get('Instances'))

                                    orthanc_series_maintags = None
                                    orthanc_series_maintags = orthanc_series_data.get('MainDicomTags')

                                    # NOTE: REDACT for production logging:
                                    #print('DEBUG: series MainDicomTags looks like.... {0}'.format(orthanc_series_maintags), flush=True)

                                    if orthanc_series_maintags:
                                        # SeriesDescription:
                                        orthanc_series_number = orthanc_series_maintags.get('SeriesNumber')
                                        orthanc_series_description = orthanc_series_maintags.get('SeriesDescription')
                                        orthanc_series_bodypart = orthanc_series_maintags.get('BodyPartExamined')
                                        orthanc_series_modality = orthanc_series_maintags.get('Modality')
                                        orthanc_series_manufacturer = orthanc_series_maintags.get('Manufacturer')
                                        orthanc_series_protocol = orthanc_series_maintags.get('ProtocolName')
                                        # NOTE: AS ABOVE == orthanc_series_num_images.
                                        if orthanc_series_num_images == 1:
                                            this_single_instance_uuid = orthanc_series_data.get('Instances')[0]

                                    if self.locutus_settings.LOCUTUS_VERBOSE:
                                        print('{0} # images = {1}, + {2} in Series #{3}, SeriesDescription={4}, Protocol={5}, BodyPart={6}, Modality={7}, Manufacturer={8}...'.format(
                                            ORTHANC_OUT_PREFIX,
                                            orthanc_study_num_images, orthanc_series_num_images,
                                            orthanc_series_number, orthanc_series_description, orthanc_series_protocol, orthanc_series_bodypart,
                                            orthanc_series_modality, orthanc_series_manufacturer), flush=True)
                                    orthanc_study_num_images += orthanc_series_num_images

                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('{0} # TOTAL STUDY # images = {1}'.format(
                                        ORTHANC_OUT_PREFIX,
                                        orthanc_study_num_images), flush=True)
                            orthanc_study_clues += f',NumImages:,{orthanc_study_num_images}'

                            # and return to the rest of the maintags:
                            if orthanc_study_maintags:
                                # StudyDescription:
                                orthanc_study_clues += ',StudyDescription:,'
                                orthanc_study_studydesc = orthanc_study_maintags.get('StudyDescription')
                                if orthanc_study_studydesc:
                                    orthanc_study_clues += orthanc_study_studydesc
                                # RequestedProcedureDescription:
                                orthanc_study_clues += ',RequestedProcedureDescription:,'
                                orthanc_study_reqdesc = orthanc_study_maintags.get('RequestedProcedureDescription')
                                if orthanc_study_reqdesc:
                                    orthanc_study_clues += orthanc_study_reqdesc
                                # 1-off SeriesDescriptions (otherwise, too many?)

                                if orthanc_study_num_series==1:
                                    if self.locutus_settings.LOCUTUS_VERBOSE:
                                        print('{0} # orthanc_study_num_series={1}, so ADDING series fields like series description={2}'.format(
                                            ORTHANC_OUT_PREFIX,
                                            orthanc_study_num_series,
                                            orthanc_series_description), flush=True)

                                    #HERE: get the InstanceNumber and AcquisitionNumber
                                    orthanc_series_instance_number = None
                                    orthanc_series_acquisition_number = None
                                    if orthanc_study_num_images==1 and this_single_instance_uuid:
                                        internal_staged_instance_url = f'http://{self.orthanc_hostname}:{self.orthanc_portnum}/instances/{this_single_instance_uuid}'
                                        orthanc_instance_data = RestToolbox.DoGet(internal_staged_instance_url)

                                        orthanc_instance_maintags = None
                                        orthanc_instance_maintags = orthanc_instance_data.get('MainDicomTags')

                                        orthanc_series_instance_number = orthanc_instance_maintags.get('InstanceNumber')
                                        orthanc_series_acquisition_number = orthanc_instance_maintags.get('AcquisitionNumber')

                                    # if one and only one series, do list its description,
                                    # as often the internal Orthanc duplicates are but of 1 series w/ 1 image:
                                    orthanc_study_clues += ',Single Series Details:,'
                                    ##########
                                    orthanc_study_clues += ',SeriesNumber:,'
                                    if orthanc_series_number:
                                        orthanc_study_clues += orthanc_series_number
                                    orthanc_study_clues += ',SeriesDescription:,'
                                    if orthanc_series_description:
                                        orthanc_study_clues += orthanc_series_description
                                    ##########
                                    orthanc_study_clues += ',InstanceNumber:,'
                                    if orthanc_series_instance_number:
                                        orthanc_study_clues += orthanc_series_instance_number
                                    orthanc_study_clues += ',AcquisitionNumber:,'
                                    if orthanc_series_acquisition_number:
                                        orthanc_study_clues += orthanc_series_acquisition_number
                                    ##########
                                    # and while here, go ahead and add other Series attributes
                                    orthanc_study_clues += ',Protocol:,'
                                    if orthanc_series_protocol:
                                        orthanc_study_clues += orthanc_series_protocol
                                    orthanc_study_clues += ',BodyPartExamined:,'
                                    if orthanc_series_bodypart:
                                        orthanc_study_clues += orthanc_series_bodypart
                                    orthanc_study_clues += ',Modality:,'
                                    if orthanc_series_modality:
                                        orthanc_study_clues += orthanc_series_modality
                                    orthanc_study_clues += ',Manufacturer:,'
                                    if orthanc_series_manufacturer:
                                        orthanc_study_clues += orthanc_series_manufacturer
                                else:
                                    if self.locutus_settings.LOCUTUS_VERBOSE:
                                        print('{0} # orthanc_study_num_series={1}, so **NOT** adding series fields like series description={2}'.format(
                                            ORTHANC_OUT_PREFIX,
                                            orthanc_study_num_series,
                                            orthanc_series_description), flush=True)
                            ######################
                            # NOTE: adding uuid just beyond the potentially empty target field,
                            # as 'uuid=abc-123...' to align with the uuid parm for Orthanc's study explorer:
                            # http://HOST:PORT/app/explorer.html#study?uuid=abc-123...-xyz--789:
                            #print('{0},{1}MANCOLS:{2},DUPLICATED:{3},MANSTATUS:{4},STATTIME:{5},STATTARGET:{6},uuid='.format(
                            ######################
                            # NOTE: replaced manifest_status w/ MANIFEST_OUTPUT_SHOW_MULTIUUIDS_MANIFEST_STATUS to facilitate easier filtering w/in the manifest_out.csv
                            # (otherwise the additional commented SHOW_MULTIUUIDS rows show up under a status filter for ERROR_MULTIPLE_CHANGE_UUIDs)
                            print('{0},{1}{2},{3},{4},{5},{6},uuid={7},,{8},{9}'.format(
                                    MANIFEST_OUTPUT_PREFIX,
                                    preshow_comment,
                                    module_specific_manifest_columns,
                                    accession_duplicated,
                                    self.locutus_settings.MANIFEST_OUTPUT_SHOW_MULTIUUIDS_MANIFEST_STATUS,
                                    status_deid_datetime,
                                    status_target,
                                    status_uuid,
                                    ORTHANC_OUT_PREFIX,
                                    orthanc_study_clues),
                                    flush=True)
                            # NOTE: removed the module_specific_xtra_columns for these expanded multiuuids:
                            #        module_specific_xtra_columns),
        # and one terminating header line to end the multi-uuids prior to the full accession:
        #print('{0},#################################################'.format(MANIFEST_OUTPUT_PREFIX), flush=True)
        # a SHORTER one, so that the full accession stands out a bit more:
        print('{0},######################'.format(MANIFEST_OUTPUT_PREFIX), flush=True)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: expand_multiuuids_to_manifest_output(multiuuid_accessions_shown): , w/ len(multiuuid_accessions_shown) now == {0}, YAYYY Gbye!!!'.format(multiuuid_accessions_shown), flush=True)
        return multiuuid_accessions_shown
        # end of expand_multiuuids_to_manifest_output()


    #########################################
    # Whole lotta retiring going on:
    # vvvvvvvvvv

    # self.locutus_settings.SQL_OUT_PREFIX NOTE: including both SQL comments (--) and Python comments (#) as: "SQL_OUT:-- # ..."
    # amongst the various self.locutus_settings.SQL_OUT_PREFIX lines within these helpers,
    # such that it is easier to grep SQL_OUT | grep -v '#'
    # and....
    # for those wanting to filter out specifically by '--' SQL comments, here's an example of how:
    # $ grep SQL_OUT locutus_summarizer_out.2024aug26at1153.txt  | sed 's/SQL_OUT://' | grep -v '\-\-'


    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    # NOTE: Stager has no batch_name, shall retire the entire accession from the Stager:
    def retire_accession_stager_only(self, accession_num, except_change_seq=None, delete_from_orthanc=False, orthanc_comment=''):
        # helper method to update and retire ONLY the corresponding MANIFEST records for an accession_num
        # (keeping status record in place for cases where the current status record is valid and possibly mid-processing,
        #  but for which the manifest record has been changed in the meantime and only it needs updating...)
        successful = True # unless an exception, thrown perhaps
        orthanc_uuids = []
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.retire_accession_stager_only(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}'.format(CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES), flush=True)
        #
        # SQL comment prior to the actual SQL command:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.retire_accession_stager_only(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                    accession_num), flush=True)
        #
        # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs,
        # BUT, if needed, we could explore a 'accession_str LIKE \'{0}.%\'' for any such sub-accessions
        # PLEASE NOTE: this SELECT accession_range_where intentionally differs from accession_num, as accession_str, since it is for the STAGER DB:
        accession_range_where = 'accession_str=\'{0}\''.format(accession_num)
        if except_change_seq:
            accession_range_where += ' AND change_seq_id != {0}'.format(except_change_seq)
        # FIRST, a commented SELECT version of the actual UPDATE command:
        # BATCHES NOTE: no batches in the Stager, retire em directly:
        preupdate_sql_select='SELECT uuid, change_seq_id, (-1*change_seq_id) as proposed_change_seq_id, '\
                            'accession_num, (-1 * accession_num) as proposed_accession_num, '\
                            'accession_str, concat(\'-\',accession_str) as proposed_accession_str, '\
                            'active, False as proposed_active, '\
                            'change_datetime_orthanc, datetime_staged FROM {0} '\
                                        'WHERE {1} '\
                                        'AND active ORDER BY uuid, change_seq_id ;'.format(
                                        self.stage_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Stager, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.stage_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions);
        # Doing these for the Stager first, as a 1st pass test of its new StagerDBconnSession:
        preupdate_sql_select_results =  self.StagerDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
            this_orthanc_uuid = preupdate_sql_select_result['uuid']
            if this_orthanc_uuid not in orthanc_uuids:
                orthanc_uuids.append(this_orthanc_uuid)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0} # added uuid \'{1}\' to orthanc_uuids for DELETE curl to Orthanc'.format(
                        ORTHANC_OUT_PREFIX, this_orthanc_uuid), flush=True)
            elif self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0} # **NOT** adding uuid \'{1}\' to orthanc_uuids for DELETE curl to Orthanc; '\
                        'uuid already there for another change_seq_id'.format(
                        ORTHANC_OUT_PREFIX, this_orthanc_uuid), flush=True)
        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        # and NOW, the actual UPDATE to the STAGER DB (hence the use of accession_str!)
        update_sql='UPDATE {0} '\
                                        'SET change_seq_id=(-1*change_seq_id), '\
                                        'accession_num=(-1 * accession_num), '\
                                        'accession_str=concat(\'-\',accession_str), '\
                                        'active=False '\
                                        'WHERE {1} '\
                                        'AND active ;'.format(
                                        self.stage_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
            # SQL blank line at end of all this method's SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        #
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.retire_accession_stager_only(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            self.StagerDBconnSession.execute(update_sql)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.retire_accession_stager_only(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
        #

        if delete_from_orthanc:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print(f'{ORTHANC_OUT_PREFIX} ####################', flush=True)
                # TODO: add subject_id, object_info01, object_info02, object_info03 (OnPrem) to this:
                print(f'{ORTHANC_OUT_PREFIX} # DELETE from ORTHANC {orthanc_comment}: accession_num=\'{accession_num}\', with {len(orthanc_uuids)} uuids:', flush=True)
            num_orthanc_uuids = len(orthanc_uuids)
            this_orthanc_uuid_num = 0
            for this_orthanc_uuid in orthanc_uuids:
                this_orthanc_uuid_num += 1
                ################################################
                # TODO: adding ORTHANCINTPROD, via the following...
                # NOTE: https://orthanc.uclouvain.be/book/users/rest.html#deleting-patients-studies-series-or-instances
                # curl -X DELETE http://localhost:8042/studies/c4ec7f68-[EXAMPLE-UUID]-155ab19f
                # from:
                # curl_cmd = 'curl -v -u  "' +  ORTHANC_USER + ':' + ORTHANC_PASS + '" ' + ORTHANC_URL + '/studies/' + study_uuid
                # as from Joey's:  trig-orthanc-docker/manage_orthanc/manage.py:
                #   def delete_studies(self, df: Path, dry_run=True):
                #   command = [ "curl", "-v", "-u", f"{self.ORTHANC_USER}:{self.ORTHANC_PASS}", "-X", "DELETE", f"{self.ORTHANC_URL}studies/{line}",
                ################################################
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>', flush=True)
                    print(f'{ORTHANC_OUT_PREFIX} echo "DELETE from ORTHANC {orthanc_comment}: accession_num=\'{accession_num}\', uuid {this_orthanc_uuid_num} of {num_orthanc_uuids}..."', flush=True)
                VAR_ORTHANC_URL = "${ORTHANC_URL}"
                VAR_ORTHANC_USER = "${ORTHANC_USER}"
                VAR_ORTHANC_PASS = "${ORTHANC_PASS}"
                for_shell_staged_study_url =  f'{VAR_ORTHANC_URL}/studies/{this_orthanc_uuid}'
                internal_staged_study_url = f'{self.orthanc_url}/studies/{this_orthanc_uuid}'
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print(f'{ORTHANC_OUT_PREFIX} curl -v -u {VAR_ORTHANC_USER}:{VAR_ORTHANC_PASS} -X DELETE {for_shell_staged_study_url}', flush=True)
                ######################
                # TODO: define a new method/subroutine for the following, as mod'd from expand_multiuuids_to_manifest_output():
                # BUT FIRST, check if enable_db:
                #
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.retire_accession_stager_only(): DELETING from Orthanc since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES: {1}'.format(
                            CLASS_PRINTNAME, for_shell_staged_study_url), flush=True)
                    orthanc_delete_retdata = RestToolbox.DoDelete(internal_staged_study_url)
                    # TODO: evaluate the following to determine a success, as it is to return: _DecodeJson(content),
                    # that is, if not (resp.status in [ 200 ]): raise Exception(resp.status)
                    # TODO: therefore also consider wrapping this in a try/catch, but for now....
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('DEBUG: RestToolbox.DoDelete() returned: {0}'.format(orthanc_delete_retdata), flush=True)
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.retire_accession_stager_only(): **NOT** DELETING from Orthanc since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES: {1}'.format(
                            CLASS_PRINTNAME, for_shell_staged_study_url), flush=True)
                #
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>', flush=True)
                print(f'{ORTHANC_OUT_PREFIX} ####################', flush=True)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print(f'{ORTHANC_OUT_PREFIX} ####################', flush=True)
                print(f'{ORTHANC_OUT_PREFIX} # **NOT** DELETE from ORTHANC {orthanc_comment}: accession_num=\'{accession_num}\', delete_from_orthanc={delete_from_orthanc}:', flush=True)
                print(f'{ORTHANC_OUT_PREFIX} ####################', flush=True)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # END of {1}.retire_accession_stager_only(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.retire_accession_stager_only(): Goodbye.'.format(CLASS_PRINTNAME), flush=True)
        return successful
        # end of retire_accession_stager_only()


    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    def reactivate_accession_manifest_only(self, accession_num, new_manifest_status, reactivate_select_info, reactivate_update_info, batch_only=False, batch_name=None):
        # helper method to update and retire ONLY the corresponding MANIFEST records for an accession_num
        # (keeping status record in place for cases where the current status record is valid and possibly mid-processing,
        #  but for which the manifest record has been changed in the meantime and only it needs updating...)
        successful = True # unless an exception, thrown perhaps
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.reactivate_accession_manifest_only(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}'.format(CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES), flush=True)

        #########################################################
        # DEV-NOTE: for MANIFEST-ONCE MULTI-UUID RESOLUTION
        # 1)consider if this reactivate_accession_manifest_only() method should ALSO update/create a new AAA_LOCUTUS_BATCHES MANIFEST-like record
        #   ==YES, it SHOULD indeed UPDATE its AAA_LOCUTUS_BATCHES MANIFEST-like record, AND loop through all of them to update those MANIFEST records.
        # 2) AND see how they might likewise "retire" the previous MANIFEST record w/ retire_accession_manifest_only(), does that clear out the AAA_LOCUTUS_BATCHES record?
        #   ==NOT ANYMORE, no need to retire, but instead re-use existing MANIFEST records
        # 3) AND AND be sure to check that ALL such MANIFEST table updates also have a corresponding AAA_LOCUTUS_BATCHES update
        # 4) ALSO ENSURE that an enable_db_updates is available and..... first TEST w/ enable_db_updates=False
        # 5) LOOK at all of the batch_only & batch_name values being passed on from the main call to resolve_multiuuids()
        # 6) AND AND AND: restrict the Resolver to ONLY running off of input manifests; no bypass CSV DB batches for these Multi-UUID Resolvers, sorry, need the RESOLVE_CMD details.
        #   STILL: DO allow the batch name to be supplied, and run a test following it through
        #   JOB DESC: Resolver_2026june03at0644_testingDRYRUN_for_RESEND_RADIOLOGY_for1x_OnPremSCIT605_ButFIRST_addBatchName_woBypass
        #########################################################

        # add a batch_clause to the Where clause, if so defined:
        # NOTING that this reactivate only needs the SELECT clause.
        # FURTHERMORE, NO LONGER NEEDED AT ALL! (even if still used in the various print statements)
        # Aiming to reactivate ALL batch MANIFEST records for this accession.
        ##########
        # NOTE: see also preload_accession() w/ further NOTES,
        # including of batch_only implying a NOT NULL batch_name.
        batch_clause = ''
        if batch_only:
            if not batch_name or not len(batch_name):
                batch_clause = 'AND batch_name IS NULL '
            else:
                batch_clause = 'AND batch_name=\'{0}\' '.format(batch_name)

        #
        # SQL comment prior to the actual SQL command:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.reactivate_accession_manifest_only(accession_num=\'{2}\', new_manifest_status={3} w/ batch_only={4}, batch_name=\'{5}\'):'.format(
                self.locutus_settings.SQL_OUT_PREFIX,
                CLASS_PRINTNAME,
                accession_num, new_manifest_status,
                batch_only, batch_name), flush=True)
        #
        # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)
        retired_accession_range_where = 'accession_num=\'-{0}\''.format(accession_num)
        # FIRST, a commented SELECT version of the actual UPDATE command:
        # NOTE: removed "now() as proposed_..." to retain the previous processing date:

        ##########
        # NOTE: the accession will have ALREADY been RETIRED from MANIFEST table.
        # As such, active is currently NOT True, huh.
        # Shoot, this gets much more complicated if we consider multiple previously retire MANIFEST records
        # (even prior to the introduction of BATCHES)
        # Q: would we want the most recent NON-active to then be updated? BUT, no retirement date saved,
        # only the last_datetime_processed, but even that could be off.
        # NOTE: recommending that reactive_accession_manifest_only() NOT even be called,
        # and that a Preloader should be (re?)-run AFTER resolving the Multi-UUIDs,
        # rather than attempting to wade through the variations here.
        # Indeed, any other batches in this and all other workspaces might also need to be Preloaded,
        # having also been retired, regardless of the particular Workspace+Batch in which this Multi-UUID was Resolved.
        # (NOTING that the Summarizer might list such a retired accession MANIFEST records as NOT_FOUND until re-Preloaded)
        ##########
        # BATCHES NOTE: go ahead and reactivate this accessions' active MANIFEST records for any such batches, yeah???
        preupdate_sql_select='SELECT accession_num, {0}, manifest_status, \'{1}\' as proposed_manifest_status, last_datetime_processed FROM {2} '\
                                        'WHERE {3} '\
                                        'AND active ;'.format(
                                        reactivate_select_info,
                                        new_manifest_status,
                                        self.manifest_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that prepdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        # and NOW, the actual UPDATE:
        # NOTE: removed "now() as proposed_..." to retain the previous processing date:
        ##########
        # NO LONGER: leave the following UPDATE without a setting of the active, just leaving as True, yeah? No.
        ##########
        # NOTE: this accession MANIFEST will have ALREADY been RETIRED
        # Therefore, must look for its NEGATED retired_accession_range_where and NOT active,
        # UPDATING to active=True and its POSITIVE accession_num.
        ##########
        # BATCHES NOTE: go ahead and reactive ALL batches for this accession's MANIFEST records,
        # omitting batch_clause for this after all, yeah?
        ##########
        update_sql='UPDATE {0} '\
                                        'SET manifest_status=\'{1}\', '\
                                        '{2}, '\
                                        'active=True, accession_num=\'{3}\' '\
                                        'WHERE {4} '\
                                        'AND NOT active ;'.format(
                                        self.manifest_table,
                                        new_manifest_status,
                                        reactivate_update_info,
                                        accession_num,
                                        retired_accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
            # SQL blank line at end of all this method's SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        #
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.reactivate_accession_manifest_only(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            self.LocutusDBconnSession.execute(update_sql)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.reactivate_accession_manifest_only(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
        #
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # END of {1}.reactivate_accession_manifest_only(accession_num=\'{2}\', manifest_status={3}):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                accession_num, new_manifest_status), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.reactivate_accession_manifest_only(): Goodbye.'.format(CLASS_PRINTNAME), flush=True)
        return successful
        # end of reactivate_accession_manifest_only()


    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    # NOTE: if not batch_only, shall retire all MANIFEST records for this accession in this Workspace;
    #       else, if batch_only, shall ONLY retire those for THIS batch in this Workspace.
    #
    # TODO: combine Settings:retire_accession*() & De-ID:preretire_accession*()
    # incl their sub-methods, (pre)retire_accession_[status/manifest]_only()
    # for a single centralized set of methods to maintain:
    def retire_accession_manifest_only(self, accession_num, batch_only=False, batch_name=None):
        # helper method to update and retire ONLY the corresponding MANIFEST records for an accession_num
        # (keeping status record in place for cases where the current status record is valid and possibly mid-processing,
        #  but for which the manifest record has been changed in the meantime and only it needs updating...)
        # BATCHES NOTE: optional batch_only & batch_name parameters to incorporate the new batch attribute.
        successful = True # unless an exception, thrown perhaps
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.retire_accession_manifest_only(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}, '\
                    'for accession_num=\'{2}\' w/ batch_only={3}, batch_name=\'{4}\''.format(
                        CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES,
                        accession_num, batch_only, batch_name), flush=True)

        # add a batch_clause to the Where clause, if so defined:
        batch_clause = ''
        if batch_only:
            if not batch_name or not len(batch_name):
                batch_clause = 'AND batch_name IS NULL '
            else:
                batch_clause = 'AND batch_name=\'{0}\' '.format(batch_name)

        #
        # SQL comment prior to the actual SQL command:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.retire_accession_manifest_only(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
        #
        # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs,
        # BUT, if needed, we could explore a 'accession_num LIKE \'{0}.%\'' for any such sub-accessions
        # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)

        # FIRST, a commented SELECT version of the actual UPDATE command:
        # BATCHES NOTE: retiring for this specific batch_clause:
        preupdate_sql_select='SELECT accession_num, concat(\'-\',accession_num) as proposed_accession_num, manifest_status '\
                                        'active, False as proposed_active '\
                                        'FROM {0} WHERE {1} {2} '\
                                        'AND active ;'.format(
                                        self.manifest_table,
                                        accession_range_where,
                                        batch_clause)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        # and NOW, the actual UPDATE:
        update_sql='UPDATE {0} '\
                                        'SET accession_num=concat(\'-\',accession_num), '\
                                        'active=False '\
                                        'WHERE {1} {2} '\
                                        'AND active ;'.format(
                                        self.manifest_table,
                                        accession_range_where,
                                        batch_clause)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
            # SQL blank line at end of all this method's SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        #
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.retire_accession_manifest_only(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            self.LocutusDBconnSession.execute(update_sql)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.retire_accession_manifest_only(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
        #

        # NOTE: ===> BATCHES table retiring/clearing into retire_accession_manifest_only() [no need for STATUS table, only MANIFEST]
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: =====> preloader to UPDATE the existing BATCH records to RETIRE at LOCUTUS_ALL_BATCHES_TABLE '\
                '(pending batch_clause=\'{0}\')...'.format(batch_clause), flush=True)

        # BATCHES NOTE: for LOCUTUS_ALL_BATCHES_TABLE: FIRST, a commented SELECT version of the actual UPDATE command, across ALL batches (pending batch_clause)
        preupdate_sql_select='SELECT accession_num, concat(\'-\',accession_num) as proposed_accession_num, manifest_status '\
                                        'active, False as proposed_active '\
                                        'FROM {0} WHERE {1} {2}'\
                                        'AND active ;'.format(
                                        self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE,
                                        accession_range_where,
                                        batch_clause)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        # LOCUTUS_ALL_BATCHES_TABLE: and NOW, the actual UPDATE:
        update_sql='UPDATE {0} '\
                                        'SET accession_num=concat(\'-\',accession_num), '\
                                        'active=False '\
                                        'WHERE {1} {2}'\
                                        'AND active ;'.format(
                                        self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE,
                                        accession_range_where,
                                        batch_clause)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
            # SQL blank line at end of all this method's SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        #
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.retire_accession_manifest_only(): updating DB (LOCUTUS_ALL_BATCHES_TABLE) since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            self.LocutusDBconnSession.execute(update_sql)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.retire_accession_manifest_only(): **NOT** updating DB (LOCUTUS_ALL_BATCHES_TABLE) since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
        #

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # END of {1}.retire_accession_manifest_only(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.retire_accession_manifest_only(): Goodbye.'.format(CLASS_PRINTNAME), flush=True)
        return successful
        # end of retire_accession_manifest_only()


    # NOTE: preload_accession()
    # as modified from preload_accession()
    # as modified from preset_accession_for_reprocessing()
    # for:  LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST
    # w/    LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST_PREPROCESSING_SUFFIX
    ################### ################### ################### ################### ###################
    # TODO: for maintainability, sub-divide this whole preload_accession() into its POTENTIAL parts, likely:
    #           preload_accession_status_only()
    #           preload_accession_manifest_only()
    #           preload_accession_batches_only()
    ################### ################### ################### ################### ###################
    # NOTE: preload_subject_id semi-redundantly at the end of this method's parameters signature,
    # for ease of adding to global BATCHES table, even though embedded in preload_* info for the MANIFEST+STATUS tables.
    def preload_accession(self, initial_manifest_status, accession_num, \
                            preload_manifest_columns_sans_status, preload_select_info, preload_insert_info, \
                            preload_update_info, preload_manifest_status_suffix, \
                            preload_batch_name, preload_module_name, preload_workspace_name,
                            preload_subject_id):
        # helper method to create/update a MANIFEST and update its STATUS records (if any), for pre-processing,
        # NOTE: preload_manifest_columns_sans_status is to be without the manifest_status,
        #   as manifest_status will be explicitly appended;
        #   likewise, no need for last_datetime_processed, nor orthanc_instance,
        #   only the subject_id and various object attributes (OnPrem)
        # NOTE: likewise, no need for the accession_num in preload_manifest_columns_sans_status!
        #####
        # NOTE: initial_manifest_status is from the calling Process() which only queried the MANIFEST table
        # NOTE: if initial_manifest_status==NOT_FOUND (or any, now!), confirm with a query of the STATUS records:
        #   if # STATUS records  < 1: new_manifest_status starts with: ONDECK_PENDING_CHANGE_PREFIX
        #   if # STATUS records == 1: new_manifest_status starts with: ONDECK_[RE-?]PROCESSING_CHANGE_PREFIX (etc)
        #   if # STATUS records  > 1: new_manifest_status starts with: ONDECK_MULTIPLES
        #####
        # NOTE: SEE ALSO preset_accession_for_reprocessing() for a more specific pre-setter,
        # using only REPROCESSING_CHANGE_MOMENTARILY="RE-PROCESSING_CHANGE_MOMENTARILY"
        # and only for those already previously processed, at least partially,
        # and without any of the pretiring of accessions done here
        #####
        # TODO: considering consolidating these:
        #   * preset_accession_for_reprocessing)
        #   * preload_accession()
        # and eventually even reassess the relevance of retiring,
        # once N:N enhancements are introduced, e.g.,:
        #   to add N:N project:batches to accessions,
        #   as well as accessions to N:N jobs:logs
        #####

        successful = True # unless an exception, thrown perhaps
        subaccessions_only = False # for accession_range_where
        # for retire_*():
        ####################
        # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
        # NOTE: Q: NO need to even cast these into floats anymore, riiiiight? right.
        ####################
        ####################
        # Juneteenth 2025: deprecating SPLIT multi-UUIDs:
        ####################

        # BATCHES NOTE: settin up the clauses for later use w/in the SQL:
        batches_insert_val_clause = ''
        batches_select_clause = ''
        # BATCHES NOTE: WAS: if preload_batches_enable:
        # WARNING: preload_batches_enable parameter is now: locutus_bypass_manifest_load_batch_from_DB,
        # thereby changing this to now be whether or not preload_batch_name is NULL or not:
        # WAS: preload_batches_enable = False
        # 4/09/2026 FIX: go ahead and enable batches EVEN IF batch_name is NULL as the default batch
        preload_batches_enable = True
        if preload_batch_name is not None and len(preload_batch_name):
            # NOTE: NO LONGER *just* for batch_name not NULL...
            # BATCHES NOTE: since a general preload_batches_enable parameter has been upgrade to # WAS: preload_bypass_manifest_load_batch_from_DB,
            # the indication of whether or not to Preload any batch (whether via a CSV or the DB BATCHES, as handled by the caller),
            # shall be determined internally to the Preloader by a non-empty preload_batch_name:
            #preload_batches_enable = True
            batches_insert_val_clause='\'{0}\''.format(preload_batch_name)
            batches_select_clause = 'batch_name=\'{0}\''.format(preload_batch_name)
        else:
            batches_insert_val_clause='NULL'
            batches_select_clause = 'batch_name is NULL'
            # 4/09/2026 FIX: ensure that this empty preload_batch_name matches the None expected by the SELECTED this_acc_batch_name
            preload_batch_name = None
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('**DEBUG**: preload_accession() called for accession_num={0} w/ preload_batches_enable={1}, batch_name=\'{2}\' (for internal preload_batches_enable={3}) & module_name=\'{4}\' & workspace_name=\'{5}\'. '.format(
                accession_num, preload_batches_enable, preload_batch_name, preload_batches_enable, preload_module_name, preload_workspace_name), flush=True)

        preload_status = 'r3m0-ZZZ-UNHANDLED-PRELOAD-'
        # ITERATION #1 really WAS this simple, LOL:
        ####
        # NOTE: generalizing towards a more applicable preload_status,
        # according to both the initial_manifest_status and num_status_records, as SELECTing below...
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.preload_accession(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                accession_num), flush=True)
        # SQL blank line before all this method's umbrella SQL:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.preload_accession(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}'.format(
                CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES), flush=True)

        ################### ################### ################### ################### ###################
        # NOTE: START OF POTENTIAL preloaded sub-divided method: preload_accession_status_only()
        # SEE ALSO:
        #           preload_accession_manifest_only()
        #           preload_accession_batches_only()
        ################### ################### ################### ################### ###################

        #
        # Part 0: STATUS table query
        # Get the preset_select_info to later INSERT the new records after a retiring the old, in case previous subject_id, etc:

        #####
        # NOTE: rather than module-specific deidentified_gs_target
        # use, as from the Setup(): self.status_target_field ==
        #   LOCUTUS_ONPREM_DICOM_STATUS_TARGET_FIELD if DICOM_MODULE_ONPREM
        #####

        # NOTE: ADDING FURTHER CAUTION with the addition of change_seq_id>0 for for non-retired accession UUIDs only:
        # NOTE: updated to the latest & greatest check for retirement... active :-)
        #
        # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # {1}.preload_accession():'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME), flush=True)
        # NOTE: allow the below SELECT to span all batches for this accession_num in this workspace,
        # since the resultint num_status_records count is later used to determine if a STATUS record even exists at all:
        # WAS w/:                       'AND batch_name=\'{4}\' '
        # Instead of filtering upon this SELECT, instead, manually filter post-SELECT at found_curr_status_batch = True.
        # BATCHES NOTE: SELECT to span across all batches:
        # HOTFIX HACK WARNING: hardcoding the expected STATUS_RECORD_UUUID_FIELDNUM here (base 0), for the subseqeuent SELECT acc (pos 0), phase (pos 1), uuid (pos 2), ..:
        STATUS_RECORD_UUID_FIELDNUM = 2
        preupdate_sql_select='SELECT accession_num, phase_processed, uuid, '\
                                        'change_type, change_seq_id, '\
                                        'datetime_processed, '\
                                        'batch_name, '\
                                        '{0}, '\
                                        '{1} FROM {2} WHERE {3} '\
                                        'AND active ;'.format(
                                        self.status_target_field,
                                        preload_manifest_columns_sans_status,
                                        self.status_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        # Now, use the initial_manifest_status, AND a count of the num_status_records,
        # to compute a more applicable preload_status:
        #############
        # NOTE: load up all STATUS results for later usage:
        # NOTE: retain copies of the curr_status_results_for_this_batch to post-retire re-INSERT:
        # at later: # INSERT new post-retired copy into the STATUS table:
        curr_status_results_for_this_batch = []
        curr_status_results_for_batchless_default = []

        num_status_records = 0
        found_curr_status_batch = False
        num_curr_status_batch_records = 0
        default_batchless_status_record = None
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            num_status_records += 1

            this_acc_subj_id = preupdate_sql_select_result['subject_id']
            this_acc_batch_name = preupdate_sql_select_result['batch_name']
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('***DEBUG*** LOOKING FOR: preload_batch_name==\'{0}\', '\
                    'FOUND: this_acc_batch_name=\'{1}\'.'.format(preload_batch_name, this_acc_batch_name))
            # NOTE: ^^^^ b/c expecting that the subsequent elif does NOT match: and this_acc_batch_name == preload_batch_name:
            if this_acc_batch_name is None or this_acc_batch_name == '':
                # default batchless STATUS record; save it in case an INSERT is need for this batch.
                # TODO: consider checking if it is None, and WARN if multiples found of such an active default batch-less STATUS!
                # NOTE: for now, a mere WARNING:
                curr_status_results_for_batchless_default.append(preupdate_sql_select_result)
                if len(curr_status_results_for_batchless_default) > 1:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('***WARNING*** found another (#{0}, multiple!) batch-less STATUS record for this accession \'{1}\'; '\
                            'this MULTI-UUID now has {0} such uuids, so far.'.format(
                            len(curr_status_results_for_batchless_default), accession_num), flush=True)
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('***DEBUG*** found 1st default batch-less STATUS record for this accession \'{0}\'.'.format(accession_num))
                # BATCHES NOTE: need to save each and every one of this MULTIPLE_CHANGE_UUIDS!

            #WAS: elif preload_batches_enable and this_acc_batch_name == preload_batch_name:
            # 4/09/2026 FIX: if rather than elif to ALWAYS compare this_acc_batch_name == preload_batch_name, even if both None:
            if preload_batches_enable and this_acc_batch_name == preload_batch_name:
                # USING: preload_batch_name (w/ preload_module_name, preload_workspace_name implied here IN the Workspace)
                found_curr_status_batch = True
                num_curr_status_batch_records += 1
                # NOTE: retain a copy of the preupdate_sql_select_result in curr_status_results_for_this_batch to post-retire re-INSERT:
                # at later: # INSERT new post-retired copy into the STATUS table:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('***DEBUG*** found STATUS record #{0} for this accession=\'{1}\' AND this batch=\'{2}\'; appending to others so found.'.format(
                        num_curr_status_batch_records, accession_num, preload_batch_name), flush=True)
                curr_status_results_for_this_batch.append(preupdate_sql_select_result)

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)

        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('DEBUG: preload_accession( initial_manifest_status={0}, accession_num=\'{1}\') FOUND num_status_records={2} '\
                '(overall, {3} such records specific to preload_batches_enable={4}, batch_name=\'{5}\') *without* DISTINCT(uuid); OVERALL MATCH to batch ?= {5}'.format(
                initial_manifest_status, accession_num, num_status_records, num_curr_status_batch_records,
                preload_batches_enable, preload_batch_name, found_curr_status_batch), flush=True)

        ##########
        # NOTE: introducing another DISINCT(uuid) check here with the above full SELECT,
        # since the above full SELECT can misconstrue num_status_records > 1
        # as needing ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX.
        #
        # NOTE: removed change_seq_id / as_change_seq_id entirely, as from the Migrator portion,
        # since it is really only needed for the Stager.
        ##########
        # BATCHES NOTE: allow the DISTINCT(uuid) to span all batches for this accession_num in this workspace
        preupdate_sql_select_DISTINCT_uuids='SELECT DISTINCT(uuid) '\
                                        'FROM {0} WHERE {1} '\
                                        'AND active ;'.format(
                                        self.status_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_DISTINCT_uuids), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select_DISTINCT_uuids
        # in order to preserve those values in the output log (as well as in the retired accessions)
        # Now, use the initial_manifest_status, AND a count of the num_status_records_DISTINCT_uuids,
        # to compute a more applicable preload_status:
        #############
        num_status_records_DISTINCT_uuids = 0
        curr_status_results_DISTINCT_uuids = []
        preupdate_sql_select_results_DISTINCT_uuids =  self.LocutusDBconnSession.execute(preupdate_sql_select_DISTINCT_uuids)
        for preupdate_sql_select_result_DISTINCT_uuids in preupdate_sql_select_results_DISTINCT_uuids.fetchall():
            num_status_records_DISTINCT_uuids += 1
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result_DISTINCT_uuids), flush=True)
            curr_status_results_DISTINCT_uuids.append(preupdate_sql_select_result_DISTINCT_uuids)

        # Part 1. STATUS table UPDATE...
        #
        # SQL comment prior to the actual SQL command:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.preload_accession(accession=\'{2}\') STATUS table updates:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                accession_num), flush=True)
            print('{0}-- # {1}.preload_accession() for STATUS:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME), flush=True)

        # Part 1a: generate preload_DB_STATUS_operation from num STATUS records (STATUS) and ...
        preload_DB_STATUS_operation = None
        # and, for any Part1a/Part1b conditions which might require a bypass of the subsequent Part 2 MANIFEST update:
        BYPASS_preload_DB_MANIFEST_operation = False

        # NO LONGER TRUE: STATUS will only ever require an UPDATE here, or *potentially* leave as is (e.g., if the same manifest attributes already, but still need to UPDATE the manifest_status itself):
        # BATCHES NOTE: STATUS may indeed require some INSERTS now that we have batches to PROPAGATE from the MIGRATED default batch (batch_name IS NULL)
        # NOTE: set default ERROR_MULTIPLE_CHANGE_UUIDS manifest_status here for all of the top conditions:
        preload_status = self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix

        if num_status_records > 1 \
        and num_status_records_DISTINCT_uuids > 1 \
        and found_curr_status_batch \
        and num_curr_status_batch_records == num_status_records_DISTINCT_uuids :
            # reGARDless of its initial_manifest_status!
            ##########
            # NOTE: num_status_records_DISTINCT_uuids helps clarify general num_status_records > 1
            # as needing ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX *only* if num_status_records_DISTINCT_uuids > 1.
            #
            # OLD REPEAT NOTE: this does NOT take care of the change_seq_id / as_change_seq_id linking for any identical uuids for an accession
            # as that is left deeper in the respective De-ID modules.
            #
            # REPEAT DONE: removed change_seq_id / as_change_seq_id entirely,
            # as from the Migrator portion, since it is really only needed for the Stager.
            ##########
            # and even though no missing STATUS records (only existing to UPDATE), the MANIFEST record shall be given manifest_status of MULTIPLE_CHANGE_UUIDS:
            # SHARING FROM ABOVE: preload_status = self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
            ## DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
            preload_DB_STATUS_operation = 'UPDATE'
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                        'w/ (MULTIPLE num_status_records=={3} AND >1 DISTINCT uuids=={4}, '\
                        'with found_curr_status_batch={5} and ALL {6} record(s) of uuids in batch'\
                        '==> UPDATE STATUS & MANIFEST as ONDECK with MULTIPLE_CHANGE_UUIDS new preload_status = {7}.'.format(
                        MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, num_status_records_DISTINCT_uuids,
                        found_curr_status_batch, num_curr_status_batch_records, preload_status), flush=True)
        elif num_status_records > 1 \
        and num_status_records_DISTINCT_uuids > 1 \
        and num_curr_status_batch_records < num_status_records_DISTINCT_uuids :
            # NOTE: not including the found_curr_status_batch check, as this could be used for both:
            #   (NOT found) OR (found AND NOT all UUID records)
            #####
            # DEV NOTE: same as the above initial if, but now that it has a check for num_curr_status_batch_records == num_status_records_DISTINCT_uuids
            # this is for the case where num_curr_status_batch_records < num_status_records_DISTINCT_uuids,
            # MEANING that we've gotta Propagate some UUIDs that have already been Migrated into the default batch (batch_name IS NULL),
            # but have NOT yet found their way into this active batch
            ######
            # Q: how to best do such a hybrid INSERT_and_UPDATE????
            # FOR NOW, merely note it as such, and care on, to handle subsequently:
            preload_DB_STATUS_operation = 'HYBRID_INSERT_SOME_and_UPDATE_ALL'
            # and regardless of how many STATUS records to INSERT -vs- just UPDATE, the MANIFEST record shall be given manifest_status of MULTIPLE_CHANGE_UUIDS:
            # SHARING FROM ABOVE: preload_status = self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
            if found_curr_status_batch:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                        'w/ (MULTIPLE num_status_records=={3} AND >1 DISTINCT uuids=={4}, '\
                        'with found_curr_status_batch={5} and a SUBSET OF {6} record(s) of uuids in batch'\
                        '==> setting action to {7} STATUS & MANIFEST as ONDECK with MULTIPLE_CHANGE_UUIDS new preload_status = {8}.'.format(
                        MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, num_status_records_DISTINCT_uuids,
                        found_curr_status_batch, num_curr_status_batch_records,
                        preload_DB_STATUS_operation, preload_status), flush=True)
            else:
                # none found for the current batch... yet:
                #WAS: preload_DB_STATUS_operation = 'INSERT'
                # Q: how to best do such a hybrid INSERT of all????
                # FOR NOW, merely note it as such, and care on, to handle subsequently:
                preload_DB_STATUS_operation = 'INSERT_ALL'
                # and for all of em, set status as PENDING_CHANGE:
                # SHARING FROM ABOVE: preload_status = self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                # ======> WAIT, WHY DID THIS NEED A CHANGE TO MULTI_UUIDS: ????
                # SHARING FROM ABOVE: preload_status = self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                        'w/ (MULTIPLE num_status_records=={3} AND >1 DISTINCT uuids=={4}, '\
                        'NONE FOUND as per found_curr_status_batch={5} '\
                        '==> setting action to {6} of the STATUS & MANIFEST as ONDECK with MULTIPLE_CHANGE_UUIDS new preload_status = {7}.'.format(
                        MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, num_status_records_DISTINCT_uuids,
                        found_curr_status_batch, preload_DB_STATUS_operation, preload_status), flush=True)
        else:
            # i.e., NOT num_status_records > 1 OR NOT num_status_records_DISTINCT_uuids > 1
            #   i.e., num_status_records <= 1 OR num_status_records_DISTINCT_uuids <= 1
            # So, either PENDING_CHANGE or FOUND and NOT Multi-UUID
            ##########
            if num_status_records > 0 \
            and not len(curr_status_results_for_batchless_default) :
                # NOTE: addition to FIX an erroneous match with the subsequent "elif num_status_records == 0 or not len(curr_status_results_for_batchless_default):"
                # for an edge case of a STATUS record for a NAMED batch_name, but none for the NULL batch (hence an empty curr_status_results_for_batchless_default)
                print('WARNING: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') FOUND num_status_records=={3} '\
                        '(for found_curr_status_batch=={4}) '\
                        'BUT with ***NO*** corresponding STATUS record for NULL-batch default; '\
                        '==> leave batch STATUS as IS, set preload_status={5}, AND WARN (here!) of such an anomaly, recommending manual fix of the DB missing NULL-batch records.'.format(
                        MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, found_curr_status_batch, preload_status), flush=True)
                # ======> NOTE: Q: where to distinguish between ZZZ_ONDECK_READY-4-PROCESSING -vs- READY_4_RE-PROCESSING?
                # Will need to let it drop on through the rest of the ladder, where that is automatically handled.
                # But first......
                # Rather than just emitting the WARNING, and realizing that it may not even be seen, much less resolved,
                # What about going ahead and auto-resolving it right here, INSERTing the missing NULL-batch version of this record?
                # It could be exactly the same, with its SUBJECT_ID and all of its de-id'd targets, just with a NULL batch:

                if len(curr_status_results_for_this_batch) < 1:
                    print('r3m0 ERROR: Preloader sees curr_status_results_for_this_batch < 1; how did we even get here, if so? '\
                        'MAYBE ignore, though within this if SHOULD NOT see, LOL.', flush=True)
                else:
                    # len() >= 1
                    if len(curr_status_results_for_this_batch) > 1:
                        print('r3m0 WARNING: Preloader sees curr_status_results_for_this_batch > 1; '\
                            'taking 1st such result and carrying on, but how did we even get here?', flush=True)
                        # Still, just take the 1st one, and carry on
                    ## >= 1, grab the first:
                    first_status_rec = curr_status_results_for_this_batch[0]
                    print('r3m0 DEBUG: Preloader about to utilize and INSERT vals from curr_status_results_for_this_batch == 1, including: '\
                        'accession_num=\'{0}\', uuid=\'{1}\', change_type=\'{2}\', change_seq_id={3}, '\
                        'RESETTING phase_processed={4} to {5}, '\
                        'RESETTING datetime_processed=\'{6}\' to now(), '\
                        'RESETTING batch_name=\'{7}\' to NULL, '.format(
                            first_status_rec['accession_num'],
                            first_status_rec['uuid'],
                            first_status_rec['change_type'],
                            first_status_rec['change_seq_id'],
                            first_status_rec['phase_processed'],
                            MIN_PROCESSING_PHASE,
                            first_status_rec['datetime_processed'],
                            first_status_rec['batch_name']),
                            flush=True)
                    # BATCHES NOTE: STATUS INSERTS need no parallel INSERT into LOCUTUS_ALL_BATCHES_TABLE.
                    # AND, leaving the subject_id & object_info* AND De-ID target fields empty,
                    # since this is for a different (NULL) batch anyhow:
                    # NOTE: batches_insert_val_clause is forced to NULL
                    missing_STATUSrec_update_sql='INSERT INTO {0} ( accession_num, datetime_processed, '\
                                            'phase_processed, uuid,  '\
                                            'change_seq_id, change_type, '\
                                            'active, batch_name ) '\
                                            'VALUES (\'{1}\', now(), '\
                                            '{2}, \'{3}\', '\
                                            '{4}, \'{5}\', '\
                                            'True, NULL) ;'.format(
                                            self.status_table,
                                            first_status_rec['accession_num'],
                                            MIN_PROCESSING_PHASE,
                                            first_status_rec['uuid'],
                                            first_status_rec['change_seq_id'],
                                            first_status_rec['change_type'])
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print(self.locutus_settings.SQL_OUT_PREFIX, missing_STATUSrec_update_sql, flush=True)
                        # SQL blank line, now that at end of all this method's SQL:
                        print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
                    #
                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.preload_accession(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                        self.LocutusDBconnSession.execute(missing_STATUSrec_update_sql)
                    else:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.preload_accession(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                    #

                    """
                    AND need the DISTINCT uuid as well:
                    FIRST PRINT, before ANY INSERT:
                    ROW should have values from....
                        preupdate_sql_select='SELECT accession_num, phase_processed, uuid, '\
                                        'change_type, change_seq_id, '\
                                        'datetime_processed, '\
                                        'batch_name, '\
                                        '{0}, '\
                                        '{1} FROM {2} WHERE {3} '\
                                        'AND active ;'.format(
                    """


                # Q: what about trying a simple AND NOT rather than OR NOT?  Will this cause any downstream issues with the NOT len(curr_status_results_for_batchless_default)?
                # UPDATING the below "elif num_status_records == 0 or not len(curr_status_results_for_batchless_default):" to an "AND not"
            # NOTE: the below elif is now a SEPARATE if ladder, now that it is nested within an else NOT "if num_status_records > 1 and num_status_records_DISTINCT_uuids > 1:""
            if num_status_records == 0 and not len(curr_status_results_for_batchless_default):
                # NOTE: AFTER the above ^^^ check for ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX.
                # reGARDless of its initial_manifest_status!
                # effectively, num_status_records == 0 (even if not exactly, since no curr_status_result_for_batchless_default found)
                # only if NO such status records at all, go ahead and set to PENDING_CHANGE:
                preload_status = self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') w/ (NO num_status_records=={3} '\
                        '*OR* NOT found_curr_status_batch=={4}) '\
                        '==> leave STATUS empty and only INSERT a new MANIFEST w/ preload_status={5}'.format(
                        MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, found_curr_status_batch, preload_status), flush=True)
                ## DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
                #print('{0}DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') with NO num_status_records={3} ==> leave STATUS empty and INSERT new MANIFEST w/ preload_status={4}'.format(
                #    MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, preload_status), flush=True)
                # NOTE: either way, leave DB_STATUS as empty:
                preload_DB_STATUS_operation = None
                # FIXED: OOPS, WAS: preload_DB_STATUS_operation = 'INSERT'

            elif not found_curr_status_batch:
                # NOTE: setting default manifest_status of PENDING_CHANGE if no batchless STATUS records found (though that is handled in the above if)
                # WAS: preload_status = self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                # NOTE: no need for a default of PENDING_CHANGE, right?  TEST with:
                preload_status = "r3m0-ZZZ-UNHANDLED-PRELOAD-not_found_curr_status_batch"
                if len(curr_status_results_for_batchless_default):
                    # NOTE: single STATUS record exists for the default batch (batch_name IS NULL) but NONE yet for this batch
                    # IF other status records, including (hopefully) the default batchless one, then set manifest_status as a fresh ONDECK-4-PROCESSING_CHANGE:
                    preload_status = self.locutus_settings.ONDECK_PROCESSING_CHANGE_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                    # r3m0: NOTE: Q: =====> Shall we PROPAGATE *ALL* such batchless default STATUS records into this batch?
                    # OR, will the if else ladder even fall into this spot, since up towards the top is the following check:
                    #
                    #    if num_status_records > 1 \
                    #    and num_status_records_DISTINCT_uuids > 1 \
                    #    and found_curr_status_batch \
                    #    and num_curr_status_batch_records == num_status_records_DISTINCT_uuids :
                    #       # == 1st case, for batch matching those STATUS records for batchless
                    #       # AND, only if these is a MultiUUID
                    #
                    #    elif num_status_records > 1 \
                    #    and num_status_records_DISTINCT_uuids > 1 \
                    #    and num_curr_status_batch_records < num_status_records_DISTINCT_uuids :
                    #       # == 2nd case, not batch matching of all or any, but still > 1 batchless DISTINCT uuids
                    #       # AND, only if these is a MultiUUID
                    #
                    # So.... NO, it might NOT be caught up above, not if it isn't a Multi-UUID.
                    # Multi-UUIDs to be handled above, but here to catch the comparable Single UUID case.
                    #
                    ###########
                    # r3m0: TODO: Q: could/should they be combined?  is there a reason for the de-coupling of these Single & Multi-UUID cases?
                    ###########

                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') w/ (found num_status_records=={3} '\
                        'AND default batch-less AND NOT found_curr_status_batch=={4}) '\
                        '==> INSERT single new STATUS (from batch-less) AND new MANIFEST w/ preload_status={5}'.format(
                        MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, found_curr_status_batch, preload_status), flush=True)
                ## DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
                #print('{0}DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') with NO num_status_records={3} ==> leave STATUS empty and INSERT new MANIFEST w/ preload_status={4}'.format(
                #    MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, preload_status), flush=True)
                # NOTE: either way, leave DB_STATUS as empty: preload_DB_STATUS_operation = None
                preload_DB_STATUS_operation = 'INSERT'
            else:
                # i.e., (if num_status_records >= 1 AND num_status_records_DISTINCT_uuids <= 1) AND found_curr_status_batch:
                # NOTE: blanket catch up top of any with num_rows==1 and only 1.
                preload_DB_STATUS_operation = 'UPDATE'
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') w/ (at LEAST one num_status_records={3} '\
                        'AND NO Multi-UUIDS [num_status_records_DISTINCT_uuids={4}] AND found_curr_status_batch={5}) '\
                        '==> getting phase_processed for batch_name BEFORE preload_status = {6}.'.format(
                        MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, num_status_records_DISTINCT_uuids, found_curr_status_batch, preload_status), flush=True)

                # w/ each of the following ifs honoring any optionally subsequent details at the end of manifest_status by using the below [:len()] checks,
                # although there is currently no major difference in how any of them are handled, except for the more explanatory print
                # and for the preload_status = ONDECK_PROCESSING_CHANGE_PREFIX +.... -vs- ONDECK_REPROCESSING_CHANGE_PREFIX
                ###############################################
                # NOTE: can just pulling the 1st such UUID in the below curr_status_results_DISTINCT_uuids[0][0]
                # BECAUSE we are in an else where num_status_records==1 AND num_status_records_DISTINCT_uuids==1
                curr_status_DISTINCT_uuid = curr_status_results_DISTINCT_uuids[0][0]
                ###############################################
                # NOTE: pulling in the phase_processed from STATUS
                # so as to NOT have to rely entirely on the MANIFEST_STATUS:
                # NOTE: adding DISTINCT() around phase_processed, so that even if multiples (for this batch, even!), for whatever reason:

                # BATCHES NOTE: subsequent SQL utilizing the batches_select_clause w/ internal preload_batches_enable as per preload_batch_name
                preupdate_sql_select_DISTINCT_uuid_phases='SELECT DISTINCT(phase_processed) '\
                                                'FROM {0} WHERE {1} AND uuid=\'{2}\' '\
                                                'AND {3} '\
                                                'AND active '\
                                                'ORDER BY phase_processed ;'.format(
                                                self.status_table,
                                                accession_range_where,
                                                curr_status_DISTINCT_uuid,
                                                batches_select_clause
                                                )
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
                    print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_DISTINCT_uuid_phases), flush=True)
                ###################
                # DO IT! Go ahead and actually execute that preupdate_sql_select_DISTINCT_uuids
                # in order to preserve those values in the output log (as well as in the retired accessions)
                # Now, use the initial_manifest_status, AND a count of the num_status_records_DISTINCT_uuids,
                # to compute a more applicable preload_status:
                #############
                num_status_records_DISTINCT_uuid_phases = 0
                curr_uuid_phase_processed = 0
                curr_status_results_DISTINCT_uuid_phases = []
                preupdate_sql_select_results_DISTINCT_uuid_phases =  self.LocutusDBconnSession.execute(preupdate_sql_select_DISTINCT_uuid_phases)
                for preupdate_sql_select_result_DISTINCT_uuid_phases in preupdate_sql_select_results_DISTINCT_uuid_phases.fetchall():
                    num_status_records_DISTINCT_uuid_phases += 1
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result_DISTINCT_uuid_phases), flush=True)
                    curr_status_results_DISTINCT_uuids.append(preupdate_sql_select_result_DISTINCT_uuid_phases)
                    curr_uuid_phase_processed = preupdate_sql_select_result_DISTINCT_uuid_phases[0]
                    # NOTE: ^^^ this curr_uuid_phase_processed is storing ONLY the last (maximum) phase_processed;
                    # be sure to see curr_status_results_DISTINCT_uuids if multiple such records.

                if num_status_records_DISTINCT_uuid_phases != 1:
                    preload_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_OTHER_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('ERROR: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                            'w/ uuid=\'{3}\' w/ MULTIPLE ({4}) num_status_records_DISTINCT_uuid_phases '\
                            '==> Erroring out, since ONDECK with MULTIPLE_CHANGE_UUIDS new preload_status = {4}.'.format(
                            MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, curr_status_DISTINCT_uuid, num_status_records_DISTINCT_uuid_phases, preload_status), flush=True)
                    successful = False
                    # TODO: throw/emit a ValueError() of some sort here for this unexpected situation?
                    #print('{0}.preload_accession(): ERROR: LOCUTUS_WORKSPACES_ENABLE set, but LOCUTUS_WORKSPACE_NAME *not* configured (and non-empty)'.format(
                    #    CLASS_PRINTNAME), flush=True)
                    #raise ValueError('Locutus {0}.Setup() ERROR: LOCUTUS_WORKSPACES_ENABLE set, but LOCUTUS_WORKSPACE_NAME *not* configured (and non-empty)'.format(
                    #                    CLASS_PRINTNAME))
                    # NOPE, NOT YET: for now, just emit this as a print(ERROR), with a MANIFEST_STATUS = ERROR:suffix
                elif ( (initial_manifest_status[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED) \
                    or (initial_manifest_status[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX)
                    or (curr_uuid_phase_processed == MAX_PROCESSING_PHASE)):
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                            'w/ uuid=\'{3}\' has phase_processed={4} (MAX_PROCESSING_PHASE) '\
                            '==> evaluating for PROCESSING -vs- RE-PROCESSING if LOCUTUS_*_DICOM_FORCE_REPROCESS_ACCESSION_STATUS...'.format(
                            MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, curr_status_DISTINCT_uuid, curr_uuid_phase_processed), flush=True)
                    # potentially.... ONDECK_REPROCESSING_CHANGE_PREFIX,
                    # ... that is, IF the selected MODULE's corresponding FORCE_REPROCESS has been set:
                    if (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM \
                        and self.locutus_settings.LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS):
                        # i.e., if FORCE_REPROCESS:
                        # WAS more VERBOSELY: preload_status = ONDECK_REPROCESSING_CHANGE_PREFIX + PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix + PREVIOUS_STATUS_DELIM + initial_manifest_status
                        #   and more concisely, without the trailing + PREVIOUS_STATUS_DELIM + initial_manifest_status:
                        preload_DB_STATUS_operation = 'UPDATE'
                        preload_status = self.locutus_settings.ONDECK_REPROCESSING_CHANGE_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                        # 4/09/2026 FIX: preload to RETAIN former STATUS record (and its deid targets) if REPROCESSING:
                        # NOTE: FORMERLY SAID: '*AND* FORCE_REPROCESS ==> UPDATE STATUS & MANIFEST as ONDECK with new preload_status = {4}'.format(...
                        # NOTE: now leaving the STATUS, UPDATING ONLY the MANIFEST
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                                'previously PROCESSED/PREVIOUSLY_PROCESSED w/ SINGLE num_status_records={3}, '\
                                '*AND* FORCE_REPROCESS ==> UPDATE MANIFEST (ONLY, retain STATUS) as ONDECK with new preload_status = {4}'.format(
                                MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, preload_status), flush=True)
                        ## DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
                        #print('{0}DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') previously PROCESSED/PREVIOUSLY_PROCESSED w/ SINGLE num_status_records={3} ==> UPDATE STATUS & MANIFEST as ONDECK with new preload_status = {4}'.format(
                        #    MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, preload_status), flush=True)
                    elif ( (initial_manifest_status[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED) \
                        or (initial_manifest_status[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX)):
                        # the selected MODULE's corresponding FORCE_REPROCESS has NOT been set,
                        # so leave these as is......
                        preload_DB_STATUS_operation = None
                        BYPASS_preload_DB_MANIFEST_operation = True
                        # BUT STILL do need the preload_status status explicitly set to the initial:
                        # lest this MANIFEST_OUTPUT emit a manifest_status of r3m0-ZZZ-UNHANDLED-PRELOAD-
                        preload_status = initial_manifest_status
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                                'previously w/ manifest_status of PROCESSED/PREVIOUSLY_PROCESSED '\
                                'w/ SINGLE num_status_records={3} '\
                                '*BUT* *NOT* FORCE_REPROCESS ==> leaving STATUS & MANIFEST w/ manifest_status = {4}'.format(
                                MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, preload_status), flush=True)
                    else:
                        # the selected MODULE's corresponding FORCE_REPROCESS has NOT been set,
                        # so leave these as is......
                        # UNLESS we have gotten here due to MAX_PROCESSING_PHASE
                        # and have a manifest_status other than PROCESSED/PREVIOUSLY_PROCESSED,
                        # which we will NOT be RE-Processing (since FORCE_REPROCESS disabled);
                        # SO.... howzabout just resetting its manifest_status to PROCESSED?
                        preload_DB_STATUS_operation = 'UPDATE'
                        # Q: but... shall it use the suffix? likely NOT, but for try it on for size:
                        # NOT: preload_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                        # NOTE: Nope, ends up looking like "PROCESSED:r3m0_testRePreLoad_of_2025june10g_withPhaseProc_andForceReproc",
                        # which is certainly intriguing, but will complicate the Summarizer STATUS_OUT for PROCESSED,
                        # so..... reset to PROCESSED, sans suffix:
                        preload_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                                'previously phase_processed=MAX (but manifest_status OTHER than PROCESSED/PREVIOUSLY_PROCESSED) '\
                                'w/ SINGLE num_status_records={3} '\
                                '*BUT* *NOT* FORCE_REPROCESS ==> RESETTING manifest_status = {4}'.format(
                                MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, preload_status), flush=True)
                else:
                    # ONDECK_PROCESSING_CHANGE_PREFIX:
                    # examples: NOT_FOUND (OK!), even if it had been subsequently staged (hence the num_status_records==1) :-D
                    #   or self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX, even already resolved to  num_status_records==1) :-D
                    # Just like the above case(s), now give the basic minimal ONDECK_OTHER_PREFIX
                    # WAS more VERBOSELY: preload_status = ONDECK_PROCESSING_CHANGE_PREFIX + PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix + PREVIOUS_STATUS_DELIM + initial_manifest_status
                    #   and more concisely, without the trailing + PREVIOUS_STATUS_DELIM + initial_manifest_status:
                    preload_status = self.locutus_settings.ONDECK_PROCESSING_CHANGE_PREFIX + self.locutus_settings.PROCESSING_SUFFIX_DELIM + preload_manifest_status_suffix
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') '\
                            'previously OTHER (non-fully-PROCESSED) w/ SINGLE num_status_records={3} '\
                            '==> UPDATE STATUS & MANIFEST as ONDECK with new preload_status = {4}: .'.format(
                            MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, preload_status), flush=True)
                    ## DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
                    #print('{0}DEBUG: preload_accession( initial_manifest_status={1}, accession_num=\'{2}\') previously OTHER (non-PROCESSED) w/ SINGLE num_status_records={3} ==> UPDATE STATUS & MANIFEST as ONDECK with new preload_status = {4}: .'.format(
                    #    MANIFEST_OUTPUT_PREFIX, initial_manifest_status, accession_num, num_status_records, preload_status), flush=True)

        # Part 1b: UPDATE the STATUS table records (no need to INSERT any NOT_FOUND)...
        if preload_DB_STATUS_operation:
            # 4/09/2026 FIX: preload to RETAIN former STATUS record (and its deid targets), but ONLY if REPROCESSING:
            bypass_insert_after_all = False

            # NOTE: STATUS won't ever do an INSERT here, only either UPDATE or *potentially* leave as is (e.g., if the same manifest attributes already, but still need to UPDATE the manifest_status itself):
            # Manifest-Once ENHANCEMENT: this might indeed now do an INSERT if not yet found for this particular batch AND found for the default batch-less STATUS record.
            # =====> meaning that we'll need to SAVE such a DEFAULT batchless set of values for it:
            #       == curr_status_result_for_batchless_default, as from above: if this_acc_batch_name is None or this_acc_batch_name == '':
            if preload_DB_STATUS_operation=='UPDATE':
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: =====> preloader to **RETIRE-STATUS** via UPDATE of the existing STATUS records with the latest and greatest subj+obj attributes...', flush=True)
                ## MANIFEST_OUTPUT DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
                #print(f'{MANIFEST_OUTPUT_PREFIX}:DEBUG: =====> preloader to **RETIRE-STATUS** via UPDATE of the existing STATUS records with the latest and greatest subj+obj attributes...')

                #######################################
                # NOTE: For ONDECK_REPROCESSING_CHANGE_PREFIX, be sure to retain former_deid_targets_to_retain,
                # as in below SELECT, to maintain the running list DeID targets for this accession
                # (even though, technically, retiring the former STATUS record).
                # This will eventually be a moot point as we move into Project FANs,
                # w/ projects/batches N:1 accessions 1:N jobs/logs
                #######################################

                # NOTE: first use the less spicy retire_accession_status_only()
                subaccessions_only=False
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                    # 4/09/2026 FIX: preload to RETAIN former STATUS record (and its deid targets), but ONLY if REPROCESSING:
                    # WAS: print('{0}.preload_accession(): retiring STATUS for THIS batch_name since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.preload_accession(): NO LONGER retiring STATUS for THIS batch_name since Carrying On w/ old targets (even w/ LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES)'.format(CLASS_PRINTNAME), flush=True)
                    except_change_seq=None
                    # NOTE: retire ONLY this batch's STATUS record(s)
                    # BATCHES NOTE: including batch info for Preloader:
                    # 4/09/2026 FIX: preload to RETAIN former STATUS record (and its deid targets) if REPROCESSING:
                    # NOTE: ====> HANG ON, may NOT want to actually RETIRE, but to CARRY ON retaining the previously de-identified targets:

                    # ====> TODO: WHEN disabling the following call to retire_accession_status_only(),
                    # WILL NEED TO also CHANGE the subsequent INSERT to an actual UPDATE!!!!!!!
                    # OR JUST DO THE UPDATE HERE, and BYPASS the subsequent INSERT.
                    # Yargh, who wrote this like this?  silly self.* ;-)
                    # LOL.
                    # AND, the ONLY UPDATE NEEDED to the STATUS record (if any at all)
                    # MIGHT be the datetime_processed value?  NO, NOT EVEN THAT.
                    # Allow the STATUS record to remain as the last ACTUAL processing, not just a Preload.
                    # The MANIFEST record, however, will be timestamp updated ;-)
                    #
                    # TODO: =====> SO... just need to ensure to DISABLE the subsequent INSERT.

                    #WAS: self.retire_accession_status_only(accession_num, except_change_seq, batch_only=preload_batches_enable, batch_name=preload_batch_name)
                    # INSTEAD, SET something like:
                    bypass_insert_after_all = True
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.preload_accession(): **NOT** retiring STATUS (regardless of **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES)'.format(CLASS_PRINTNAME), flush=True)
                #

            # 4/09/2026 FIX: preload to RETAIN former STATUS record (and its deid targets) if REPROCESSING:
            if not bypass_insert_after_all:
                # NOTE: then INSERT the new STATUS
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: =====> preloader to (A) INSERT the new STATUS record(s) with the latest and greatest subj+obj attributes... '\
                        '(len(curr_status_results_for_this_batch)=={0} & len(curr_status_results_for_batchless_default)=={1})'.format(
                            len(curr_status_results_for_this_batch), len(curr_status_results_for_batchless_default)), flush=True)
                ## MANIFEST_OUTPUT DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
                #print(f'{MANIFEST_OUTPUT_PREFIX}:DEBUG: =====> preloader to INSERT the new STATUS record with the latest and greatest subj+obj attributes...')

                # INSTEAD of looping through the curr_status_results_for_this_batch,
                # just create a new one to INSERT, as from: curr_status_result_for_batchless_default
                # EXCEPT, perhaps, for the former_de-id_targets_to_retain:
                curr_status_record = None
                former_deid_targets_to_retain = None

                # BATCHES NOTE: looking to INSERT potentially MORE than just one STATUS record,
                # as per curr_status_results_for_batchless_default (was curr_status_result_for_batchless_default)
                # i.e., if preload_status = self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX

                curr_status_results_for_batchless_default_uuids = [item['uuid'] for item in curr_status_results_for_batchless_default]
                print('r3m0 DEBUG: len(curr_status_results_for_batchless_default)={0}, curr_status_results_for_batchless_default={1}'.format(
                    len(curr_status_results_for_batchless_default), curr_status_results_for_batchless_default
                ), flush=True)

                curr_status_results_for_this_batch_uuids = [item['uuid'] for item in curr_status_results_for_this_batch]
                print('r3m0 DEBUG: len(curr_status_results_for_this_batch)={0}, curr_status_results_for_this_batch={1}'.format(
                    len(curr_status_results_for_this_batch), curr_status_results_for_this_batch
                ), flush=True)

                # NOTE: below comparison limited to ONLY the uuid field (and maybe the accession_num?) since other record fields will certainly vary
                status_uuids_ONLY_to_insert = [item for item in curr_status_results_for_batchless_default_uuids if item not in curr_status_results_for_this_batch_uuids]
                print('r3m0 DEBUG: len(status_uuids_ONLY_to_insert)={0}, status_uuids_ONLY_to_insert={1},'.format(
                    len(status_uuids_ONLY_to_insert), status_uuids_ONLY_to_insert
                ), flush=True)
                # HOTFIX HACK WARNING: using the hardcoded expected STATUS_RECORD_UUUID_FIELDNUM from the SELECT acc, phase, uuid, ..
                # TODO: consider an alternate approach? First, though, confirm that STATUS_RECORD_UUID_FIELDNUM does indeed return the expected base 0 UUID field:
                status_uuids_to_insert = [item for item in curr_status_results_for_batchless_default if item[STATUS_RECORD_UUID_FIELDNUM] in status_uuids_ONLY_to_insert]
                print('r3m0 DEBUG: len(status_uuids_to_insert)={0}, status_uuids_to_insert={1},'.format(
                    len(status_uuids_to_insert), status_uuids_to_insert
                ), flush=True)

                if len(curr_status_results_for_this_batch):
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('DEBUG: =====> preloader to (B) INSERT into batch {0} missing of {1} STATUS record(s) FROM the curr_status_results_for_batchless_default, '\
                            'w/ {2} previously existing for this batch_name=\'{3}\' prior to just retiring; LIST = {4}'.format(
                            len(status_uuids_to_insert), len(curr_status_results_for_batchless_default), len(curr_status_results_for_this_batch),
                            preload_batch_name, status_uuids_to_insert), flush=True)
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('DEBUG: =====> preloader to (C) INSERT into batch ALL {0} of STATUS record(s) FROM the curr_status_results_for_batchless_default, '\
                            'since NONE previously existed for this batch_name=\'{1}\'; LIST = {2}'.format(
                            len(status_uuids_to_insert), preload_batch_name, status_uuids_to_insert), flush=True)

                status_uuids_to_insert_counter = 0
                # BATCHES NOTE: wrapping the subsequent INSERT into a foreach curr_status_results_for_batchless_default:
                for curr_status_record in status_uuids_to_insert:
                    # BEWARE: mid-Dev had introduced a few temporary records w/ phase_processed=NULL
                    # resulting in "TypeError: 'NoneType' object is not subscriptable" from the following:
                    if curr_status_record and 'phase_processed' in curr_status_record and curr_status_record['phase_processed']:
                        curr_phase_processed = curr_status_record['phase_processed']
                    else:
                        curr_phase_processed = MIN_PROCESSING_PHASE

                    curr_change_seq_id = curr_status_record['change_seq_id']
                    curr_change_type = curr_status_record['change_type']
                    curr_uuid = curr_status_record['uuid']

                    status_uuids_to_insert_counter += 1
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('DEBUG: =====> preloader ABOUT to INSERT STATUS record(s) #{0} of {1} for batch_name=\'{1}\'.'.format(
                            status_uuids_to_insert_counter, len(status_uuids_to_insert), preload_batch_name), flush=True)

                    # NOTE: subsequent SQL utilizing the batches_insert_val_clause w/ internal preload_batches_enable as per preload_batch_name
                    # BATCHES NOTE: STATUS INSERTS need no parallel INSERT into LOCUTUS_ALL_BATCHES_TABLE.
                    update_sql='INSERT INTO {0} ( {1}, accession_num, datetime_processed, '\
                                            'phase_processed, change_seq_id, uuid,  '\
                                            'change_type, {9}, active, '\
                                            'batch_name ) '\
                                            'VALUES ({2}, \'{3}\', now(), '\
                                            '{4}, {5}, \'{6}\', '\
                                            '\'{7}\', \'{8}\', True, '\
                                            '{10} ) ;'.format(
                                            self.status_table,
                                            preload_manifest_columns_sans_status,
                                            preload_insert_info,
                                            accession_num,
                                            curr_phase_processed,
                                            curr_change_seq_id,
                                            curr_uuid,
                                            curr_change_type,
                                            former_deid_targets_to_retain,
                                            self.status_target_field,
                                            batches_insert_val_clause)
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
                        # SQL blank line at end of all this method's SQL:
                        print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
                    #
                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.preload_accession(): INSERT STATUS into DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                        self.LocutusDBconnSession.execute(update_sql)
                    else:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.preload_accession(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                    #

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # END of {1}.preload_accession(accession=\'{2}\') STATUS updates.'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                    accession_num), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

        ################### ################### ################### ################### ###################
        # NOTE: END OF POTENTIAL Preloader sub-divided method: preload_accession_status_only()
        # SEE ALSO:
        #           preload_accession_manifest_only()
        #           preload_accession_batches_only()
        ################### ################### ################### ################### ###################


        ################### ################### ################### ################### ###################
        # NOTE: START OF POTENTIAL Preloader sub-divided method: preload_accession_manifest_only()
        # SEE ALSO:
        #           preload_accession_status_only()
        #           preload_accession_batches_only()
        ################### ################### ################### ################### ###################

        #
        # Part 2: MANIFEST table update...
        #
        # SQL comment prior to the actual SQL command:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.preload_accession(accession=\'{2}\') MANIFEST table updates:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                accession_num), flush=True)
            print('{0}-- # {1}.preload_accession() for MANIFEST:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME), flush=True)

        # Part 2a: generate preload_DB_MANIFEST_operation from preload_DB_MANIFEST_operation initial_manifest_status (if existing MANIFEST):
        preload_DB_MANIFEST_operation = None
        # NOTE: MANIFEST shall only ever require an UPDATE or INSERT (if previously NOT_FOUND) here:
        if BYPASS_preload_DB_MANIFEST_operation:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('DEBUG: bypassing Preloader Part 2 with BYPASS_preload_DB_MANIFEST_operation={0}'.format(BYPASS_preload_DB_MANIFEST_operation))
        elif initial_manifest_status==self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND:
            # so NOW, the only actual INSERT of the MANIFEST table, since it was **really** NOT_FOUND
            # (regardless of how many STATUS records, the MANIFEST shall still require this preload INSERT)
            preload_DB_MANIFEST_operation = 'INSERT'
        else:
            # (and, regardless of how many STATUS records, the MANIFEST shall still require a preload UPDATE)
            preload_DB_MANIFEST_operation = 'UPDATE'


        # Part 2b: UPDATE &/or INSERT (once preretired) the MANIFEST table records...
        if preload_DB_MANIFEST_operation and not BYPASS_preload_DB_MANIFEST_operation:
            # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
            accession_range_where = 'accession_num=\'{0}\''.format(accession_num)

            # FIRST, a commented SELECT version of the actual INSERT command:
            # BATCHES NOTE: selecting across all batch_names:
            preupdate_sql_select='SELECT accession_num, manifest_status, \'{0}\' as proposed_manifest_status, '\
                                        'last_datetime_processed, now() as proposed_last_datetime_processed, '\
                                        'batch_name, '\
                                        '{1} FROM {2} WHERE {3} '\
                                        'AND active ;'.format(
                                        preload_status,
                                        preload_select_info,
                                        self.manifest_table,
                                        accession_range_where)
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
            ###################
            # DO IT! Go ahead and actually execute that preupdate_sql_select
            # in order to preserve those values in the output log (as well as in the retired accessions)
            num_manifest_records = 0
            found_curr_manifest_batch = False
            preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
            for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
                num_manifest_records += 1

                this_acc_subj_id = preupdate_sql_select_result['subject_id']
                this_acc_batch_name = preupdate_sql_select_result['batch_name']

                # BATCHES NOTE: either NOT enabled and using default NULL batch, or enabled and matching:
                if (not preload_batches_enable and (this_acc_batch_name is None or this_acc_batch_name=='')) \
                    or (preload_batches_enable and (this_acc_batch_name == preload_batch_name)):
                    found_curr_manifest_batch = True
                #####
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)

            if preload_batch_name and (not found_curr_manifest_batch and not BYPASS_preload_DB_MANIFEST_operation and preload_DB_MANIFEST_operation=='UPDATE') :
                # NOTE: without having found the current batch in the MANIFEST TABLE for this particular accession,
                # do NOT do an UPDATE after all, leaving the other batch MANIFEST records for this accession as is.
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: =====> preloader did NOT find this accession already in MANIFEST Table for this batch (preload_batches_enable={0}, batch_name=\'{1}\'); '\
                        'changing potential UPDATE of MANIFEST TABLE to an INSERT for batch \'{1}\' ...'.format(preload_batches_enable, preload_batch_name), flush=True)
                preload_DB_MANIFEST_operation='INSERT'

            ###################
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
                print('DEBUG: preload_accession( initial_manifest_status={0}, accession_num=\'{1}\') FOUND num_manifest_records={2} for preload_batches_enable={3}, batch_name=\'{4}\'; OVERALL MATCH to batch ?= {5}'.format(
                    initial_manifest_status, accession_num, num_manifest_records, preload_batches_enable, preload_batch_name, found_curr_manifest_batch), flush=True)

            # and NOW, the actual INSERT to the MANIFEST table:
            if preload_DB_MANIFEST_operation=='UPDATE':
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: =====> preloader to **RETIRE-MANIFEST** via UPDATE of the existing MANIFEST records with the latest and greatest subj+obj attributes '\
                        ' (for preload_batches_enable={0}, batch_name=\'{1}\')...'.format(preload_batches_enable, preload_batch_name), flush=True)
                ## MANIFEST_OUTPUT DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
                #print(f'{MANIFEST_OUTPUT_PREFIX}:DEBUG: =====> preloader to **RETIRE-MANIFEST** via UPDATE of the existing MANIFEST records with the latest and greatest subj+obj attributes...')

                # NOTE: first use the less spicy retire_accession_manifest_only()
                subaccessions_only=False
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.preload_accession(): retiring MANIFEST since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                    # NOTE: TODO: when (if) doing an UPDATE, will need to ensure that it is ONLY for the one record of this same batch:
                    # BATCHES NOTE: including batch info for Preloader:
                    success = self.retire_accession_manifest_only(accession_num, batch_only=preload_batches_enable, batch_name=preload_batch_name)
                    if not success:
                        successful = False
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.preload_accession(): **NOT** retiring MANIFEST **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)

            # NOTE: then INSERT the new MANIFEST
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('DEBUG: =====> preloader to (D) INSERT the new MANIFEST records with the latest and greatest subj+obj attributes...')
            ## MANIFEST_OUTPUT DEBUG print, prefaced by MANIFEST_OUTPUT_PREFIX for a quick+easy grep of ONLY these DEBUG clues and results during a run
            #print(f'{MANIFEST_OUTPUT_PREFIX}:DEBUG: =====> preloader to INSERT the new MANIFEST records with the latest and greatest subj+obj attributes...')

            # NOW (w/ above UPDATE doing a retire), need to INSERT either way; shift tab back out.... outdent:
            # INSERT into the MANIFEST table:

            # NOTE: subsequent SQL utilizing the batches_insert_val_clause w/ internal preload_batches_enable as per preload_batch_name
            # BATCHES NOTE: MANIFEST INSERTS also have parallel INSERT to LOCUTUS_ALL_BATCHES_TABLE.
            # TODO: can start switching this call over to the new Settings.insert_batches_table(), to allow reuse within the De-ID modules.
            update_sql='INSERT INTO {0} ( {1}, manifest_status, accession_num, last_datetime_processed, '\
                                        '    active, '\
                                        '    batch_name ) '\
                                        'VALUES ({2}, \'{3}\', \'{4}\', now(), '\
                                        '    True, '\
                                        '    {5} ) ;'.format(
                                        self.manifest_table,
                                        preload_manifest_columns_sans_status,
                                        preload_insert_info,
                                        preload_status,
                                        accession_num,
                                        batches_insert_val_clause)
            # BATCHES NOTE: MANIFEST INSERTS also have parallel INSERTs to LOCUTUS_ALL_BATCHES_TABLE,
            # as implemented further below, at preload_DB_BATCHES_operation = 'INSERT':

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
                # SQL blank line at end of all this method's SQL:
                print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
            #
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.preload_accession(): INSERT MANIFEST into DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                self.LocutusDBconnSession.execute(update_sql)
            else:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.preload_accession(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            #

            ################### ################### ################### ################### ###################
            # NOTE: START OF POTENTIAL Preloader sub-divided method: preload_accession_batches_only()
            # SEE ALSO:
            #           preload_accession_status_only()
            #           preload_accession_manifest_only()
            ################### ################### ################### ################### ###################

            ##########
            #
            # Part 3: LOCUTUS_ALL_BATCHES_TABLE table update, as nested within Part 2 MANIFEST (following Part 2b: UPDATE &/or INSERT...)
            # since only applicable if doing an INSERT/UPDATE to the MANIFEST TABLE
            #
            # SQL comment prior to the actual SQL command:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
                print('{0}-- # START of {1}.preload_accession(accession=\'{2}\') MANIFEST -> LOCUTUS_ALL_BATCHES_TABLE updates:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                    accession_num), flush=True)
                print('{0}-- # {1}.preload_accession() for LOCUTUS_ALL_BATCHES_TABLE: {2}'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE), flush=True)
                print('{0}-- # {1}.preload_accession():'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME), flush=True)
            # NOTE: allow the below SELECT to span all batches for this accession_num in this workspace,
            # since the resultint num_status_records count is later used to determine if a STATUS record even exists at all:
            # WAS w/:                       'AND batch_name=\'{4}\' '
            # BATCHES NOTE: leave all possibily module, workspace, and batches in the following query by accession_num,
            # to filter down to this module, workspace, and batch from the ALL_BATCHES_TABLE subsequently:
            preupdate_sql_select='SELECT accession_num, '\
                                            'module, '\
                                            'workspace, '\
                                            'batch_name, '\
                                            'subject_id, '\
                                            'manifest_status, '\
                                            'last_datetime_processed '\
                                            'FROM {0} WHERE {1} '\
                                            'AND active ;'.format(
                                            self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE,
                                            accession_range_where)
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
            ###################
            # DO IT! Go ahead and actually execute that preupdate_sql_select
            # in order to preserve those values in the output log (as well as in the retired accessions)
            # Now, use the initial_manifest_status, AND a count of the num_status_records,
            # to compute a more applicable preload_status:
            #############
            num_global_batch_records = 0
            found_curr_global_batch = False
            preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
            for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
                num_global_batch_records += 1

                this_global_acc_num = preupdate_sql_select_result['accession_num']
                this_global_module = preupdate_sql_select_result['module']
                this_global_workspace = preupdate_sql_select_result['workspace']
                this_global_batch_name = preupdate_sql_select_result['batch_name']
                this_global_subj_id = preupdate_sql_select_result['subject_id']

                if preload_batches_enable and \
                    (this_global_batch_name == preload_batch_name \
                    and this_global_workspace == preload_workspace_name \
                    and this_global_module == preload_module_name) :
                    # USING: preload_batch_name AND preload_module_name & preload_workspace_name EXPLICITLY here IN the global BATCHES table:
                    found_curr_global_batch = True
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)

            # NOTE: the global BATCHES table uses only the subject_id, no object_info, etc.
            # So, no need for the full preload_update_info, etc., just the subject_id:

            preload_DB_BATCHES_operation = 'TBD'
            # NOTE: no worries about retiring these old BATCH records
            if num_global_batch_records > 0 and found_curr_global_batch:
                preload_DB_BATCHES_operation = 'UPDATE'
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: =====> preloader to UPDATE the existing BATCH record with the latest subject ID + manifest_status ...')
                # and DO the UPDATE directly here, since now retiring of BATCH records:

                # NOTE: subsequent SQL utilizing the batches_select_clause (for UPDATE selection) w/ preload_batches_enable + preload_batch_name
                update_sql='UPDATE {0} '\
                                        'SET manifest_status=\'{1}\', '\
                                        'subject_id=\'{2}\', '\
                                        'last_datetime_processed=now() '\
                                        'WHERE {3} '\
                                        'AND module=\'{4}\' '\
                                        'AND workspace=\'{5}\' '\
                                        'AND {6} '\
                                        'AND active ;'.format(
                                        self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE,
                                        preload_status,
                                        preload_subject_id,
                                        accession_range_where,
                                        preload_module_name,
                                        preload_workspace_name,
                                        batches_select_clause)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
                    # nah, don't await SQL blank line until end of all this method's SQL:
                    print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
                #
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.preload_accession(): UPDATE BATCH into DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                    self.LocutusDBconnSession.execute(update_sql)
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.preload_accession(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                #
            else:
                preload_DB_BATCHES_operation = 'INSERT'
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: =====> preloader to (E) INSERT the new BATCH record with the latest subject ID + manifest_status ...')
                # and DO the INSERT directly here:

                # NOTE: subsequent SQL utilizing the batches_insert_val_clause w/ preload_batches_enable + preload_batch_name
                # BATCHES NOTE: MANIFEST INSERTS also have parallel INSERT to LOCUTUS_ALL_BATCHES_TABLE.
                # TODO: can start switching this call over to the new Settings.insert_batches_table(), to allow reuse within the De-ID modules.
                update_sql='INSERT INTO {0} ( accession_num, module, workspace, batch_name, '\
                                        'subject_id, manifest_status, last_datetime_processed, active ) '\
                                        'VALUES (\'{1}\', \'{2}\', \'{3}\', {4}, '\
                                        '\'{5}\', \'{6}\', now(), True) ;'.format(
                                        self.locutus_settings.LOCUTUS_ALL_BATCHES_TABLE,
                                        accession_num,
                                        preload_module_name,
                                        preload_workspace_name,
                                        batches_insert_val_clause,
                                        preload_subject_id,
                                        preload_status)
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
                    # SQL blank line at end of all this method's SQL:
                    print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
                #
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.preload_accession(): INSERT BATCH into DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                    self.LocutusDBconnSession.execute(update_sql)
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.preload_accession(): **NOT** inserting into DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                #

            ###################
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
                print('DEBUG: preload_accession( initial_manifest_status={0}, accession_num=\'{1}\') FOUND num_global_batch_records={2} for preload_batches_enable={3}, batch_name=\'{4}\' *without* DISTINCT(uuid); MATCH ?= {5}'.format(
                    initial_manifest_status, accession_num, num_global_batch_records, preload_batches_enable, preload_batch_name, found_curr_global_batch), flush=True)
                print('{0}-- # END of {1}.preload_accession(accession=\'{2}\') MANIFEST -> LOCUTUS_ALL_BATCHES_TABLE updates.'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                    accession_num), flush=True)
                print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

            ################### ################### ################### ################### ###################
            # NOTE: END OF POTENTIAL Preloader sub-divided method: preload_accession_batches_only()
            # SEE ALSO:
            #           preload_accession_status_only()
            #           preload_accession_manifest_only()
            ################### ################### ################### ################### ###################

            ##########

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # END of {1}.preload_accession(accession=\'{2}\') MANIFEST updates.'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                accession_num), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)


        ################### ################### ################### ################### ###################
        # NOTE: END OF POTENTIAL Preloader sub-divided method: preload_accession_manifest_only()
        # SEE ALSO:
        #           preload_accession_status_only()
        #           preload_accession_batches_only()
        ################### ################### ################### ################### ###################


        # end of all this method's umbrella SQL:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
            print('{0}-- # END of {1}.preload_accession(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                accession_num), flush=True)
            print('{0}-- # ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.preload_accession(): Goodbye.'.format(CLASS_PRINTNAME), flush=True)

        return (successful, preload_status)
        # end of preload_accession()


    # NOTE: preset_accession_for_reprocessing()
    # for:  LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS
    # w/    LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS_SUFFIX
    def preset_accession_for_reprocessing(self, accession_num, manifest_status, only_update_manifest_time, preset_select_info, preset_update_info, preset_manifest_status_suffix):
        # helper method to reset the MANIFEST & STATUS records for an accession_num for reprocessing,
        # but only if already previously processed, at least partially;
        # allow all PENDING_CHANGE and PENDING_CHANGE_RADIOLOGY_MERGE to remain as they are.
        #####
        # NOTE: SEE ALSO preload_accession() for a more general pre-loader,
        # with varying ZZZ-ONDECK status values supporting more manifest_status possibilities,
        # as well as preretiring of the accessions.
        #####
        # TODO: considering consolidating these:
        #   * preset_accession_for_reprocessing)
        #   * preload_accession()
        # and eventually even reassess the relevance of retiring,
        # once N:N project:batches and jobs:logs enhancements are introduced
        #####

        successful = True # unless an exception, thrown perhaps

        # safety net, even if PENDING should have already been filtered out:
        if (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING):
            successful = False
            preset_status = manifest_status
            return (successful, preset_status)

        #####
        # NOTE: supporting %MULTIPLE% to ONLY update last_datetime_processed=now()
        # as currently determined by the external caller, via:
        #####
        #only_update_manifest_time = False
        #if ( (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX) \
        #    or (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX) ):
        #    only_update_manifest_time = True
        #####
        # to NOT update their manifest_status in the MANIFEST table
        # NOR to do any updates to their STATUS table,
        # but to ONLY update their times in the MANIFEST table
        #
        # if only_update_manifest_time (for %MULTIPLE%),
        # then leave manifest_status as is:
        #####
        preset_status = manifest_status
        if not only_update_manifest_time:
            preset_status = self.locutus_settings.REPROCESSING_CHANGE_MOMENTARILY
            if preset_manifest_status_suffix and len(preset_manifest_status_suffix) > 0:
                preset_status = self.locutus_settings.REPROCESSING_CHANGE_MOMENTARILY + self.locutus_settings.REPROCESSING_SUFFIX_DELIM + preset_manifest_status_suffix

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.preset_accession_for_reprocessing(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}'.format(
                    CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES), flush=True)
        #
        # SQL comment prior to the actual SQL command:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.preset_accession_for_reprocessing(accession=\'{2}\'):'.format(
                    self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)

        #
        # Part 1: Manifest table
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # {1}.preset_accession_for_reprocessing()part01:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME), flush=True)
        # reuse accession_range_where variable name for consistency with other helpers,
        # but only search via equals with preset for the provided accession_num:
        #
        # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)

        # FIRST, a commented SELECT version of the actual UPDATE command:
        # BATCHES NOTE: TODO: could UPDATE this, but leaving preset_accession_for_reprocessing() for now since deprecating, yeah?
        preupdate_sql_select='SELECT accession_num, manifest_status, \'{0}\' as proposed_manifest_status, '\
                                        'last_datetime_processed, now() as proposed_last_datetime_processed, '\
                                        '{1} FROM {2} WHERE {3} '\
                                        'AND active ;'.format(
                                        preset_status,
                                        preset_select_info,
                                        self.manifest_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        # and NOW, the actual UPDATE of the MANIFEST table:
        ####
        # Part 1: update the MANIFEST table:
        # NOTE: the below WHERE *also* adds in a AND manifest_stats not like \'%MULTIPLE%\;,
        # because those will be reset accordingly in the actual processing of module_onprem_dicom:
        #####
        # NOTE ALSO, though: shouldn't even need these "AND manifest_status not like WHERE..."" clauses;
        # preset_accession_for_reprocessing() will only be called for such manifest_status,
        # the subsequent UPDATE STATUS table won't have such a manifest_status anyhow (at least, not w/o its MANIFEST table)
        # NOT: 'AND manifest_status not like \'PENDING%\' '\
        # NOT: 'AND manifest_status not like \'%MULTIPLE%\''.format(
        update_sql='UPDATE {0} '\
                                        'SET manifest_status=\'{1}\', '\
                                        'last_datetime_processed=now(), '\
                                        '{2} WHERE {3} '\
                                        'AND active ;'.format(
                                        self.manifest_table,
                                        preset_status,
                                        preset_update_info,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
            # nah, don't await SQL blank line until end of all this method's SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.preset_accession_for_reprocessing(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            self.LocutusDBconnSession.execute(update_sql)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.preset_accession_for_reprocessing(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
        #

        #
        # Part 2: Status table
        if only_update_manifest_time:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # SKIPPING {1}.preset_accession_for_reprocessing()part02 since manifest_status={2}'.format(
                        self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, manifest_status), flush=True)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}.preset_accession_for_reprocessing()part02:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME), flush=True)
            # BATCHES NOTE: TODO: could UPDATE this, but leaving preset_accession_for_reprocessing() for now since deprecating, yeah?
            preupdate_sql_select='SELECT accession_num, phase_processed, {0} as proposed_phase_processed, '\
                                            'change_seq_id,  '\
                                            'datetime_processed, now() as proposed_datetime_processed, '\
                                            '{1} FROM {2} WHERE {3} '\
                                            'AND active ;'.format(
                                            MIN_PROCESSING_PHASE,
                                            preset_select_info,
                                            self.status_table,
                                            accession_range_where)
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
            ###################
            # DO IT! Go ahead and actually execute that preupdate_sql_select
            # in order to preserve those values in the output log (as well as in the retired accessions)
            preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
            for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
            ###################
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            # and NOW, the actual UPDATE of the STATUS table:
            # Part 2: update the STATUS table:
            update_sql='UPDATE {0} '\
                                            'SET phase_processed={1}, '\
                                            'datetime_processed=now(), '\
                                            '{2} WHERE {3} '\
                                            'AND phase_processed >= {4} '\
                                            'AND active ;'.format(
                                            self.status_table,
                                            MIN_PROCESSING_PHASE,
                                            preset_update_info,
                                            accession_range_where,
                                            MIN_PROCESSING_PHASE)
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
                # SQL blank line, now that at end of all this method's SQL:
                print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
            #
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.preset_accession_for_reprocessing(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
                self.LocutusDBconnSession.execute(update_sql)
            else:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.preset_accession_for_reprocessing(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            #

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # END of {1}.preset_accession_for_reprocessing(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME,
                accession_num), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.preset_accession_for_reprocessing(): Goodbye.'.format(CLASS_PRINTNAME), flush=True)
        return (successful, preset_status)
        # end of preset_accession_for_reprocessing()


    # And a new helper method to facilitate more complex resolution requirements
    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    # NOTE: if not batch_only, shall retire all STATUS records for this accession in this Workspace;
    #       else, if batch_only, shall ONLY retire those for THIS batch in this Workspace.
    #
    # TODO: combine Settings:retire_accession*() & De-ID:preretire_accession*()
    # incl their sub-methods, (pre)retire_accession_[status/manifest]_only()
    # for a single centralized set of methods to maintain:
    def retire_accession_status_only(self, accession_num, except_change_seq=None, batch_only=False, batch_name=None):
        # helper method to update and retire ONLY the corresponding STATUS records for an accession_num
        successful = True # unless an exception, thrown perhaps
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.retire_accession_status_only(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}, '\
                    'for accession_num=\'{2}\' w/ batch_only={3}, batch_name=\'{4}\', and except_change_seq={5}'.format(
                        CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES,
                        accession_num, batch_only, batch_name, except_change_seq), flush=True)

        # add a batch_clause to the Where clause, if so defined:
        batch_clause = ''
        if batch_only:
            if not batch_name or not len(batch_name):
                batch_clause = 'AND batch_name IS NULL '
            else:
                batch_clause = 'AND batch_name=\'{0}\' '.format(batch_name)

        #
        # SQL comment prior to the actual SQL command:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.retire_accession_status_only(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
        #
        # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs,
        # BUT, if needed, we could explore a 'accession_num LIKE \'{0}.%\'' for any such sub-accessions
        # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)
        if except_change_seq:
            accession_range_where += ' AND change_seq_id != {0}'.format(except_change_seq)
        # FIRST, a commented SELECT version of the actual UPDATE command:
        # BATCHES NOTE: selecting only the specified batch_clause to retire:
        preupdate_sql_select='SELECT batch_name, change_seq_id, (-1*change_seq_id) as proposed_change_seq_id, '\
                            'accession_num, concat(\'-\',accession_num) as proposed_accession_num, '\
                            'active, False as proposed_active, '\
                            'phase_processed, uuid, {3} '\
                                        'FROM {0} WHERE {1} {2} '\
                                        'AND active ORDER BY batch_name, change_seq_id ;'.format(
                                        self.status_table,
                                        accession_range_where,
                                        batch_clause,
                                        self.status_target_field)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        # and NOW, the actual UPDATE:
        update_sql='UPDATE {0} '\
                                        'SET change_seq_id=(-1*change_seq_id), '\
                                        'accession_num=concat(\'-\',accession_num), '\
                                        'active=False '\
                                        'WHERE {1} {2}'\
                                        'AND active ;'.format(
                                        self.status_table,
                                        accession_range_where,
                                        batch_clause)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
            # SQL blank line at end of all this method's SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        #
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.retire_accession_status_only(): updating DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            self.LocutusDBconnSession.execute(update_sql)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.retire_accession_status_only(): **NOT** updating DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
        #

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # END of {1}.retire_accession_status_only(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.retire_accession_status_only(): Goodbye.'.format(CLASS_PRINTNAME), flush=True)
        return successful
        # end of retire_accession_status_only()


    # As recycled from modules_onprem_dicom.py:preretire_accession()
    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    # NOTE: if not batch_only, shall retire all (MANIFEST+STATUS) records for this accession in this Workspace;
    # else, if batch_only, shall ONLY retire those for THIS batch in this Workspace.
    #
    # TODO: combine Settings:retire_accession*() & De-ID:preretire_accession*()
    # incl their sub-methods, (pre)retire_accession_[status/manifest]_only()
    # for a single centralized set of methods to maintain:
    def retire_accession(self, accession_num, retire_batch_only=False, retire_batch_name=None):
        # helper method to update and retire ALL corresponding MANIFEST and STATUS records for an accession_num:
        successful = True # unless an exception, thrown perhaps
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.retire_accession(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}'.format(CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES), flush=True)
            print('VERBOSE {0}.retire_accession(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}, '\
                    'for accession_num=\'{2}\' w/ batch_only={3}, batch_name=\'{4}\'.'.format(
                        CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES,
                        accession_num, batch_only, batch_name), flush=True)
        #
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.retire_accession(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
        #
        # NOTE: this general version of retire_accession will retire all using the same subaccessions_only,
        # and methods such as the 2x resolve_multiuuids(), namely:
        #   * resolve_multiuuids_merge_radiology()
        #   * resolve_multiuuids_consolidate_locally()
        # each of which may require variations to this theme....
        # TODO: Once each of those ^^^ 2x methods have been completed, determine if this can support both,
        # e.g., by way of multile subaccessions_only parms, perhaps as:
        #    * manifest_subaccessions_only
        #    * status_subaccessions_only
        #    * stage_subaccessions_only
        # until then, here is the general:
        # TODO: preface each with a "success = " to save & check the return value for each of these:

        # BATCHES NOTE: including batch info for Retire, whether defaults or defined:
        self.retire_accession_manifest_only(accession_num, batch_only=retire_batch_only, batch_name=retire_batch_name)
        self.retire_accession_status_only(accession_num, except_change_seq=None, batch_only=retire_batch_only, batch_name=retire_batch_name)
        # BATCHES NOTE: no batch info needed for Stager:
        self.retire_accession_stager_only(accession_num, except_change_seq=None, delete_from_orthanc=True, orthanc_comment='via_retire_accession()')

        #
        # SQL blank line at end of all this method's SQL:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
            print('{0}-- # END of {1}.retire_accession(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        #
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.retire_accession(): Goodbye.'.format(CLASS_PRINTNAME), flush=True)
        return successful
        # end of retire_accession()


    ###############
    # NOTE: resolve_multiuuids_delete_locally() is initially using retire_accession()
    #   as a kinder, gentler first-pass approach, but could easily switch over to this once ready:
    ###############
    # FUTURE_delete_accession():
    # As recycled from modules_onprem_dicom.py:predelete_accession()'
    # NOTE: this currently only deletes the 2x tables within the Locutus DB:
    #   MANIFEST & STATUS
    # TODO: introduce a DELETE from the STager DB's StableStudies table as well.
    # UNTIL THEN, just note this method as a FUTURE:
    # TODO: break this down into each of the more atomic sub-methods, such as:
    #       FUTURE_delete_accession_manifest_only()
    #       FUTURE_delete_accession_status_only()
    #       FUTURE_delete_accession_stager_only()
    #   and switch resolve_multiuuids_delete_locally() to use this:
    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    def FUTURE_delete_accession(self, accession_num, manifest_status, subaccessions_only=True):
        # helper method to update and delete corresponding records for an accession_num:
        successful = True # unless an exception, thrown perhaps
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.delete_accession(): Hello! LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES={1}'.format(CLASS_PRINTNAME, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES), flush=True)
        #
        # SQL comment prior to the actual SQL command:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # -- -- -- -- -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.delete_accession(accession=\'{2}\'}):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
        #
        # delete_accession Part 1: Manifest table
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # {1}.delete_accession()part01:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME), flush=True)

        # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs,
        # BUT, if needed, we could explore a 'accession_num LIKE \'{0}.%\'' for any such sub-accessions
        # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)

        # FIRST, a commented SELECT version of the actual UPDATE command:
        # BATCHES NOTE: TODO: could UPDATE this, but leaving as is for now since FUTURE_delete_accession() not even active yet, yeah?
        preupdate_sql_select='SELECT accession_num as accession_num_for_DELETE FROM {0} '\
                                        'WHERE {1} '\
                                        'AND active ;'.format(
                                        self.manifest_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        # and NOW, the actual UPDATE, a DELETE:
        update_sql='DELETE FROM {0} '\
                                        'WHERE {1} '\
                                        'AND active ;'.format(
                                        self.manifest_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
            # nah, don't await SQL blank line until end of all this method's SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.delete_accession(): DELETING from DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            self.LocutusDBconnSession.execute(update_sql)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.delete_accession(): **NOT** DELETING from DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
        #

        #
        # delete_accession Part 2: Status table
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # {1}.delete_accession()part02:'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME), flush=True)
        # BATCHES NOTE: TODO: could UPDATE this, but leaving as is for now since FUTURE_delete_accession() not even active yet, yeah?
        preupdate_sql_select='SELECT accession_num as accession_num_for_DELETE FROM {0} '\
                                        'WHERE {1} '\
                                        'AND active ;'.format(
                                        self.status_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions)
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        for preupdate_sql_select_result in preupdate_sql_select_results.fetchall():
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
        ###################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        # and NOW, the actual UPDATE, a DELETE:
        update_sql='DELETE FROM {0} '\
                                        'WHERE {1} '\
                                        'AND active ;'.format(
                                        self.status_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print(self.locutus_settings.SQL_OUT_PREFIX, update_sql, flush=True)
            # SQL blank line, now that at end of all this method's SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
        #
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.delete_accession(): DELETING from DB since LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
            self.LocutusDBconnSession.execute(update_sql)
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.delete_accession(): **NOT** DELETING from DB since **NOT** LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES'.format(CLASS_PRINTNAME), flush=True)
        #

        #
        # delete_accession Part 3: Staging DB's StableStudies Table
        # =======> TODO: implement,
        # after the corresponding addition to retire_accession(), etc.
        #

        #
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # END of {1}.delete_accession(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            print('{0}-- # -- -- -- -- -- -- -- -- -- -- -- --'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('VERBOSE {0}.delete_accession(): Goodbye.'.format(CLASS_PRINTNAME), flush=True)
        return successful
        # end of delete_accession()
    # ^^^^^^^^^^
    # Whole lotta retiring
    #########################################


    ####################
    # for resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY:
    # AND resolve_cmd == RESOLVE_VIA_RESEND_RADIOLOGY:
    # NOTE: now available, and incorporated into each of the above retiring methods,
    # a default SAFE MODE / DRY RUN, only overridden when the following new setting is enabled:
    #   self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES
    #
    # thereby allowing initial checks of the SQL as emitted via each dry run,
    # to enable a semi-automated approach to the manual process, at least until comfortable letting it loose as fully automated! :-)
    ####################
    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    def resolve_multiuuids_merge_radiology(self, resolve_cmd, accession_num, manifest_status, resolve_select_info, resolve_update_info, resolve_batches_enable=None, resolve_batch_name=''):
        # PASSING IN resolve_cmd to all now, as per the following possibilities:
        #   resolve_cmd = RESOLVE_VIA_MERGE_RADIOLOGY, or...
        #   resolve_cmd = RESOLVE_VIA_RESEND_RADIOLOGY
        success = False
        new_manifest_status = manifest_status
        print('Summarizer::resolve_multiuuids_merge_radiology()... welcome!', flush=True)
        print('Summarizer::resolve_multiuuids_merge_radiology() for accession=\'{0}\', manifest_status={1}, resolve_batches_enable={2}, resolve_batch_name=\'{3}\''.format(
                accession_num, manifest_status, resolve_batches_enable, resolve_batch_name), flush=True)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- # START of {1}.resolve_multiuuids_merge_radiology(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            # SQL blank line before all this method's umbrella SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)

        use_new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_OTHER_AFTER_RESOLVE_GENERIC
        # default for resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY:
        if resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY:
            use_new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_MERGE
        elif resolve_cmd == RESOLVE_VIA_RESEND_RADIOLOGY:
            use_new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_RESEND
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: resolve_multiuuids_merge_radiology() found old manifest_status=\'{0}\', '\
                'and resolve_cmd ({1}) sets new base status==\'{2}\'.'.format(
                    new_manifest_status, resolve_cmd,
                    use_new_manifest_status), flush=True)
        new_manifest_status = use_new_manifest_status
        #############
        # NOTE: if its old manifest_status (manifest_status) started with ONDECK_OTHER_PREFIX,
        # be sure to re-preface the new manifest_status w/ it, and retain any Pre-loaded suffix:
        #############
        # 6/08/2026 AAA_LOCUTUS_BATCHES Resolver Patch, as part of Manifest-Once Batches:
        # regardless of previous ONDECK status, go ahead and update to use_new_manifest_status with dicom_summarize_stats_resolve_multiuuids_suffix,
        # if so defined:
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS_SUFFIX:
            new_manifest_status = new_manifest_status \
                                    + self.locutus_settings.PROCESSING_SUFFIX_DELIM \
                                    + self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS_SUFFIX
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: resolve_multiuuids_merge_radiology() found previous manifest_status=\'{0}\' for this accession_num+workspace+batch; '\
                'setting new Resolved status==\'{1}\' for this accession_num across all workspace batches.'.format(
                    manifest_status,
                    new_manifest_status), flush=True)

        # and set this new_manifest_status back for the use_new_manifest_status for subsequent sections such as reactivate():
        use_new_manifest_status = new_manifest_status

        ####################################
        # From the manual merge prep recipe:

        # 1a) Locutus MANIFEST table:
        #
        ######################
        # 6/08/2026 AAA_LOCUTUS_BATCHES Resolver Patch, as part of Manifest-Once Batches:
        # NOTE: no longer automatically retiring OR re-activating the MANIFEST records, due to the complexities of multi-batches & multiple retired records.
        # INSTEAD, running with the assumption that AAA_LOCUTUS_BATCHES will be actively maintained from here on,
        # and can be used to assist with some per-Workspace UPDATES of all this accession's various MANIFEST records.
        # ALSO, recommending that the Preloader be run again for any related batch FOLLOWING the Multi-UUID Resolver.
        ######################
        success = self.update_accession_manifest_status_across_all_workspaces(accession_num, use_new_manifest_status)
        # ^^^ FORMERLY, WAS:
        """
        # RETIRE (or DELETE?) *ONLY* the sub-accession(s) in the MANIFEST table:
        # BATCHES NOTE: leaving as defaults both batch_only (=False) & batch_name (=None) to Resolve all batches:
        success = self.retire_accession_manifest_only(accession_num)
        # NOTE: the above ^^^ w/ only the subaccessions of the MANIFEST table,
        #
        # and.... REACTIVATE its one remaining whole accession in the MANIFEST table:

        if success:
            # effectively a NO-OP for this case to explicitly exist, w/ new_manifest_status alreeady set above before reactivate_accession_manifest_only()
            new_manifest_status=use_new_manifest_status
            ##############################################################
            # BATCHES NOTE: using the active resolve_batches_enable, resolve_batch_name to Reactivate this batch only:
            # Q: or.... do we want the reactive_accession_manifest_only to reactivate ALL such batches?
            #######
            success = self.reactivate_accession_manifest_only(accession_num, new_manifest_status, resolve_select_info, resolve_update_info,
                                                                resolve_batches_enable, resolve_batch_name)
        print('r3m0 DEBUG: WARNING: currently skipping OUT-COMMENTED call to self.reactivate_accession_manifest_only(...,resolve_batches_enable={0}, resolve_batch_name=\'{1}\')'.format(
            resolve_batches_enable, resolve_batch_name), flush=True)
        """

        #
        # 2) Locutus STATUS table
        # NOTE: and the below vvv w/ *ALL* (incl the whole accession) of the STATUS table:
        # NOTE: with no except_change_seq's, go ahead and retire ALL of em'
        if success:
            # BATCHES NOTE: leaving as defaults both batch_only (=False) & batch_name (=None) to Resolve all batches:
            success = self.retire_accession_status_only(accession_num, except_change_seq=None)

        # 3) Stager StableStudies Table
        #
        # RETIRE *ALL* change_seq_ids for this accession, to await the new uuid:
        if success:
            # BATCHES NOTE: no batch info needed for Stager:
            self.retire_accession_stager_only(accession_num, except_change_seq=None, delete_from_orthanc=True, orthanc_comment='via_resolve_multiuuids_merge_radiology()')
        ####################
        # self.locutus_settings.SQL_OUT_PREFIX NOTE: & LATER, perhaps just leaving the clues here (whether under SQL_OUT:, or otherwise)
        # DONE: 4) ORTHANCINTPROD (as now resolved directly by retire_accession_stager_only())
        # 5) OnPrem (and/or isilon, whatever the deid'd target)
        # ===> BE SURE to RECORD/NOTE all of the applicable deidentified_gs_target/self.status_target_field for each uuid, such that they can later be removed from GS
        ####################

        if self.locutus_settings.LOCUTUS_VERBOSE:
            # end of all this method's umbrella SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
            print('{0}-- # END of {1}.resolve_multiuuids_merge_radiology(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            print('{0}-- # ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

        if success and not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            # leaving the summary result manifest_status for this accession as a RESOLVING:
            #new_manifest_status += ':{0}_{1}_FROM_{2}_to_{3}_via_{4}'.format(
            #    SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX, accession_num)
            # REMOVING subsequent TEXT, morphing from PREFIX into full ERROR:
            new_manifest_status = SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX + ":" + use_new_manifest_status
            #####
            # NVM TRY: moving the SAFEMODE/DRYRUN "prefix" to the end, to allow grouping w/ the normal
            # new_manifest_status = use_new_manifest_status + '_[' + SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX + ']'
            # NOTE: putting the SAFEMODE PREFIX at the end does merge it in with the others, but without any Preloader+suffx,
            # so, in the interest of standing out more completely, leaving the SAFEMODE PREFIX as a PREFIX. ;-)
            #####
        elif success and self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            # effectively a NO-OP for this case to explicitly exist, w/ new_manifest_status alreeady set above before reactivate_accession_manifest_only()
            new_manifest_status=use_new_manifest_status
        else:
            #new_manifest_status += ':{0}_{1}_FROM_{2}_to_{3}_via_{4}'.format(
            #    ERROR_RESOLVING_PREFIX, accession_num)
            # REMOVING subsequent TEXT, morphing from PREFIX into full ERROR:
            new_manifest_status = ERROR_RESOLVING_PREFIX + ":" + resolve_cmd

        print('Summarizer::resolve_multiuuids_merge_radiology(), returning: success={0}, '\
                    'accession_num=\'{1}\', new_manifest_status={2}'.format(
                        success, accession_num, new_manifest_status), flush=True)
        print('Summarizer::resolve_multiuuids_merge_radiology()... bye!', flush=True)
        return (success, accession_num, new_manifest_status)
        # end of resolve_multiuuids_merge_radiology()


    ####################
    # for resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
    # for resolve_cmd == RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX
    # NOTE: now available, and incorporated into each of the above retiring methods,
    # a default SAFE MODE / DRY RUN, only overridden when the following new setting is enabled:
    #   self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES
    #
    # thereby allowing initial checks of the SQL as emitted via each dry run,
    # to enable a semi-automated approach to the manual process, at least until comfortable letting it loose as fully automated! :-)
    ####################
    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    def resolve_multiuuids_consolidate_locally(self, resolve_cmd, accession_num, manifest_status, resolve_select_info, resolve_update_info, resolve_batches_enable=None, resolve_batch_name='', keeper_uuid=None):
        # PASSING IN resolve_cmd to all now,
        #   resolve_cmd = RESOLVE_VIA_CONSOLIDATE_LOCALLY
        #   resolve_cmd = RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX
        success = False
        new_manifest_status = manifest_status
        print('Summarizer::resolve_multiuuids_consolidate_locally()... welcome!', flush=True)
        print('Summarizer::resolve_multiuuids_consolidate_locally() resolve_cmd={0} '\
                'for accession=\'{1}\', with keeper_uuid=\'{2}\', and manifest_status={3}'.format(
                resolve_cmd,
                accession_num,
                keeper_uuid,
                manifest_status), flush=True)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- START of {1}.resolve_multiuuids_consolidate_locally(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            # SQL blank line before all this method's umbrella SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)

        ####################################
        # From the manual merge prep recipe:

        # Step 0: Initial Intel Gathering:
        # Start with the Locutus STATUS table, in order to determine:
        # a) the maximum change_seq_id (of its subaccessions)
        # b) the maximum phase_processed (of its subaccessions)
        # hoping that the same subaccession happens to have a max of each,
        # else we might need to error out and leave this to the user to manually resolve.

        # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs,
        # BUT, if needed, we could explore a 'accession_num LIKE \'{0}.%\'' for any such sub-accessions
        # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)

        # default where_uuid for when not keeper_uuid:
        accession_range_where_uuid = accession_range_where
        if keeper_uuid:
            # create another WHERE clause specifically for those tables (status & stager)
            # with uuid fields:
            accession_range_where_uuid = accession_range_where + ' AND uuid=\'{0}\''.format(keeper_uuid)

        ###############################
        # 0a) locutus=# select max(phase_processed) as max_phase_processed from onprem_dicom_status  where accession_num = ACC;
        # 0b) locutus=# select max(change_seq_id) as max_change from onprem_dicom_status  where accession_num = ACC;
        # FIRST, a commented SELECT version of the actual UPDATE command:
        # BATCHES NOTE: checking across all batches to multi-UUID resolve, yeah?
        preupdate_sql_select='SELECT max(change_seq_id) as max_change_seq_id, '\
                                    'max(phase_processed) as max_phase_processed  FROM {0} '\
                                        'WHERE {1} '\
                                        'AND active ;'.format(
                                        self.status_table,
                                        accession_range_where_uuid)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions);
        # Doing these for the Stager first, as a 1st pass test of its new StagerDBconnSession:
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        # just one result:
        preupdate_sql_select_result = preupdate_sql_select_results.fetchone()
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
            ###################
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

        max_change_seq_id = preupdate_sql_select_result['max_change_seq_id']
        max_phase_processed = preupdate_sql_select_result['max_phase_processed']

        if not max_change_seq_id and not max_phase_processed:
            print('ERROR: resolve_multiuuids_consolidate_locally() found NEITHER max change_seq_id '\
                    'NOR max phase_processed for accession=\'{0}\' with keeper_uuid=\'{1}\'.'.format(
                        accession_num, keeper_uuid), flush=True)
            # TODO = consider alternate approach to this bail early, at least indent subsequent code?
            success = False
            new_manifest_status = ERROR_RESOLVING_PREFIX + ":" + resolve_cmd
            return (success, accession_num, new_manifest_status)

        ###############################
        # 0c) locutus=# select phase_processed as max_change_phase_processed from onprem_dicom_status
        #               where accession_num = ACC
        #               and change_seq_id=(select max(change_seq_id) from onprem_dicom_status
        #                           where accession_num = ACC );
        # FIRST, a commented SELECT version of the actual UPDATE command:
        # BATCHES NOTE: checking across all batches to multi-UUID resolve, yeah?
        preupdate_sql_select='SELECT phase_processed as max_change_phase_processed, '\
                                    'accession_num as max_change_old_accession_num '\
                                    'FROM {0} '\
                                        'WHERE {1} AND change_seq_id={2} '\
                                        'AND active ;'.format(
                                        self.status_table,
                                        accession_range_where_uuid,
                                        max_change_seq_id)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.status_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions);
        # Doing these for the Stager first, as a 1st pass test of its new StagerDBconnSession:
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        # just one result:
        preupdate_sql_select_result = preupdate_sql_select_results.fetchone()
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
            ###################
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

        max_change_phase_processed = preupdate_sql_select_result['max_change_phase_processed']
        max_change_old_accession_num = preupdate_sql_select_result['max_change_old_accession_num']

        # CHECK, if:
        # if max_change_phase_processed < max_phase_processed
        #    SET AN ERROR, recommending manual resolution
        if (max_change_phase_processed < max_phase_processed):
            success = False
            # TODO: create a more real error from this, and tally it?
            print('ERROR: resolve_multiuuids_consolidate_locally() found that the max change_seq_id ({0}) '\
                    'has a phase_processed ({1}) which is less than that ({2}) '\
                    'of another change_seq_id for this accession (\'{3}\').'.format(
                        max_change_seq_id, max_change_phase_processed,
                        max_phase_processed, accession_num), flush=True)
        else:
            success = True
            # if DEBUG/VERBOSE:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('DEBUG: YAY!!!! resolve_multiuuids_consolidate_locally() found that the max change_seq_id ({0}) '\
                    'has a phase_processed ({1}) which matches that ({2}) '\
                    'of the max change_seq_id for this accession (\'{3}\').'.format(
                        max_change_seq_id, max_change_phase_processed,
                        max_phase_processed, accession_num), flush=True)
        # CHECK else (if max_change_phase_processed == max_phase_processed):
        #    carry on, using the max_change, its  max_change_acc_num and its max_change_uuid, as via:
        # 0d) locutus=# select accession_num max_change_acc_num, uuid as max_change_uuid from onprem_dicom_status  where accession_num >= 6486329 and accession_num < 6486330 and change_seq_id=(select max(change_seq_id) from onprem_dicom_status  where accession_num >= 6486329 and accession_num < 6486330);


        ################################
        # 1) Locutus STATUS table
        # NOTE: and the below vvv w/EXCEPTION (for the max_change_seq_id) of the STATUS table:
        # BATCHES NOTE: leaving as defaults both batch_only (=False) & batch_name (=None) to Resolve all batches:
        success = self.retire_accession_status_only(accession_num, except_change_seq=max_change_seq_id)

        ################################
        # 2) Locutus MANIFEST table
        # PREP: get the current manifest_status specific to the max_change_old_accession_num:

        ###############################
        # as recycled from:
        # 0c) locutus=# select phase_processed as max_change_phase_processed from onprem_dicom_status
        #               where accession_num = ACC
        #               and change_seq_id=(select max(change_seq_id) from onprem_dicom_status
        #                           where accession_num = ACC);
        # FIRST, a commented SELECT version of the actual UPDATE command:
        # BATCHES NOTE: checking across all batches to multi-UUID resolve, yeah?
        preupdate_sql_select='SELECT manifest_status as max_change_manifest_status '\
                                    'FROM {0} '\
                                        'WHERE accession_num=\'{1}\' '\
                                        'AND active ;'.format(
                                        self.manifest_table,
                                        max_change_old_accession_num)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions);
        # Doing these for the Stager first, as a 1st pass test of its new StagerDBconnSession:
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        # just one result:
        preupdate_sql_select_result = preupdate_sql_select_results.fetchone()
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
            ###################
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

        max_change_manifest_status = preupdate_sql_select_result['max_change_manifest_status']

        #WAS: 2a) RETIRE (or DELETE?) *ONLY* the sub-accession(s) in the MANIFEST table:

        if success:
            # BATCHES NOTE: leaving as defaults both batch_only (=False) & batch_name (=None) to Resolve all batches:
            new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_OTHER_AFTER_LOCAL_CONSOLIDATION
            # NOTE: be aware of cases where the ERROR_MULTIPLE_CHANGE_UUIDS have already been PROCESSED,
            # perhaps sure that the reactivate_accession_manifest_only.
            #####
            # Following the above 1) retire_accession_status_only()
            #   & NO LONGER: 2a) retire_accession_manifest_only()
            # the new phase_processed will === MIN_PROCESSING_PHASE at the below reactivate_accession_manifest_only(),
            # and there is no more need to compare against a previous max_phase_processed or manifest_status,
            # EXCEPT, perhaps, for the already existing else of an accession missing/PENDING_CHANGE:
            #   new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_AFTER_LOCAL_CONSOLIDATION + "_and_" + "READY2PROCESS"
            if max_phase_processed >= MIN_PROCESSING_PHASE:
                # an accession already existed for this, regardless of how processed or not
                # initial/default manifest_status of `RESOLVED_MULTIUUIDS`
                new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_OTHER_AFTER_RESOLVE_GENERIC
                if resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
                    new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_OTHER_AFTER_LOCAL_CONSOLIDATION
                elif resolve_cmd == RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX:
                    new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_AFTER_LOCAL_CHOICE

                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: resolve_multiuuids_consolidate_locally() found that max_phase_processed ({0}) >= MIN==2 '\
                        'so w/ resolve_cmd ({1}) sets new base status==\'{2}\'.'.format(
                            max_phase_processed, resolve_cmd,
                            new_manifest_status), flush=True)
            else:
                new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_AFTER_RESOLVE_GENERIC
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: resolve_multiuuids_consolidate_locally() found that max_phase_processed ({0}) < MIN==2 '\
                        'so w/ resolve_cmd ({1}) sets new base status==\'{2}\'.'.format(
                            max_phase_processed, resolve_cmd,
                            new_manifest_status), flush=True)

            #############
            # 6/08/2026 AAA_LOCUTUS_BATCHES Resolver Patch, as part of Manifest-Once Batches:
            # regardless of previous ONDECK status, go ahead and update to use_new_manifest_status with dicom_summarize_stats_resolve_multiuuids_suffix,
            # if so defined:
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS_SUFFIX:
                new_manifest_status = new_manifest_status \
                                        + self.locutus_settings.PROCESSING_SUFFIX_DELIM \
                                        + self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS_SUFFIX
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('DEBUG: resolve_multiuuids_merge_radiology() found previous manifest_status=\'{0}\' for this accession_num+workspace+batch; '\
                    'setting new Resolved status==\'{1}\' for this accession_num across all workspace batches.'.format(
                        manifest_status,
                        new_manifest_status), flush=True)

            ######################
            # 6/08/2026 AAA_LOCUTUS_BATCHES Resolver Patch, as part of Manifest-Once Batches:
            # NOTE: no longer automatically retiring OR re-activating the MANIFEST records, due to the complexities of multi-batches & multiple retired records.
            # INSTEAD, running with the assumption that AAA_LOCUTUS_BATCHES will be actively maintained from here on,
            # and can be used to assist with some per-Workspace UPDATES of all this accession's various MANIFEST records.
            # ALSO, recommending that the Preloader be run again for any related batch FOLLOWING the Multi-UUID Resolver.
            ######################
            success = self.update_accession_manifest_status_across_all_workspaces(accession_num, new_manifest_status)
            # ^^^ FORMERLY, WAS:
            #WAS: success = self.retire_accession_manifest_only(accession_num)

            ######################
            # NOTE: no longer automatically re-activating, due to the complexities of multi-batches & multiple retired records.
            # INSTEAD, recommending that the Preloader be run again FOLLOWING the Multi-UUID Resolver.
            ######################


        # 3) Stager StableStudies Table
        #
        # RETIRE *ONLY* change_seq_ids != max_change_seq_id for this accession,
        if success:
            # BATCHES NOTE: no batch info needed for Stager:
            self.retire_accession_stager_only(accession_num, except_change_seq=max_change_seq_id, delete_from_orthanc=True, orthanc_comment='via_resolve_multiuuids_consolidate_locally()')


        # Wrap up the new_manifest_status:
        if success and not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            #new_manifest_status += ':{0}_{1}_FROM_{2}_to_{3}_via_{4}'.format(
            #    SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX, accession_nun)
            # REMOVING subsequent TEXT, morphing from PREFIX into full ERROR:
            # new_manifest_status = SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX + ":" + resolve_cmd
            # NOTE: since new_manifest_status is already set above, merely preface it with the SAFEMODE:
            new_manifest_status = SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX + ':' + new_manifest_status
            #####
            # NVM TRY: moving the SAFEMODE/DRYRUN "prefix" to the end, to allow grouping w/ the normal
            # new_manifest_status = use_new_manifest_status + '_[' + SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX + ']'
            # NOTE: putting the SAFEMODE PREFIX at the end does merge it in with the others, but without any Preloader+suffx,
            # so, in the interest of standing out more completely, leaving the SAFEMODE PREFIX as a PREFIX. ;-)
            #####
        elif success and self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            # explicitly leave new_manifest_status as is, as a NO-OP merely to highlight this case:
            new_manifest_status = new_manifest_status
        else:
            #new_manifest_status += ':{0}_{1}_FROM_{2}_to_{3}_via_{4}'.format(
            #    ERROR_RESOLVING_PREFIX, accession_num)
            # REMOVING subsequent TEXT, morphing from PREFIX into full ERROR:
            new_manifest_status = ERROR_RESOLVING_PREFIX + ":" + resolve_cmd

        ####################
        # self.locutus_settings.SQL_OUT_PREFIX NOTE: & LATER, perhaps just leaving the clues here (whether under SQL_OUT:, or otherwise)
        # DONE: 4) ORTHANCINTPROD (as now resolved directly by retire_accession_stager_only())
        # 5) OnPrem
        # ===> BE SURE to RECORD/NOTE all of the applicable deidentified_gs_target/self.status_target_field for each uuid, such that they can later be removed from GS
        ####################

        if self.locutus_settings.LOCUTUS_VERBOSE:
            # end of all this method's umbrella SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
            print('{0}-- END of {1}.resolve_multiuuids_consolidate_locally(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            print('{0}-- # ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

        print('Summarizer::resolve_multiuuids_consolidate_locally(), returning: success={0}, '\
                'accession_num=\'{1}\', new_manifest_status={2}'.format(
                    success, accession_num, new_manifest_status), flush=True)
        print('Summarizer::resolve_multiuuids_consolidate_locally()... bye!', flush=True)
        return (success, accession_num, new_manifest_status)
        # end of resolve_multiuuids_consolidate_locally()


    ####################
    # resolve_multiuuids_delete_locally()....
    # for resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY
    # and resolve_cmd == RESOLVE_VIA_DELETE_STAGE_ONLY
    # NOTE: now available, and incorporated into each of the above retiring methods,
    # a default SAFE MODE / DRY RUN, only overridden when the following new setting is enabled:
    #   self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES
    #
    # thereby allowing initial checks of the SQL as emitted via each dry run,
    # to enable a semi-automated approach to the manual process, at least until comfortable letting it loose as fully automated! :-)
    ####################
    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    def resolve_multiuuids_delete_locally(self, resolve_cmd, accession_num, manifest_status, resolve_batches_enable=None, resolve_batch_name='', delete_from_stage_only=True):
        # PASSING IN resolve_cmd to all now,
        #   resolve_cmd = RESOLVE_VIA_DELETE_LOCALLY
        #   resolve_cmd = RESOLVE_VIA_DELETE_STAGE_ONLY (with )
        success = False
        new_manifest_status = manifest_status
        print('Summarizer::resolve_multiuuids_delete_locally()... welcome!', flush=True)
        print('Summarizer::resolve_multiuuids_delete_locally() resolve_cmd={0} '\
                'for accession=\'{1}\', manifest_status={2}'.format(
                resolve_cmd, accession_num, manifest_status), flush=True)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)
            print('{0}-- START of {1}.resolve_multiuuids_delete_locally(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            # SQL blank line before all this method's umbrella SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)

        # NOTE: this is for the core of resolve_multiuuids_delete_locally()
        # EITHER: call retire_accession() OR call FUTURE_delete_accession()
        #
        # TODO: Eventually do consider switching over to the eventual delete_accession().
        # For now, though, let's go with the slightly less spicy retire_accession().
        #
        # NOTE: we want to delete ALL of the accession parts (manifest, status, AND stager),
        #   including the whole AND subaccessions, so subaccessions_only=False:
        if delete_from_stage_only:
            # BATCHES NOTE: no batch info needed for Stager:
            success = self.retire_accession_stager_only(accession_num, except_change_seq=None,
                            delete_from_orthanc=True, orthanc_comment='via_resolve_multiuuids_delete_locally()')
        else:
            # the above retire_accession_stager_only() is but one of the three retirement procedures
            # with the more broadly reaching retire_accession():
            # BATCHES NOTE: leaving as defaults both batch_only (=False) & batch_name (=None) to Resolve all batches:
            #WAS: success = self.retire_accession(accession_num)
            ##########
            # 6/08/2026: UPDATE of the above call retire_acecession(), to NOT do the manifest,
            # at least not until the subsequent call to update_accession_manifest_status_across_all_workspaces()
            # once the new_manifest_status is appended with the Resolvers suffix, if so configured.
            ##########
            self.retire_accession_status_only(accession_num, except_change_seq=None, batch_only=False)
            # BATCHES NOTE: no batch info needed for Stager:
            self.retire_accession_stager_only(accession_num, except_change_seq=None, delete_from_orthanc=True, orthanc_comment='via_retire_accession()')

        use_new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_OTHER_AFTER_RESOLVE_GENERIC
        # default for resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY:
        if resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY:
            use_new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_AFTER_LOCAL_DELETE
        elif resolve_cmd == RESOLVE_VIA_DELETE_STAGE_ONLY:
            use_new_manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_AFTER_LOCAL_DELETE_STAGE_ONLY
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: resolve_multiuuids_delete_locally() found old manifest_status=\'{0}\', '\
                'and resolve_cmd ({1}) sets new base status==\'{2}\'.'.format(
                    new_manifest_status, resolve_cmd,
                    use_new_manifest_status), flush=True)
        new_manifest_status = use_new_manifest_status
        #############
        # 6/08/2026 AAA_LOCUTUS_BATCHES Resolver Patch, as part of Manifest-Once Batches:
        # regardless of previous ONDECK status, go ahead and update to use_new_manifest_status with dicom_summarize_stats_resolve_multiuuids_suffix,
        # if so defined:
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS_SUFFIX:
            new_manifest_status = new_manifest_status \
                                    + self.locutus_settings.PROCESSING_SUFFIX_DELIM \
                                    + self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS_SUFFIX
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: resolve_multiuuids_merge_radiology() found previous manifest_status=\'{0}\' for this accession_num+workspace+batch; '\
                'setting new Resolved status==\'{1}\' for this accession_num across all workspace batches.'.format(
                    manifest_status,
                    new_manifest_status), flush=True)
        #############
        # and set this new_manifest_status back for the use_new_manifest_status for subsequent sections such as reactivate():
        use_new_manifest_status = new_manifest_status

        ######################
        # 6/08/2026 AAA_LOCUTUS_BATCHES Resolver Patch, as part of Manifest-Once Batches:
        # NOTE: no longer automatically retiring OR re-activating the MANIFEST records, due to the complexities of multi-batches & multiple retired records.
        # INSTEAD, running with the assumption that AAA_LOCUTUS_BATCHES will be actively maintained from here on,
        # and can be used to assist with some per-Workspace UPDATES of all this accession's various MANIFEST records.
        # ALSO, recommending that the Preloader be run again for any related batch FOLLOWING the Multi-UUID Resolver.
        ######################
        success = self.update_accession_manifest_status_across_all_workspaces(accession_num, new_manifest_status)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            # end of all this method's umbrella SQL:
            print(self.locutus_settings.SQL_OUT_PREFIX, flush=True)
            print('{0}-- END of {1}.resolve_multiuuids_delete_locally(accession=\'{2}\'):'.format(self.locutus_settings.SQL_OUT_PREFIX, CLASS_PRINTNAME, accession_num), flush=True)
            print('{0}-- # ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

        if success and not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            new_manifest_status = SAFEMODE_RESOLVING_MULTIUUID_STATUS_PREFIX + ":" + use_new_manifest_status
        elif success and self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
            # as set above before reactivate_accession_manifest_only()
            new_manifest_status = use_new_manifest_status
        else:
            #new_manifest_status += ':{0}_{1}_FROM_{2}_to_{3}_via_{4}'.format(
            #    ERROR_RESOLVING_PREFIX, accession_num, resolve_cmd)
            # REMOVING subsequent TEXT, morphing from PREFIX into full ERROR:
            new_manifest_status = ERROR_RESOLVING_PREFIX + ":" + resolve_cmd

        print('Summarizer::resolve_multiuuids_delete_locally(), returning: success={0}, '\
                'accession_num=\'{1}\', new_manifest_status={2}'.format(
                    success, accession_num, new_manifest_status), flush=True)
        print('Summarizer::resolve_multiuuids_delete_locally()... bye!', flush=True)
        return (success, accession_num, new_manifest_status)
        # end of resolve_multiuuids_delete_locally()


    ####################
    # resolve_multiuuids() == to read the RESOLVE_VIA_CMD from the manifest row, and pass it on to specific resolve_multiuuids_*()
    # and accumulate each accession into to an accs_resolved list, perhaps returning the whole number from each call, via:
    #         return (success, accession_num, manifest_status, recently_resolved, formerly_resolved)
    ####################
    # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs:
    # NOTE: resolve_select_info & resolve_update_info both use double-quotes since they can contain single quotes within:
    def resolve_multiuuids(self, accession_num, resolve_cmd, manifest_status, resolve_select_info, resolve_update_info, resolve_batches_enable=None, resolve_batch_name=''):
        print('##################################################################')
        print('Summarizer::resolve_multiuuids()... welcome!', flush=True)
        print('Summarizer::resolve_multiuuids() called with accession_num=\'{0}\', '\
                'resolve_cmd=\'{1}\', manifest_status=\'{2}\', resolve_select_info=\"{3}\", '\
                'resolve_update_info=\"{4}\", resolve_batches_enable={5}, '\
                'resolve_batch_name=\'{6}\''.format(accession_num,
                resolve_cmd, manifest_status, resolve_select_info,
                resolve_update_info, resolve_batches_enable, resolve_batch_name), flush=True)
        #previously_resolved = False
        # above previously_resolved is now ambiguous, split into...
        # a) those recently resolved during this very session:
        recently_resolved = False
        # b) those formerly resolved in previous sessions, and already with a PENDING_CHANGE_RADIOLOGY_MERG
        formerly_resolved = False
        success = False

        ####################
        # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
        # NOTE: Q: NO need to even cast these into floats anymore, riiiiight? right.
        ####################
        ####################
        # Q: will we still want to include any sub/whole statistics?
        # especially w/ the few remaining SPLIT accessions no longer as status quo
        # NOTE: for now, see how the above fix rolls on through to the stats.
        ####################

        # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs,
        # BUT, if needed, we could explore a 'accession_num LIKE \'{0}.%\'' for any such sub-accessions
        # NOTE: to see all SPLITS, consider: accession_range_where = 'accession_num_src=\'{0}\''.format(accession_num)
        accession_range_where = 'accession_num=\'{0}\''.format(accession_num)
        ###############################
        # BATCHES NOTE: checking across all batches to multi-UUID resolve, yeah?

        # DEV NOTE: seems to be fine to omit batch_name & query total "parts"
        # (in fact, relatively irrelevant since num_accession_parts is only utilized for a print(), hmmm)
        preupdate_sql_select='SELECT COUNT(*) as num_accession_parts '\
                                    'FROM {0} '\
                                        'WHERE {1} '\
                                        'AND active ;'.format(
                                        self.manifest_table,
                                        accession_range_where)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # DB=Locutus, Table={1}'.format(self.locutus_settings.SQL_OUT_PREFIX, self.manifest_table), flush=True)
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select), flush=True)
        ###################
        # DO IT! Go ahead and actually execute that preupdate_sql_select
        # in order to preserve those values in the output log (as well as in the retired accessions);
        # Doing these for the Stager first, as a 1st pass test of its new StagerDBconnSession:
        preupdate_sql_select_results =  self.LocutusDBconnSession.execute(preupdate_sql_select)
        # just one result:
        preupdate_sql_select_result = preupdate_sql_select_results.fetchone()
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}-- # {1}'.format(self.locutus_settings.SQL_OUT_PREFIX, preupdate_sql_select_result), flush=True)
            ###################
            print('{0}-- #'.format(self.locutus_settings.SQL_OUT_PREFIX), flush=True)

        num_accession_parts = preupdate_sql_select_result['num_accession_parts']


        # TODO: add a safety check HERE to count the total number of whole&sub-accessions within this accession range
        # ESPECIALLY given that the resolved_status for CONSOLIDATE_LOCALLY could very well just be PROCESSED,
        # (if its max_change_seq_id sub-accession has already been PROCESSED), and it won't have any other identifying features.
        # As such, if the count is already down to 1 and the command is CONSOLIDATE_LOCALLY, then we can note it as already resolved.

        if accession_num in self.resolved_multiuuid_whole_accessions:
            print('Summarizer::resolve_multiuuids()... ALREADY JUST RECENTLY RESOLVED accession \'{0}\' in this very session'.format(accession_num), flush=True)
            # previously_resolved = True
            # above previously_resolved is now ambiguous, split into...
            # a) those recently resolved during this very session:
            recently_resolved = True
        elif manifest_status == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_MERGE:
            # support both THIS RESOLVE_VIA_MERGE_RADIOLOGY *and* the next, RESOLVE_VIA_RESEND_RADIOLOGY:
            print('Summarizer::resolve_multiuuids()... FORMERLY RESOLVED accession \'{0}\' in a previous such session (via {1}), to manifest_status={2}'.format(
                accession_num, RESOLVE_VIA_MERGE_RADIOLOGY, manifest_status), flush=True)
            # previously_resolved = True
            # above previously_resolved is now ambiguous, split into...
            # b) those explicitly resolved in previous sessions and already with a PENDING_CHANGE_RADIOLOGY_MERGE
            formerly_resolved = True
        elif manifest_status == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_RESEND:
            # support both the above RESOLVE_VIA_MERGE_RADIOLOGY *and* THIS RESOLVE_VIA_RESEND_RADIOLOGY:
            print('Summarizer::resolve_multiuuids()... FORMERLY RESOLVED accession \'{0}\' in a previous such session (via {1}), to manifest_status={2}'.format(
                accession_num, RESOLVE_VIA_RESEND_RADIOLOGY, manifest_status), flush=True)
            # previously_resolved = True
            # above previously_resolved is now ambiguous, split into...
            # b) those explicitly resolved in previous sessions and already with a PENDING_CHANGE_RADIOLOGY_RESEND
            formerly_resolved = True
        ########
        # NOTE: moving this elif down into the elif resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY or RESOLVE_VIA_CONSOLIDATE_LOCALLY:
        #elif num_accession_parts == 1 \
        #    and resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
        #    print('Summarizer::resolve_multiuuids()... *SEEMINGLY* FORMERLY RESOLVED accession {0} to {1} in a previous such session (via {2}), already down to 1 record, with manifest_status={3}'.format(
        #        accession_num, RESOLVE_VIA_CONSOLIDATE_LOCALLY, manifest_status), flush=True)
        #    # previously_resolved = True
        #    # above previously_resolved is now ambiguous, split into...
        #    # c) those implicitly resolved in previous sessions, regardless of the manifest_status:
        #    formerly_resolved = True
        #    # NOTE: not necessarily true. this COULD be the case whereby a single MANIFEST record
        #    # was not yet "split" into sub-accessions (especially since no longer doing so!),
        #    # yet still with multiple STATUS records
        ########
        elif manifest_status == self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND:
            print('Summarizer::resolve_multiuuids()... UNABLE TO FIND accession \'{0}\' to resolve as manifest_status=\'{1}\''.format(
                accession_num, manifest_status), flush=True)
            # and explicitly leaving success as False:
            success = False
        elif (resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY) \
            or (resolve_cmd == RESOLVE_VIA_RESEND_RADIOLOGY) \
            or (resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY) \
            or (resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY) \
            or (resolve_cmd == RESOLVE_VIA_DELETE_STAGE_ONLY) \
            or (len(resolve_cmd) > len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX) \
                and (resolve_cmd[:len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX)] == RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX)):

            print('Summarizer::resolve_multiuuids()... RESOLVING accession \'{0}\' via: {1}'.format(
                accession_num, resolve_cmd), flush=True)

            resolve_cmd_success = False
            # and for a subsequent print, use the supplied accession_num as resolve_accession_whole:
            resolve_accession_whole = accession_num
            # and
            resolve_manifest_status = f'UNKNOWN_RESOLVE_SUBCMD:{resolve_cmd}'

            # NOTE: num_accession_parts == 1 PRESUMES a single batch being evaluated;
            # TODO: limit above SELECT COUNT(*) to include the batch_name
            # NOTE: even if NOT YET resolved, a single batch SHOULD have but 1 part,
            # and it does NOT imply a previous resolution:
            # Q: =====> why specific to the CONSOLIDATE_LOCALLY????
            if num_accession_parts == 1 \
            and resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
                # r3m0: TODO: Q: confirm that this print is even relevant enough to retain?
                print('Summarizer::resolve_multiuuids()... *SEEMINGLY* FORMERLY RESOLVED '\
                    '(*OR* MERELY NEWLY PRE-LOADED) accession \'{0}\' in a previous such session '\
                    '(via {1}), currently w/ only 1 MANIFEST record, with manifest_status={2}, '\
                    'so... carrying on into STATUS records in case more.'.format(
                    accession_num, RESOLVE_VIA_CONSOLIDATE_LOCALLY, manifest_status), flush=True)
            # NOTE: first pass, just let it drop on down and see what happens:

            if ((resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY) or (resolve_cmd == RESOLVE_VIA_RESEND_RADIOLOGY)):
                (resolve_cmd_success, resolve_accession_whole, resolve_manifest_status) \
                    = self.resolve_multiuuids_merge_radiology(resolve_cmd, accession_num, \
                                            manifest_status, resolve_select_info, resolve_update_info, \
                                            resolve_batches_enable, resolve_batch_name)

            elif resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY:
                # NOTE: RESOLVE_VIA_DELETE_LOCALLY and RESOLVE_VIA_DELETE_STAGE_ONLY both sharing resolve_multiuuids_delete_locally():
                # with RESOLVE_VIA_DELETE_LOCALLY's explicitly setting delete_from_stage_only=False:
                (resolve_cmd_success, resolve_accession_whole, resolve_manifest_status) \
                    = self.resolve_multiuuids_delete_locally(resolve_cmd, accession_num, \
                                            manifest_status, \
                                            resolve_batches_enable, resolve_batch_name, \
                                            delete_from_stage_only=False)

            elif resolve_cmd == RESOLVE_VIA_DELETE_STAGE_ONLY:
                # NOTE: RESOLVE_VIA_DELETE_STAGE_ONLY and RESOLVE_VIA_DELETE_LOCALLY both sharing resolve_multiuuids_delete_locally():
                # with RESOLVE_VIA_DELETE_STAGE_ONLY's explicitly setting delete_from_stage_only=True:
                (resolve_cmd_success, resolve_accession_whole, resolve_manifest_status) \
                    = self.resolve_multiuuids_delete_locally(resolve_cmd, accession_num, \
                                            manifest_status, \
                                            resolve_batches_enable, resolve_batch_name, \
                                            delete_from_stage_only=True)

            elif resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
                # NOTE: RESOLVE_VIA_CONSOLIDATE_LOCALLY and RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX both sharing resolve_multiuuids_consolidate_locally():
                # with RESOLVE_VIA_CONSOLIDATE_LOCALLY's explicitly setting keeper_uuid=None:
                (resolve_cmd_success, resolve_accession_whole, resolve_manifest_status) \
                    = self.resolve_multiuuids_consolidate_locally(resolve_cmd, accession_num, \
                                            manifest_status, resolve_select_info, resolve_update_info, \
                                            resolve_batches_enable, resolve_batch_name, \
                                            keeper_uuid=None)
            else:
                # resolve_cmd like RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX%:
                # since other were filtered out prior to even calling resolve_multiuuids().
                chosen_uuid = resolve_cmd[len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX):]
                # NOTE: RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX and RESOLVE_VIA_CONSOLIDATE_LOCALLY both sharing resolve_multiuuids_consolidate_locally():
                # with RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX's uuid explicitly set as keeper_uuid=chosen_uuid:
                (resolve_cmd_success, resolve_accession_whole, resolve_manifest_status) \
                    = self.resolve_multiuuids_consolidate_locally(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX, \
                                            accession_num, \
                                            manifest_status, resolve_select_info, resolve_update_info, \
                                            resolve_batches_enable, resolve_batch_name, \
                                            keeper_uuid=chosen_uuid)

            success = resolve_cmd_success
            if success:
                print('Summarizer::resolve_multiuuids() received a success={0} from the resolve_cmd={1} for accession \'{2}\' with a resulting resolve_accession_whole={3} and manifest_status={4}'.format(
                        resolve_cmd_success, resolve_cmd, accession_num, resolve_accession_whole, resolve_manifest_status), flush=True)
                manifest_status = resolve_manifest_status
                # Q: only add this as a resolved whole accession if resolve_cmd_success, yeah?
                self.resolved_multiuuid_whole_accessions.append(accession_num)
            else:
                print('ERROR: Summarizer::resolve_multiuuids() received a non-success={0} from the resolve_cmd={1} for accession \'{2}\' with a resulting resolve_accession_whole={3} and manifest_status={4}'.format(
                        resolve_cmd_success, resolve_cmd, accession_num, resolve_accession_whole, resolve_manifest_status), flush=True)
                # Q: throw any exceptions here? or in the resolve_cmd handlers?
                # NOTE: the resolve_cmd handlers are now setting an ERROR message into the returned resolve_manifest_status, so update it anyhow:
                manifest_status = resolve_manifest_status

        else:
            print('ERROR: Summarizer::resolve_multiuuids(): unable to resolve accession \'{0}\' through unknown RESOLVE_VIA={1};'\
                    ' expects RESOLVE_VIA of <{2}, {3}, {4}, {5}, {6}, or {7}>'.format(
                                accession_num,
                                resolve_cmd,
                                RESOLVE_VIA_MERGE_RADIOLOGY,
                                RESOLVE_VIA_RESEND_RADIOLOGY,
                                RESOLVE_VIA_CONSOLIDATE_LOCALLY,
                                RESOLVE_VIA_DELETE_LOCALLY,
                                RESOLVE_VIA_DELETE_STAGE_ONLY,
                                RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX), flush=True)
            # Q: throw an exception?  Or just let it quietly proceed?

        print('Summarizer::resolve_multiuuids()... goodbye!', flush=True)
        print('##################################################################')
        # return the updated manifest_status:
        return (success, accession_num, manifest_status, recently_resolved, formerly_resolved)


    def Process(self):
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process() says Howdy'.format(CLASS_PRINTNAME), flush=True)

        total_errors_encountered = 0


        # [from module_onprem_dicom.py's] Processing Phase 3:
        ###########################################################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process(): Getting ready to summarize status for accessions in  '\
                    'the appropriate module for changes from the MRI Manifest lines:'.format(
                    CLASS_PRINTNAME),
                    flush=True)

        nowtime = datetime.datetime.utcnow()
        print('@ {0} UTC: START of PHASE batch'.format(nowtime), flush=True)
        print('====================================================', flush=True)

        #######################################
        # NOTE: add tally of workspace COUNT(DISTINCT(accession_num)) to better help confirm the correct Module & Workspace:
        total_distinct_accessions_in_workspace = 0
        # BATCHES NOTE: checking across all batches:
        manifest_status_results = self.LocutusDBconnSession.execute('SELECT COUNT(DISTINCT(accession_num)) as num '\
                                'FROM {0} '\
                                'WHERE active ;'.format(
                                self.manifest_table))
        manifest_status_row = manifest_status_results.fetchone()
        total_distinct_accessions_in_workspace = manifest_status_row['num']
        #######################################


        #######################################
        # NOTE: add tally of BATCH in workspace COUNT(DISTINCT(accession_num)) to better help confirm the correct Module & Workspace:
        total_distinct_accessions_in_batch = 0
        # NOTE: either way, setup the batch_clause, for later use in the Dynamically Grouped SQL:
        batch_clause = ''
        if not self.locutus_settings.LOCUTUS_BATCH_NAME or not len(self.locutus_settings.LOCUTUS_BATCH_NAME):
            batch_clause = 'AND batch_name IS NULL '
        else:
            batch_clause = 'AND batch_name=\'{0}\' '.format(self.locutus_settings.LOCUTUS_BATCH_NAME)

        # WAS: if self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
        # r3m0: DEBUG: TEST FORCE IT ANYHOW, since it seems that the BATCH_NAME is still used for the Summarizer even if NOT BYPASSED.
        # So... Q: **if** NOT using the Preloader and NOT BYPASS_MANIFEST,
        #           should BATCH_NAME be RESET back to the DEFAULT?
        # OR..... at least WARN of it and potential confusion? OR..... what?????
        total_distinct_accessions_in_batch = self.locutus_settings.batch_cursor_MAX_counter_unfiltered
        #######################################

        # Initial header for the line-by-line comparison with the input manifest:
        # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
        # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
        #

        module_specific_manifest_columns = ''
        # But first, create an optional column set as per the module being summarized,
        # and be sure to keep a trailing "," before the subsequent MANIFEST_HEADER_AS_OF_DATETIME & nowtime:
        module_specific_xtra_columns = ','

        # BATCHES NOTE: even w/ LOCUTUS_BATCHES_ENABLE/LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB, input_manifest_type has been set for this output by Setup():
        if self.locutus_settings.input_manifest_type == src_modules.settings.MANIFEST_TYPE_DEFAULT_SIMPLIFIED:
            module_specific_manifest_columns = '{0},{1}'.format(
                                                    src_modules.settings.MANIFEST_HEADER_ACCESSION_NUM,
                                                    src_modules.settings.SIMPLIFIED_MANIFEST_HEADER_MANIFEST_VER)
        elif self.locutus_settings.input_manifest_type == src_modules.settings.ONPREM_MANIFEST_TYPE:
            # ONPREM-specific versioned manifest:
            #
            module_specific_manifest_columns = '{0},{1},{2},{3},{4},{5},{6}'.format(
                                                    src_modules.settings.ONPREM_MANIFEST_HEADER_SUBJECT_ID,
                                                    src_modules.settings.ONPREM_MANIFEST_HEADER_OBJECT_INFO_01,
                                                    src_modules.settings.ONPREM_MANIFEST_HEADER_OBJECT_INFO_02,
                                                    src_modules.settings.ONPREM_MANIFEST_HEADER_OBJECT_INFO_03,
                                                    src_modules.settings.ONPREM_MANIFEST_HEADER_ACCESSION_NUM,
                                                    src_modules.settings.ONPREM_MANIFEST_HEADER_DEID_QC_STATUS,
                                                    src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER)
            # be sure to keep BOTH a leading & trailing "," around these optional column headers:
            module_specific_xtra_columns = ',{0},{1},{2},{3},{4},{5},{6},{7},'.format(
                                                    MANIFEST_OUTPUT_ONPREM_EXTRAS,
                                                    MANIFEST_HEADER_OUTPUT_ONPREM_FILENAME_FOR_LOCAL,
                                                    MANIFEST_HEADER_OUTPUT_ONPREM_FW_GROUP,
                                                    MANIFEST_HEADER_OUTPUT_ONPREM_FW_PROJECT,
                                                    MANIFEST_HEADER_OUTPUT_ONPREM_FW_IMPORT_SUBJECT_ARG,
                                                    MANIFEST_HEADER_OUTPUT_ONPREM_FW_IMPORT_SESSION_ARG,
                                                    MANIFEST_HEADER_OUTPUT_ONPREM_DEID_QC_STATUS,
                                                    MANIFEST_HEADER_OUTPUT_ONPREM_DEID_QC_STUDY_EXPLORER_URL)
        ################
        print('{0},{1},{2},{3},{4},{5}{6}{7} (UTC)'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    module_specific_manifest_columns,
                    MANIFEST_HEADER_ACCESSION_DUPLICATES,
                    MANIFEST_HEADER_STATUS,
                    MANIFEST_HEADER_DEID_DATETIME,
                    MANIFEST_HEADER_DEID_TARGET,
                    module_specific_xtra_columns,
                    '{0}={1}'.format(MANIFEST_HEADER_AS_OF_DATETIME,nowtime)),
                    flush=True)
        ################
        # and add as first comment lines the job description
        # (with an extra comma to space the longer description):
        print('{0},# JOB,DESCRIPTION:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.JOB_DESCRIPTION),
                    flush=True)
        # and the input manifest name
        # (with an extra comma to space the longer filename):
        print('{0},# INPUT,MANIFEST:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.INPUT_MANIFEST_NAME),
                    flush=True)
        # and again, the processing time:
        print('{0},# AS OF,DATETIME:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    nowtime),
                    flush=True)
        # current Locutus APP_NAME:
        print('{0},# APP_NAME:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    src_modules.settings.APP_NAME),
                    flush=True)
        # current Locutus APP_VERSION:
        print('{0},# APP_VERSION:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    src_modules.settings.APP_VERSION),
                    flush=True)
        # active Locutus module to Summarize:
        print('{0},# MODULE:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper()),
                    flush=True)

        # active Workspace to Summarize:
        print('{0},# WORKSPACES_ENABLED:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_WORKSPACES_ENABLE),
                    flush=True)
        # active Workspace to Summarize:
        print('{0},# WORKSPACE_NAME:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_WORKSPACE_NAME),
                    flush=True)
        # active Workspace MANIFEST table to Summarize:
        print('{0},# WORKSPACE_MANIFEST_TABLE:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.manifest_table),
                    flush=True)
        # active Workspace STATUS table to Summarize:
        print('{0},# WORKSPACE_STATUS_TABLE:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.status_table),
                    flush=True)

        #######################################
        # NOTE: add tally of workspace COUNT(DISTINCT(accession_num)),
        # total counts in the current Workspace, to help debug unexpected results from the Summarizer:
        print('{0},# TOTAL_WORKSPACE_ACCESSIONS:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    total_distinct_accessions_in_workspace),
                    flush=True)
        #######################################

        #######################################
        # MANIFEST-ONCE:
        # bypass Manifest CSV to instead Batch from DB into Summarize:
        #WAS: print('{0},# BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:,{1}'.format(
        print('{0},# LOAD_BATCH_FROM_DB_BYPASS_MANIFEST_CSV:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB),
                    flush=True)

        # WAS: if self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
        # r3m0: DEBUG: TEST FORCE IT ANYHOW, since it seems that the BATCH_NAME is still used for the Summarizer even if NOT BYPASSED.
        # So... Q: **if** NOT using the Preloader and NOT BYPASS_MANIFEST,
        #           should BATCH_NAME be RESET back to the DEFAULT?
        # OR..... at least WARN of it and potential confusion? OR..... what?????
        if True:
            # current Batch name to Summarize:
            print('{0},# BATCH_NAME:,{1}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        self.locutus_settings.LOCUTUS_BATCH_NAME),
                        flush=True)

        # NOTE: even though now printing the BATCH_NAME for each of these,
        # only show the TOTAL_BATCH (which might be as -1) and various BATCH FILTERS if LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
        if self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
            # total counts in the BATCH of the current Workspace, to help debug unexpected results from the Summarizer:
            # NOTE: total_distinct_accessions_in_batch = self.locutus_settings.batch_cursor_MAX_counter_unfiltered
            print('{0},# {1}:,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        MANIFEST_HEADER_ACCESSIONS_TOTAL_BATCH,
                        total_distinct_accessions_in_batch),
                        flush=True)

            # NEXT UP, the BATCH FILTER stuff, but only if set, either through the batch_cusror_clause_filter OR the filter_counter_from/to:
            if self.locutus_settings.batch_cursor_clause_filter_only \
            or self.locutus_settings.batch_filter_counter_from \
            or self.locutus_settings.batch_filter_counter_to:
                print('{0},# {1}:,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        MANIFEST_HEADER_BATCH_FILTER,
                        self.locutus_settings.batch_cursor_clause_filter_only),
                        flush=True)
                print('{0},# {1}:,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        MANIFEST_HEADER_ACCESSIONS_FILTERED_BATCH,
                        self.locutus_settings.batch_cursor_MAX_counter),
                        flush=True)
                print('{0},# {1}:,{2}:{3}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        MANIFEST_HEADER_BATCH_COUNTER_RANGE,
                        self.locutus_settings.batch_filter_counter_from,
                        self.locutus_settings.batch_filter_counter_to),
                        flush=True)
        #######################################

        #######################################
        # show accession mode:
        print('{0},# SHOW_ACCESSIONS:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS),
                    flush=True)
        ##########
        # NOTE: self.locutus_settings.LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS deprecated
        ##########
        # remove_text mode:
        #print('{0},# REMOVE_TEXT_FROM_INPUT_ACCESSIONS:,{1}'.format(
        #            MANIFEST_OUTPUT_PREFIX,
        #            self.locutus_settings.LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS),
        #            flush=True)
        ##########
        # show multiuuids mode:
        print('{0},# SHOW_MULTIUUIDS:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS),
                    flush=True)
        # redact accession mode:
        print('{0},# REDACT_ACCESSIONS:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS),
                    flush=True)
        # enable_db_updates mode:
        print('{0},# ENABLE_DB_UPDATES:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES),
                    flush=True)
        # preload_reprocessing_status mode:
        print('{0},# PRELOAD_NEW_ACCESSIONS:,{1}'.format(
                MANIFEST_OUTPUT_PREFIX,
                self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST),
                flush=True)
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST:
            print('{0},# PRELOAD_SUFFIX:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST_PREPROCESSING_SUFFIX),
                    flush=True)
        #####
        # LIKELY DEPRECATING PRESET (as superceded by the Preloader); no need to confuse the Summarizer/Preload output with its setting:
        ## preset_reprocessing_status mode:
        #print('{0},# PRESET_REPROCESSING_STATUS:,{1},SUFFIX:,{2}'.format(
        #            MANIFEST_OUTPUT_PREFIX,
        #            self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS,
        #            self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS_SUFFIX),
        #            flush=True)
        #####

        # resolve multi-uuids command mode:
        print('{0},# RESOLVE_MULTIUUIDS:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS),
                    flush=True)

        ################

        processed_uuids_in_this_phase = False
        manifest_done = False
        rownum = 1
        subtotal_comparisons_found_staged = 0
        subtotal_comparisons_not_staged = 0
        total_comparisons_checked = 0
        accession_list = []
        processed_ints_accession_list = []
        output_accession_list = []
        other_errors_found = []
        other_statuses_found = []
        multiuuid_accessions_shown = []

        # reset counters for specifically known status values:
        total_duplicates = 0
        total_not_found = 0
        total_not_found_duplicates = 0
        total_status_pending = 0
        total_status_pending_radiology_merge = 0
        total_status_pending_radiology_resend = 0
        total_status_pending_duplicates = 0
        total_status_processsed = 0
        total_status_processsed_duplicates = 0
        total_status_processsed_subaccessions = 0
        total_status_processsed_whole_accessions = 0
        total_status_previous_processsing = 0
        total_status_previous_processsing_duplicates = 0
        total_status_processing = 0
        total_status_processing_momentarily = 0
        total_status_multiple_splits = 0
        total_status_multiuuids = 0
        total_status_multiuuids_duplicates = 0
        total_status_error_other = 0
        total_status_error_other_duplicates = 0
        total_status_other = 0
        total_accessions_with_text_NOT_removed = 0

        # for preloaded accessions
        total_preloaded_accessions = 0
        total_preloaded_accessions_pending = 0
        total_preloaded_accessions_processing = 0
        total_preloaded_accessions_reprocessing = 0
        total_preloaded_accessions_multiples = 0
        total_preloaded_accessions_other = 0
        total_preloaded_accessions_duplicates = 0
        other_ondecks_found = []

        # for preset accessions
        total_preset_accessions = 0

        # TODO: definitely explore a better (ahem, actual) data structure for these.
        # and for multi-uuid resolutions, sets for each resolve_subcmd
        total_resolved_successfully_viaRADIOLOGY_MERGE = 0
        total_resolved_successfully_viaRADIOLOGY_RESEND = 0
        total_resolved_successfully_viaCONSOLIDATE = 0
        total_resolved_successfully_viaCHOICE = 0
        total_resolved_successfully_viaDELETE_LOCAL = 0
        total_resolved_successfully_viaDELETE_STAGE = 0
        #
        total_resolved_recently_viaRADIOLOGY_MERGE = 0
        total_resolved_recently_viaRADIOLOGY_RESEND = 0
        total_resolved_recently_viaCONSOLIDATE = 0
        total_resolved_recently_viaCHOICE = 0
        total_resolved_recently_viaDELETE_LOCAL = 0
        total_resolved_recently_viaDELETE_STAGE = 0
        #
        total_resolved_formerly_viaRADIOLOGY_MERGE = 0
        total_resolved_formerly_viaRADIOLOGY_RESEND = 0
        total_resolved_formerly_viaCONSOLIDATE = 0
        total_resolved_formerly_viaCHOICE = 0
        total_resolved_formerly_viaDELETE_LOCAL = 0
        total_resolved_formerly_viaDELETE_STAGE = 0
        #
        errors_resolving_viaRADIOLOGY_MERGE = 0
        errors_resolving_viaRADIOLOGY_RESEND = 0
        errors_resolving_viaCONSOLIDATE = 0
        errors_resolving_viaCHOICE = 0
        errors_resolving_viaDELETE_LOCAL = 0
        errors_resolving_viaDELETE_STAGE = 0
        #
        errors_resolving_viaOTHER = 0

        # END o' the special HEADER rows, add a delimting row of comment '#'s prior to summarizing each accession
        print('{0},###############'.format(
                    MANIFEST_OUTPUT_PREFIX), flush=True)

        try:
            # partA == initial manifest reading prior to the while not manifest_done loop
            if not self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
                # NOTE:MANIFEST_READER=part2a-ALT-CSV:
                # loading manifest from CSV
                curr_manifest_row = self.locutus_settings.get_next_nontrivial_row_via_manifest_CSV()
            else:
                # NOTE:MANIFEST_READER=part2a-ALT-DB:
                # manifest-once loading batch from DB:
                curr_manifest_row = self.locutus_settings.get_next_acc_via_batch_cursor()
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): partA head of while not manifest_done parsing input manifest body line of "{1}"'.format(CLASS_PRINTNAME, curr_manifest_row), flush=True)
        except StopIteration as e:
            manifest_done = True
        while not manifest_done:
            if not processed_uuids_in_this_phase:
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.Process(): START of PHASE: phase processing this batch...'.format(CLASS_PRINTNAME), flush=True)
                processed_uuids_in_this_phase = True

            if self.locutus_settings.LOCUTUS_VERBOSE:
                # print a delimiting line between the Multi-UUIDs, especially useful between those found to NOT be actual Multi-UUIDs:
                print('- - - - - - - - - - - - - - - - - - - - - - - - - -     - - - - - - - - - - - - - - - - - - - - - - - - - - ', flush=True)

            total_comparisons_checked += 1
            updated_status_table_flag = False
            accession_duplicated = 'n/a'
            optional_resolved_comment = ''

            # NOTE: now introducing more robust handling into the expected input columns:
            ##################
            # WARNING: the below str != str(int(str)) check is quite comparable to that in cmd_dicom_stage_compare.py,
            # with the latter's employing a somewhat alternative approach via re.match("\d+", accession_num),
            # perhaps leading to different results for any w/ mid-num text, '123abc456'.
            ##################

            # NOTE: SAFETY CHECK to support both get_next_acc_via_batch_cursor() & get_next_nontrivial_row_via_manifest_CSV(), just in case:
            if not curr_manifest_row:
                # TODO: can add more info here
                print('ERROR: safety check throwing exception prior rather than dump core via NULL curr_manifest_row.'.format(result_row), flush=True)
                raise StopIteration

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('**DEBUG**: Summarizer post-SAFETY CHECK, input_manifest_source_column={0}, curr_manifest_row={1}.'.format(
                        self.locutus_settings.input_manifest_source_column, curr_manifest_row), flush=True)

            ####################
            # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
            accession_str = curr_manifest_row[self.locutus_settings.input_manifest_source_column]

            accession_num_int = src_modules.settings.atoi(accession_str)
            accession_restr = src_modules.settings.itoa(accession_num_int)

            if (accession_str != accession_restr):
                ####################
                # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.Process(): found an alpha-numeric accession_str ~!= accession_num_int:  \'{1}\' ~!= itoa(\'{2}\')=={3} ;  '\
                        'Keeping calm and carrying on w/ accession_str=\'{1}\', courtesy of the Juneteenth 2025 alpha-numeric accessions Upgrade.'.format(
                        CLASS_PRINTNAME,
                        accession_str,
                        accession_num_int,
                        accession_restr),
                        flush=True)

                #if self.locutus_settings.LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS:
                #    accession_str = accession_restr
                #    total_accessions_with_text_NOT_removed += 1
                # STILL, go ahead and tally up those with text included at all, even if no longer removed:
                total_accessions_with_text_NOT_removed += 1
                ##########
                # FORCING the raw accession_str back into accession_restr for "safe_acc_num_str" to later use:
                accession_restr = accession_str
                ##########
                # NOTE: self.locutus_settings.LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS deprecated
                ##########
            curr_multiuuid_resolve_cmd = ''
            if self.locutus_settings.manifest_header_ver==src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER_to_RESOLVE_MULTIUUIDS:
                curr_multiuuid_resolve_cmd = curr_manifest_row[src_modules.settings.ONPREM_MANIFEST_RESOLVEVIA_COLUMN_OFFSET]

            ####################
            # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
            # NOTE: Q: NO need to even cast these into floats anymore, riiiiight? right.
            this_accession_is_duplicate = False
            if accession_str in accession_list:
                this_accession_is_duplicate = True
                accession_duplicated = ACCESSION_DUPLICATED
                total_duplicates += 1
            accession_list.append(accession_str)
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): Looking for accession # `{1}` (from input column offset {2}) within DICOM module `{3}` in table `{4}`'.format(
                                    CLASS_PRINTNAME,
                                    accession_str,
                                    self.locutus_settings.input_manifest_source_column,
                                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE,
                                    self.manifest_table),
                                    flush=True)
            # BATCHES NOTE: filtering by batch_clause for the following accession query of manifest_status:
            manifest_status_result = self.LocutusDBconnSession.execute('SELECT manifest_status, '\
                                    'last_datetime_processed '\
                                    'FROM {0} '\
                                    'WHERE accession_num=\'{1}\' '\
                                    'AND active {2} ;'.format(
                                    self.manifest_table,
                                    accession_str,
                                    batch_clause))
            manifest_status_row = manifest_status_result.fetchone()
            manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND

            status_targets = ''
            status_target = ''
            status_deid_datetime = ''
            status_deid_qc_status = ''
            status_deid_qc_explorer = ''
            if manifest_status_row:
                manifest_status = manifest_status_row['manifest_status']
                status_deid_datetime = manifest_status_row['last_datetime_processed']
                # and for this non-empty manifest row, find its corresponding status row's output target:
                module_xtra_fields_msg = ''
                module_xtra_fields_cols = ''
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM:
                    # DICOM_MODULE_ONPREM::
                    # NOTE: preface w/ commas for since these are optional strings:
                    module_xtra_fields_msg = ', deid_qc_status field \'{0}\', and deid_qc_explorer field \'{1}\''.format(
                                                self.status_deid_qc_status_field,
                                                self.status_deid_qc_explorer_field)
                    module_xtra_fields_cols = ', {0}, {1}'.format(
                                                self.status_deid_qc_status_field,
                                                self.status_deid_qc_explorer_field)

                # NOTE: assemble the extra deid_qc_* fields ONLY as an option for module ONPREM
                # NOTE: no column before {5} in case its optional fields are empty:
                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.Process(): Looking for accession # `{1}` within DICOM module `{2}` in table `{3}` '\
                            'for output target field `{4}`{5}'.format(
                                        CLASS_PRINTNAME,
                                        accession_str,
                                        self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE,
                                        self.status_table,
                                        self.status_target_field,
                                        module_xtra_fields_msg),
                                        flush=True)
                # BATCHES NOTE: filtering by batch_clause for the following accession query of manifest_status:
                status_target_result = self.LocutusDBconnSession.execute('SELECT {0}{1} '\
                                        'FROM {2} '\
                                        'WHERE accession_num=\'{3}\' AND (change_seq_id>0) '\
                                        'AND active {4} ;'.format(
                                        self.status_target_field,
                                        module_xtra_fields_cols,
                                        self.status_table,
                                        accession_str,
                                        batch_clause))
                status_target_row = status_target_result.fetchone()
                if status_target_row:
                    status_targets = status_target_row[self.status_target_field]
                    # retrieve the most recent target, last, of any comma-delimited targets:
                    if status_targets:
                        status_target = status_targets.split(',')[-1]
                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM:
                        status_deid_qc_status = status_target_row[self.status_deid_qc_status_field]
                        status_deid_qc_explorer = status_target_row[self.status_deid_qc_explorer_field]


            # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
            # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
            #if self.locutus_settings.LOCUTUS_VERBOSE:
            # NOTE: COULD quote the manifest_status due to some of the `PREVIOUS_PROCESSING_USED_[oldval1,oldval2,oldval3]_PRERETIRE_etc[newval1,newval2,newval3]`
            # BUT let's make a safe version of it here in the same form that the processing will eventually be updated :
            # namely, use a '/' rather than ',' to separate:
            if not manifest_status:
                manifest_status = '[NONE]'
            manifest_status = manifest_status.replace(',', '/')
            #
            # And....create the initial manifest columns and optional xtra columns as per the module+manifest being summarized:
            module_specific_manifest_columns = ''
            module_specific_xtra_columns = ''
            # TODO: create a method for generating such module_specific_manifest_columns for MANIFEST_TYPE_DEFAULT_SIMPLIFIED.
            # especially now that expand_multiuuids_to_manifest_output() is repurposing them:
            #
            # BATCHES NOTE: even w/ LOCUTUS_BATCHES_ENABLE/LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB, input_manifest_type has been set for this output by Setup():
            if self.locutus_settings.input_manifest_type == src_modules.settings.MANIFEST_TYPE_DEFAULT_SIMPLIFIED:
                # be sure to leave an empty column for the data-less SIMPLIFIED_MANIFEST_HEADER_MANIFEST_VER:
                module_specific_manifest_columns = '{0},'.format(accession_str)
            else:
                # shared operations for both self.input_manifest_type in (ONPREM_MANIFEST_TYPE):
                # TODO: use os.path to split, but for now:
                status_target_basename = ''
                if status_targets:
                    status_target_basename = status_targets.split('/')[-1]

                # and specific operations for each self.input_manifest_type:
                if self.locutus_settings.input_manifest_type == src_modules.settings.ONPREM_MANIFEST_TYPE:
                    # ONPREM-specific versioned manifest:
                    #
                    # NOTE: essentially, just the first ONPREM_MANIFEST_NUM_HEADERS rows as such:
                    # module_specific_manifest_columns = ','.join(curr_manifest_row[0:(ONPREM_MANIFEST_NUM_HEADERS-1)])
                    ######
                    # TODO: create a function for the following fw_session_import_arg, since now used in multiple places:
                    # NOTE: generate the helper column for downstream import into Flywheel via --session <age>d_<location>
                    # (as copied from that used in module_onprem_dicom.py)
                    ######
                    curr_subject_id = curr_manifest_row[src_modules.settings.ONPREM_MANIFEST_SUBJ_COLUMN_OFFSET]
                    curr_fw_subject_import_arg = '{0}'.format(curr_subject_id)
                    #
                    # curr_object_info_01 not currently used, but still load for comparison:
                    curr_object_info_01 = curr_manifest_row[src_modules.settings.ONPREM_MANIFEST_OBJ01_COLUMN_OFFSET]
                    #
                    curr_object_info_02 = curr_manifest_row[src_modules.settings.ONPREM_MANIFEST_OBJ02_COLUMN_OFFSET]
                    curr_object_info_03 = curr_manifest_row[src_modules.settings.ONPREM_MANIFEST_OBJ03_COLUMN_OFFSET]
                    #WAS: curr_fw_session_import_arg = '{0}d_{1}'.format(curr_object_info_02, curr_object_info_03)
                    # NOTE: BEWARE: curr_fw_session_import_arg is now also generated in module_onprem_dicom.py
                    curr_fw_session_import_arg = ''
                    if curr_object_info_02 or curr_object_info_03:
                        curr_fw_session_import_arg = '{0}d_{1}'.format(curr_object_info_02, curr_object_info_03)
                    #
                    curr_deid_qc_status = curr_manifest_row[src_modules.settings.ONPREM_MANIFEST_DEID_QC_STATUS_OFFSET]

                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.Process(): VERBOSE: accession # `{1}` has raw OnPrem manifest_status=`{2}`'.format(
                                            CLASS_PRINTNAME,
                                            accession_str,
                                            manifest_status),
                                            flush=True)

                    # TODO: create a method for generating such module_specific_manifest_columns for ONPREM_MANIFEST_TYPE.
                    # especially now that expand_multiuuids_to_manifest_output() is repurposing them:
                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS:
                        # build up manually:
                        # TODO: consider an approach which could detect a fullstop '.' and extract the sub-accession part, e.g., the '.001',
                        # and then to share it as REDACTED.001, REDACTED.002, etc..
                        # FIRST PASS ATTEMPT:
                        module_specific_manifest_columns = ','.join([curr_subject_id, \
                                                                    curr_object_info_01, curr_object_info_02, curr_object_info_03, \
                                                                    'REDACTED_ACCESSION', \
                                                                    curr_deid_qc_status])
                        # TODO: consider just using a closer variation of this for the non-redacted version below as well:
                    else:
                        # if no redaction, can just use the entire curr_manifest_row as is:
                        module_specific_manifest_columns = ','.join(curr_manifest_row[0:(src_modules.settings.ONPREM_MANIFEST_NUM_HEADERS-1)])
                    # be sure to leave an empty column for the data-less ONPREM_MANIFEST_HEADER_MANIFEST_VER:
                    module_specific_manifest_columns += ','

                    if (manifest_status == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED) \
                        or (('PREVIOUS_PROCESSING_USED_' not in manifest_status) \
                            and (   (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX) \
                                 or (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)) ):
                        # NOTE: potentially add previously processed manifest attributes as PREVIOUS_PROCESSING_USED_,
                        # so long as a PREVIOUS_PROCESSING_USED_ isn't already appended to the end of the PROCESSED or MULTIPLES status.
                        # BATCHES NOTE: filtering by batch_clause for the following accession query of manifest_status:
                        manifest_attributes_result = self.LocutusDBconnSession.execute('SELECT subject_id, '\
                                                'object_info_01, object_info_02, object_info_03 '\
                                                'FROM {0} '\
                                                'WHERE accession_num=\'{1}\' '\
                                                'AND active {2} ;'.format(
                                                self.manifest_table,
                                                accession_str,
                                                batch_clause))
                        manifest_attributes_row = manifest_attributes_result.fetchone()
                        prev_subject_id = manifest_attributes_row['subject_id']
                        prev_object_info_01 = manifest_attributes_row['object_info_01']
                        prev_object_info_02 = manifest_attributes_row['object_info_02']
                        prev_object_info_03 = manifest_attributes_row['object_info_03']
                        if (curr_subject_id != prev_subject_id) or \
                            (curr_object_info_01 != prev_object_info_01) or \
                            (curr_object_info_02 != prev_object_info_02) or \
                            (curr_object_info_03 != prev_object_info_03):
                            if manifest_status == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED:
                                # for PROCESSED, replace the status entirely:
                                manifest_status = 'PREVIOUS_PROCESSING_USED_'
                            else:
                                # for MULTIPLEs (either ERROR or already SPLIT), append:
                                manifest_status += '_' + self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX  # 'PREVIOUS_PROCESSING_USED_'
                            manifest_status +='[{0}/{1}/{2}/{3}]'.format(prev_subject_id, prev_object_info_01, prev_object_info_02, prev_object_info_03)
                            manifest_status += self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_MIDFIX    # '_PRERETIRE_OR_PREDELETE_OR_FORCE_TO_REPROCESS_'
                            manifest_status +='[{0}/{1}/{2}/{3}]'.format(curr_subject_id, curr_object_info_01, curr_object_info_02, curr_object_info_03)
                        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): VERBOSE: accession # `{1}` has modified OnPrem manifest_status=`{2}`'.format(
                                                CLASS_PRINTNAME,
                                                accession_str,
                                                manifest_status),
                                                flush=True)

                    #####
                    # SETUP OnPrem subject & object attributes to pass in:
                    preset_select_info = 'subject_id, \'{0}\' as proposed_subject_id, object_info_01, \'{1}\' as proposed_object_info_01, '\
                                                    'object_info_02, \'{2}\' as proposed_curr_object_info_02, object_info_03, \'{3}\' as proposed_curr_object_info_03 '\
                                                    .format(curr_subject_id, curr_object_info_01, curr_object_info_02, curr_object_info_03)
                    preset_update_info = 'subject_id=\'{0}\', object_info_01=\'{1}\', object_info_02=\'{2}\', object_info_03=\'{3}\' '\
                                                    .format(curr_subject_id, curr_object_info_01, curr_object_info_02, curr_object_info_03)
                    preload_manifest_columns_sans_status = 'subject_id, object_info_01, object_info_02, object_info_03'
                    preload_select_info = preset_select_info
                    preload_insert_info = '\'{0}\', \'{1}\', \'{2}\', \'{3}\' '\
                                                    .format(curr_subject_id, curr_object_info_01, curr_object_info_02, curr_object_info_03)
                    preload_update_info = preset_update_info
                    #####

                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST:
                        # OnPrem NOTE: Now, Let's go ahead and PRELOAD all of them!
                        #
                        # PRE-PROCESSING PRE-LOAD initially only picked up all previously NOT_FOUND
                        # but not any PENDING_CHANGE% nor any other for which a MANIFEST record is already implied;
                        # pre-load the same OnPrem manifest_status for any accessions NOT yet FOUND with a MANIFEST record:
                        # <><><><><><><><>< !!!!!!! <><><><><><><><> !!!!!!!! <><><><><><><><><>
                        # NOT just those above NOT_FOUND which are to be INSERTED to be added,
                        # but ALSO any other already existing MANIFEST record, as well as corresponding STATUS records,
                        # for best determining/updating PENDING_CHANGEs, MULTIPLE_CHANGE_UUIDs
                        # and even retaining the previous manifest_status value beyond the ONDECK_PROCESSING_CHANGE_PREFIX="ZZZ-ONDECK-4-PROCESSING_CHANGE"
                        # and suffix.
                        #################################
                        # <><><><><><><><>< !!!!!!! <><><><><><><><> !!!!!!!! <><><><><><><><><>
                        ######
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('VERBOSE: OnPrem **LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST**, w/ manifest_status={2}, so calling preload_accession(accession_num=\'{0}\', suffix={1}, batches_enable={3}, batch_name=\'{4}\')'.format(
                                    accession_str, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST_PREPROCESSING_SUFFIX,
                                    manifest_status, self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB, self.locutus_settings.LOCUTUS_BATCH_NAME), flush=True)
                        # now, call it: (adding curr_subject_id to the end, for ease of adding to global BATCHES table, even though embedded in preload_* info for the MANIFEST+STATUS tables)
                        (successful, manifest_status) = self.preload_accession(manifest_status, accession_str, preload_manifest_columns_sans_status, preload_select_info, preload_insert_info, preload_update_info, \
                                                                                self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST_PREPROCESSING_SUFFIX, \
                                                                                self.locutus_settings.LOCUTUS_BATCH_NAME,
                                                                                self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper(),
                                                                                self.locutus_settings.LOCUTUS_WORKSPACE_NAME,
                                                                                curr_subject_id)
                        total_preloaded_accessions += 1
                        ######

                    if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS:
                        # REPROCESSING will pickup all previously PROCESSED%, ERROR%'d
                        # but bypass any PENDING_CHANGE%, as most will likely remain PENDING.
                        # preset the same OnPrem manifest_status for a similarly expected manifest_status subset:
                        if ( (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING)] != self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING) ) :
                            # NOW also with %MULTIPLE%s, but ONLY their times in the MANIFEST table,
                            # to ensure that they likewise show up in SQL queries for:
                            #   last_datetime_processing > [batch start time]
                            only_update_manifest_time = False
                            if ( (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX) \
                                or (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX) ):
                                only_update_manifest_time = True
                            #########
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: OnPrem **LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS**, so calling preset_accession_for_reprocessing(accession_num=\'{0}\', suffix={1})'.format(
                                        accession_str, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS_SUFFIX), flush=True)
                            # now, call it:
                            (successful, manifest_status) = self.preset_accession_for_reprocessing(accession_str, manifest_status, only_update_manifest_time,
                                                                        preset_select_info, preset_update_info,
                                                                        self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS_SUFFIX)
                            total_preset_accessions += 1
                    # even with VERBOSE, can quiet these down:
                    #elif self.locutus_settings.LOCUTUS_VERBOSE:
                    #    print('VERBOSE: ***NOT*** LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS, so **NOT** calling preset_accession_for_reprocessing('\
                    #                'accession_str={0}, suffix={1}), manifest_status={2}'.format(
                    #                accession_str, self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS_SUFFIX, manifest_status), flush=True)

                    # TODO: create a method for generating such module_specific_manifest_columns for ONPREM_MANIFEST_TYPE.
                    # even if expand_multiuuids_to_manifest_output() isn't repurposing these module_specific_xtra_columns:
                    #
                    # be sure to keep a leading "," before this optional column:
                    # NOTE: including empty initial field for data-less MANIFEST_OUTPUT_ONPREM_EXTRAS column:
                    module_specific_xtra_columns = ',,{0},{1},{2},{3},{4},{5},{6}'.format(
                        status_target_basename,
                        self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_ONPREM_FLYWHEEL_GROUP,
                        self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_ONPREM_FLYWHEEL_PROJECT,
                        curr_fw_subject_import_arg,
                        curr_fw_session_import_arg,
                        status_deid_qc_status,
                        status_deid_qc_explorer)

            ##############################################################
            # NOTE: leaving the data-less MANIFEST_HEADER_SUMMARIZED_FROM column blank (',,')

            # WARNING: LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS write-mode

            # ATTEMPT #0, based entirely off of the MANIFEST_STATUS:
            #if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS:
            #    if ((manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX) or \
            #        (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)):
            # WARNING: the above will be limited in ability to differently process the following two options:
            # 1) RESOLVE_VIA: MERGE_RADIOLOGY
            # 2) RESOLVE_VIA: CONSOLIDATE_LOCALLY
            # So, instead, if using the new ONPREM_MANIFEST_HEADER_MANIFEST_VER_to_RESOLVE_MULTIUUIDS manifest format,
            # then merely utilize the ONPREM_MANIFEST_HEADER_RESOLVEVIA command column at ONPREM_MANIFEST_RESOLVEVIA_COLUMN_OFFSET
            # 1st, then... HOW to detect the cvs_ver?
            # TODO: expose the csv_header_ver from the Setup() function, perhaps a self.Settings.... == self.input_manifest_version! :-D
            ##########

            #####
            # NEXT == to read the RESOLVE_VIA_CMD from the manifest row, and pass it on to resolve_multiuuids
            # AND THEN == add each of these to an accs_resolved list, perhaps returning the whole number from each call to:
            #####

            # Q: why is the current test not falling into this, w/a manifest_ver of:
            #       locutus_manifest_ver:locutus.onprem_dicom.resolve_multiuuids.2024oct22
            # NOTE: wasn't yet added in as a recognized module above.  DONE.
            # NOTE: as such, the temporary DEBUG message below is NOT normally needed
            #print('DEBUG: JUST before check of LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS={0}, '\
            #    'DICOMmodule={1}, w/ manifest_header_ver={1}...'.format(
            #        self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS,
            #        self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper(),
            #        self.manifest_header_ver), flush=True)
            ##########
            #if curr_multiuuid_resolve_cmd:
            #    print('DEBUG: and the curr_multiuuid_resolve_cmd={0}'.format( \
            #        curr_multiuuid_resolve_cmd), flush=True)
            # NOTE: temporary DEBUG message below is NOT normally needed
            #else:
            #    print('DEBUG: WARNING - found an empty resolve_cmd ! ! ! ', flush=True)

            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS \
                and (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM \
                        and self.locutus_settings.manifest_header_ver==src_modules.settings.ONPREM_MANIFEST_HEADER_MANIFEST_VER_to_RESOLVE_MULTIUUIDS) \
                and not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS:

                #######################################
                # NOTE: limit the Resolver to *ACTUAL* Multi-UUIDs for such RESOLVE_CMD calls of self.resolve_multiuuids()
                # tally up the # of DISTINCT(uuid) for this accession in this workspace+batch (can't check all of em)
                #######################################
                # NOTE: add tally of workspace COUNT(DISTINCT(accession_num)) to better help confirm the correct Module & Workspace:
                total_distinct_accession_uuids_in_workspace = 0
                # BATCHES NOTE: checking across all batches:
                num_uuids_status_results = self.LocutusDBconnSession.execute('SELECT COUNT(DISTINCT(uuid)) as num '\
                                        'FROM {0} '\
                                        'WHERE accession_num=\'{1}\' '\
                                        'AND active ;'.format(
                                        self.status_table,
                                        accession_str))
                num_uuids_status_row = num_uuids_status_results.fetchone()
                total_distinct_accession_uuids_in_workspace = num_uuids_status_row['num']
                #######################################


                if curr_multiuuid_resolve_cmd:
                    print('LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: curr_multiuuid_resolve_cmd=\'{0}\''.format( \
                        curr_multiuuid_resolve_cmd), flush=True)

                # leaving the next as a new starting if to the next if/elif ladder:
                if total_distinct_accession_uuids_in_workspace < 2:
                    # WARNING: not a real Multi-UUID:
                    print('WARNING: LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS ignoring resolv_cmd=\'{0}\' for accession_str=\'{1}\' w/ manifest_status=\'{2}\' since total_distinct_accession_uuids_in_workspace={3}'.format(
                        curr_multiuuid_resolve_cmd, accession_str, manifest_status, total_distinct_accession_uuids_in_workspace), flush=True)
                elif not curr_multiuuid_resolve_cmd:
                    print('WARNING: LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS ignoring an empty resolv_cmd=\'{0}\' for accession_str=\'{1}\' w/ manifest_status=\'{2}\''.format(
                        curr_multiuuid_resolve_cmd, accession_str, manifest_status), flush=True)
                elif (curr_multiuuid_resolve_cmd != RESOLVE_VIA_MERGE_RADIOLOGY) \
                    and (curr_multiuuid_resolve_cmd != RESOLVE_VIA_RESEND_RADIOLOGY) \
                    and (curr_multiuuid_resolve_cmd != RESOLVE_VIA_CONSOLIDATE_LOCALLY) \
                    and (curr_multiuuid_resolve_cmd != RESOLVE_VIA_DELETE_LOCALLY) \
                    and (curr_multiuuid_resolve_cmd != RESOLVE_VIA_DELETE_STAGE_ONLY) \
                    and (curr_multiuuid_resolve_cmd[:len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX)] != RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX):
                    # TODO: consider adding to ^^^^ a len(curr_multiuuid_resolve_cmd) >? len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX)
                    print('ERROR: unknown resolv_cmd=\'{0}\' for accession_str=\'{1}\' w/ manifest_status=\'{2}\';'\
                        'expects RESOLVE_VIA of <{3}, {4}, {5}, {6}, {7}, or {8}>'.format(
                        curr_multiuuid_resolve_cmd, accession_str, manifest_status,
                        RESOLVE_VIA_MERGE_RADIOLOGY, RESOLVE_VIA_RESEND_RADIOLOGY,
                        RESOLVE_VIA_CONSOLIDATE_LOCALLY, RESOLVE_VIA_DELETE_LOCALLY,
                        RESOLVE_VIA_DELETE_STAGE_ONLY, RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX), flush=True)
                    manifest_status = ERROR_RESOLVING_PREFIX + ':unknown_resolve_cmd=' + curr_multiuuid_resolve_cmd
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: **DOING** resolv_cmd=\'{0}\' for accession_str=\'{1}\' w/ manifest_status=\'{2}\''.format(
                            curr_multiuuid_resolve_cmd, accession_str, manifest_status), flush=True)
                    previously_resolved = False
                    (resolve_success, resolved_accession_num, updated_manifest_status, recently_resolved, formerly_resolved) \
                        = self.resolve_multiuuids(accession_str, curr_multiuuid_resolve_cmd, manifest_status, preload_manifest_columns_sans_status, preset_update_info,
                                                    self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB,
                                                    self.locutus_settings.LOCUTUS_BATCH_NAME)

                    print('Summarizer::Process() received the following from its resolve_cmd=\'{0}\' for accession \'{1}\': '\
                        'resolve_success={2}, resolved_accession_num=\'{3}\', updated_manifest_status=\'{4}\', recently_resolved={5}, formerly_resolved={6}'.format(
                            curr_multiuuid_resolve_cmd, accession_str,
                            resolve_success, resolved_accession_num, updated_manifest_status, recently_resolved, formerly_resolved),
                            flush=True)

                    # TODO: definitely explore a better (ahem, actual) data structure for these.
                    if resolve_success:
                        if curr_multiuuid_resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY:
                            total_resolved_successfully_viaRADIOLOGY_MERGE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_successfully_viaRADIOLOGY_MERGE to.... {0}'.format(total_resolved_successfully_viaRADIOLOGY_MERGE), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_RESEND_RADIOLOGY:
                            total_resolved_successfully_viaRADIOLOGY_RESEND += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_successfully_viaRADIOLOGY_RESEND to.... {0}'.format(total_resolved_successfully_viaRADIOLOGY_RESEND), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
                            total_resolved_successfully_viaCONSOLIDATE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_successfully_viaCONSOLIDATE to.... {0}'.format(total_resolved_successfully_viaCONSOLIDATE), flush=True)
                        elif curr_multiuuid_resolve_cmd[:len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX)] == RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX:
                            total_resolved_successfully_viaCHOICE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_successfully_viaCHOICE to.... {0}'.format(total_resolved_successfully_viaCHOICE), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY:
                            total_resolved_successfully_viaDELETE_LOCAL += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_successfully_viaDELETE_LOCAL to.... {0}'.format(total_resolved_successfully_viaDELETE_LOCAL), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_DELETE_STAGE_ONLY:
                            total_resolved_successfully_viaDELETE_STAGE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_successfully_viaDELETE_STAGE to.... {0}'.format(total_resolved_successfully_viaDELETE_STAGE), flush=True)
                    elif recently_resolved:
                        if curr_multiuuid_resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY:
                            total_resolved_recently_viaRADIOLOGY_MERGE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_recently_viaRADIOLOGY_MERGE to.... {0}'.format(total_resolved_recently_viaRADIOLOGY_MERGE), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_RESEND_RADIOLOGY:
                            total_resolved_recently_viaRADIOLOGY_RESEND += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_recently_viaRADIOLOGY_RESEND to.... {0}'.format(total_resolved_recently_viaRADIOLOGY_RESEND), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
                            total_resolved_recently_viaCONSOLIDATE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_recently_viaCONSOLIDATE to.... {0}'.format(total_resolved_recently_viaCONSOLIDATE), flush=True)
                        elif curr_multiuuid_resolve_cmd[:len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX)] == RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX:
                            total_resolved_recently_viaCHOICE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_recently_viaCHOICE to.... {0}'.format(total_resolved_recently_viaCHOICE), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY:
                            total_resolved_recently_viaDELETE_LOCAL += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_recently_viaDELETE_LOCAL to.... {0}'.format(total_resolved_recently_viaDELETE_LOCAL), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_DELETE_STAGE_ONLY:
                            total_resolved_recently_viaDELETE_STAGE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_recently_viaDELETE_STAGE to.... {0}'.format(total_resolved_recently_viaDELETE_STAGE), flush=True)
                    elif formerly_resolved:
                        if curr_multiuuid_resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY:
                            total_resolved_formerly_viaRADIOLOGY_MERGE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_formerly_viaRADIOLOGY_MERGE to.... {0}'.format(total_resolved_formerly_viaRADIOLOGY_MERGE), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_RESEND_RADIOLOGY:
                            total_resolved_formerly_viaRADIOLOGY_RESEND += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_formerly_viaRADIOLOGY_RESEND to.... {0}'.format(total_resolved_formerly_viaRADIOLOGY_RESEND), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
                            total_resolved_formerly_viaCONSOLIDATE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_formerly_viaCONSOLIDATE to.... {0}'.format(total_resolved_formerly_viaCONSOLIDATE), flush=True)
                        elif curr_multiuuid_resolve_cmd[:len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX)] == RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX:
                            total_resolved_formerly_viaCHOICE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_formerly_viaCHOICE to.... {0}'.format(total_resolved_formerly_viaCHOICE), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY:
                            total_resolved_formerly_viaDELETE_LOCAL += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_formerly_viaDELETE_LOCAL to.... {0}'.format(total_resolved_formerly_viaDELETE_LOCAL), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_DELETE_STAGE_ONLY:
                            total_resolved_formerly_viaDELETE_STAGE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing total_resolved_formerly_viaDELETE_STAGE to.... {0}'.format(total_resolved_formerly_viaDELETE_STAGE), flush=True)
                    else:
                        if curr_multiuuid_resolve_cmd == RESOLVE_VIA_MERGE_RADIOLOGY:
                            errors_resolving_viaRADIOLOGY_MERGE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing errors_resolving_viaRADIOLOGY_MERGE for ERRORS to.... {0}'.format(errors_resolving_viaRADIOLOGY_MERGE), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_RESEND_RADIOLOGY:
                            errors_resolving_viaRADIOLOGY_RESEND += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing errors_resolving_viaRADIOLOGY_RESEND for ERRORS to.... {0}'.format(errors_resolving_viaRADIOLOGY_RESEND), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_CONSOLIDATE_LOCALLY:
                            errors_resolving_viaCONSOLIDATE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing errors_resolving_viaCONSOLIDATE for ERRORS to.... {0}'.format(errors_resolving_viaCONSOLIDATE), flush=True)
                        elif curr_multiuuid_resolve_cmd[:len(RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX)] == RESOLVE_VIA_CHOOSE_LOCALLY_PREFIX:
                            errors_resolving_viaCHOICE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing errors_resolving_viaCHOICE for ERRORS to.... {0}'.format(errors_resolving_viaCHOICE), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_DELETE_LOCALLY:
                            errors_resolving_viaDELETE_LOCAL += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing errors_resolving_viaDELETE_LOCAL for ERRORS to.... {0}'.format(errors_resolving_viaDELETE_LOCAL), flush=True)
                        elif curr_multiuuid_resolve_cmd == RESOLVE_VIA_DELETE_STAGE_ONLY:
                            errors_resolving_viaDELETE_STAGE += 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing errors_resolving_viaDELETE_STAGE for ERRORS to.... {0}'.format(errors_resolving_viaDELETE_STAGE), flush=True)

                        else:
                            # and others, such as unknown resolve sub commands:
                            errors_resolving_viaOTHER+= 1
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: post-LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS: incrementing errors_resolving_viaOTHER for ERRORS to.... {0}'.format(errors_resolving_viaOTHER), flush=True)

                    # TODO: update the active accession with this newly resolved one!
                    #ASAP, else the any .001, .002, etc. sub-acc# will still show
                    # will NEED to do a .replace in:
                    if recently_resolved:
                        # a) those recently resolved during this very session:
                        # the whole number of this accession has already been resolved, hoping to insert a comment in front of this MANIFEST_OUTPUT
                        optional_resolved_comment = '# RESOLVED Multi-UUID to \'{0}\': '.format(resolved_accession_num)
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('VERBOSE: JUST RECENTLY resolved the group of sub-accessions for this accession_str={0} with resolved_accession_num=\'{1}\'; '\
                                'to preface its input manifest row with the following comment: {2}'.format(
                                accession_str, resolved_accession_num, optional_resolved_comment), flush=True)
                    if formerly_resolved:
                        # b) those resolved in previous sessions and already with a PENDING_CHANGE_RADIOLOGY_MERG
                        # no need for any comment, can leave it exactly as is:
                        optional_resolved_comment = ''
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('VERBOSE: FORMERLY resolved the group of sub-accessions for this accession_str={0} with resolved_accession_num=\'{1}\'.'.format(
                                accession_str, resolved_accession_num), flush=True)
                    elif resolve_success:
                        # this is a newly resolved accession_num, do replace its manifest_input accession to the resolved whole number:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('VERBOSE: **TRYING** to replace accession_str={0} with resolved_accession_num=\'{1}\' into: module_specific_manifest_columns={2}'.format(
                                accession_str, resolved_accession_num, module_specific_manifest_columns), flush=True)

                        module_specific_manifest_columns = module_specific_manifest_columns.replace(accession_str, resolved_accession_num)

                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('VERBOSE: **CHECKING** supposedly replaced accession_str={0} with resolved_accession_num=\'{1}\' into: module_specific_manifest_columns={2}'.format(
                                accession_str, resolved_accession_num, module_specific_manifest_columns), flush=True)

                        accession_str = resolved_accession_num
                    #else:
                    #    # ERROR in resolving, so leave the accession_str as is, but do still set the new manifest (since w/ error info), either way:
                    manifest_status = updated_manifest_status

                    # HERE: TODO:
                    # add a list of all resolved accession_strs!!!
                    # AND USE it to double-check any PRIOR to doing the resolution, either here OR elsewhere
                    # Q: for those found to already be resolved (e.g., sub-accessions in the same), can we emit a COMMENTED version of those to MANIFEST_OUTPUT?
                    #
                    #######
                    # NOTE: see also the new expand_multiuuids_to_manifest_output() method
                    #######
                    #
                    # NOTE: this could be an earlier post-split manifest, with sub-accessions .001, .002, etc.
                    # TODO: ensure that this is efficient for any such redundant calls, as the multi-uuids shall have been resolved with the first such call
            if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('DEBUG: LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS for accession_num=\'{0}\''.format(accession_str), flush=True)

                ####################
                # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
                # NOTE: Q: NO need to even cast these into floats anymore, riiiiight? right.
                ####################
                ####################
                # Q: will we still want to include any sub/whole statistics?
                # especially w/ the few remaining SPLIT accessions no longer as status quo
                # NOTE: for now, see how the above fix rolls on through to the stats.
                ####################

                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS and \
                    ((manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX \
                        and not ERROR_RESOLVING_PREFIX in manifest_status.upper()) \
                    or (manifest_status.upper()[:len(self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX)] == self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX ) \
                    or (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX) ):
                        print('DEBUG: LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS about to expand_multiuuids_to_manifest_output() for accession_num=\'{0}\''.format(accession_str), flush=True)
                        multiuuid_accessions_shown = self.expand_multiuuids_to_manifest_output(multiuuid_accessions_shown, accession_str, accession_duplicated, manifest_status,
                                                                    batch_clause=batch_clause,
                                                                    commented=True, orthanc_comment='via_expand_multiuuids_to_manifest_output()')
                print('{0},{1}{2},{3},{4},{5},{6}{7}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        optional_resolved_comment,
                        module_specific_manifest_columns,
                        accession_duplicated,
                        manifest_status,
                        status_deid_datetime,
                        status_target,
                        module_specific_xtra_columns),
                        flush=True)

                if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS and \
                    ((manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX \
                        and not ERROR_RESOLVING_PREFIX in manifest_status.upper()) \
                    or (manifest_status.upper()[:len(self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX)] == self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX ) \
                    or (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX) ):
                    # NOTE: another of the same row of hashes to FOLLOW the main accession to help better delineate , if showing multiuuids:
                    print('{0},#################################################'.format(MANIFEST_OUTPUT_PREFIX), flush=True)

                ############
                # NOTE: is this delay even needed anymore? Disabling 8/21/2024, until such time that a future need is encountered:
                #
                #print("DEBUG: sleeping 1 second(s) to slow down for buffer flushing....", flush=True)
                ## DEBUG time.sleep(10) WORKS, try faster 1second delays:
                #time.sleep(1)
                #print("DEBUG: done sleeping 1 second(s) to slow down for buffer flushing.", flush=True)

            # Summarize specifically known status values:
            #if manifest_status == self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND:
            # expand to a like self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND%, to include any potential resolve_multiuuid errors:
            if (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND):
                if this_accession_is_duplicate:
                    # NOTE: quietly tallying duplicates alongside, for later printing:
                    total_not_found_duplicates += 1
                else:
                    total_not_found += 1

            elif manifest_status.upper() == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: CHECKING FOR DUPLICATES before adding +1 to total_status_processsed={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_processsed, manifest_status, accession_str), flush=True)

                # for the "whole" accession counts, strip off any sub-accession `.001`, etc., by splitting at the `.`
                ####################
                # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
                # NOTE: Q: any need to even split down into the ints anymore? nope.
                ####################
                accession_int = accession_str.split('.')[0]
                ####################
                if accession_int not in processed_ints_accession_list:
                    # NOTE: ^^^^ could now be replaced with... if not this_accession_is_duplicate:
                    processed_ints_accession_list.append(accession_int)
                    total_status_processsed += 1
                    # sub-counter for any PROCESSED sub-accessions from a split, i.e., with a fractional '.001', '.002', etc.
                    if '.' in accession_str:
                        total_status_processsed_subaccessions += 1

                    if self.locutus_settings.LOCUTUS_VERBOSE \
                    and self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS:
                        print('VERBOSE: NEW accession; ADDING {0} to processed_ints_accession_list (len={1}) as {2}'.format(
                            accession_str, len(processed_ints_accession_list), accession_int), flush=True)
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('VERBOSE: AND adding +1 to give total_status_processsed={0}, manifest_status={1} for accession_str={2}'.format(
                                    total_status_processsed, manifest_status, accession_str), flush=True)
                else:
                    # NOTE: but tallying up the number of duplicates seen as PROCESSED:
                    total_status_processsed_duplicates += 1

                    if self.locutus_settings.LOCUTUS_VERBOSE \
                    and self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS:
                        print('VERBOSE: DUPLICATE already FOUND {0} in processed_ints_accession_list (len={1}) as {2}; LEAVING total_status_processsed={3}'.format(
                            accession_str, len(processed_ints_accession_list), accession_int, total_status_processsed), flush=True)

            elif manifest_status.upper() == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING:
                if this_accession_is_duplicate:
                    # NOTE: quietly tallying duplicates alongside, for later printing:
                    total_status_pending_duplicates += 1
                else:
                    total_status_pending += 1

                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_pending={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_pending, manifest_status, accession_str), flush=True)
            elif manifest_status.upper() == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_MERGE:
                if this_accession_is_duplicate:
                    # NOTE: (w/ general PENDING), quietly tallying duplicates alongside, for later printing:
                    total_status_pending_duplicates += 1
                else:
                    total_status_pending_radiology_merge += 1

                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_pending_radiology_merge={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_pending_radiology_merge, manifest_status, accession_str), flush=True)
            elif manifest_status.upper() == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_RESEND:
                if this_accession_is_duplicate:
                    # NOTE: (w/ general PENDING), quietly tallying duplicates alongside, for later printing:
                    total_status_pending_duplicates += 1
                else:
                    total_status_pending_radiology_resend += 1

                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_pending_radiology_resend={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_pending_radiology_resend, manifest_status, accession_str), flush=True)
            elif (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSING_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSING_PREFIX) :
                # TODO: Q: shall we consider a dupe count for those (non-core) PROCESSING as well?
                total_status_processing += 1
                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_processing={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_processing, manifest_status, accession_str), flush=True)
            elif (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSING_MOMENTARILY_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSING_MOMENTARILY_PREFIX) :
                # TODO: Q: shall we consider a dupe count for those (non-core) PROCESSING MOMENTARILY as well?
                total_status_processing_momentarily += 1
                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_processing_momentarily={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_processing_momentarily, manifest_status, accession_str), flush=True)
            elif ((manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX \
                    and not ERROR_RESOLVING_PREFIX in manifest_status.upper()) \
                or (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)):
                # NOTE: allow locally appended ERRORS_RESOLVING_PREFIX such as the following:
                # ACCESSION_HAS_MULTIPLE_SPLITS_PREVIOUS_PROCESSING_USED_[HM75VM5X_145870953308_739/]_PRERETIRE_OR_PREDELETE_TO_REPROCESS_[22q_0315/739]:ERROR_RESOLVING_MULTIUUID_CANDIDATE_6486329_FROM_6486329_to_6486330_via_CONSOLIDATE_LOCALLY
                # to fall down into the other section, for more explicit highlighting of it.
                #
                # TODO: EMIT A WARNING that these can be resolved with: self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS ???
                # For now, though, continue to count them both:
                #
                if (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX):
                    # TODO: Q: shall we consider a dupe count for those (non-core) MULTIPLE SPLITS as well?
                    total_status_multiple_splits += 1
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_multiple_splits={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_multiple_splits, manifest_status, accession_str), flush=True)
                elif (manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX):
                    if this_accession_is_duplicate:
                        # NOTE: quietly tallying duplicates alongside, for later printing:
                        total_status_multiuuids_duplicates += 1
                    else:
                        total_status_multiuuids += 1

                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_multiuuids={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_multiuuids, manifest_status, accession_str), flush=True)
                # NOTE: initially trying expand_multiuuids_to_manifest_output() right at the LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS:
                #if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS:
                #    self.expand_multiuuids_to_manifest_output(multiuuid_accessions_shown, accession_str, module_specific_manifest_columns, accession_duplicated, manifest_status, commented=True)
            elif manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_OTHER_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_OTHER_PREFIX:
                if this_accession_is_duplicate:
                    # NOTE: quietly tallying duplicates alongside, for later printing:
                    total_status_error_other_duplicates += 1
                else:
                    total_status_error_other += 1

                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_error_other={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_error_other, manifest_status, accession_str), flush=True)
                if manifest_status not in other_errors_found:
                    # NOTE: treat this as a set of distinct other_errors:
                    if manifest_status not in other_errors_found:
                        other_errors_found.append(manifest_status)
            elif manifest_status.upper()[:len(self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX)] == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX:
                if this_accession_is_duplicate:
                    # NOTE: quietly tallying duplicates alongside, for later printing:
                    total_status_previous_processsing_duplicates += 1
                else:
                    total_status_previous_processsing += 1

                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_previous_processsing={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_previous_processsing, manifest_status, accession_str), flush=True)
            elif manifest_status.upper()[:len(self.locutus_settings.ONDECK_OTHER_PREFIX)] == self.locutus_settings.ONDECK_OTHER_PREFIX:
                # ONDECK_OTHER_PREFIX is the base starting string for all ONDECK, now filter:
                if this_accession_is_duplicate:
                    # NOTE: (w/ general PRELOADED), quietly tallying duplicates alongside, for later printing:
                    total_preloaded_accessions_duplicates += 1
                elif manifest_status.upper()[:len(self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX)] == self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX:
                    total_preloaded_accessions_pending += 1
                elif manifest_status.upper()[:len(self.locutus_settings.ONDECK_PROCESSING_CHANGE_PREFIX)] == self.locutus_settings.ONDECK_PROCESSING_CHANGE_PREFIX:
                    total_preloaded_accessions_processing += 1
                elif manifest_status.upper()[:len(self.locutus_settings.ONDECK_REPROCESSING_CHANGE_PREFIX)] == self.locutus_settings.ONDECK_REPROCESSING_CHANGE_PREFIX:
                    total_preloaded_accessions_reprocessing += 1
                elif manifest_status.upper()[:len(self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX)] == self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX:
                    total_preloaded_accessions_multiples += 1
                else:
                    total_preloaded_accessions_other += 1
                    # NOTE: treat this as a set of distinct other_preloaded:
                    if manifest_status not in other_ondecks_found:
                        other_ondecks_found.append(manifest_status)
            else:
                total_status_other += 1
                if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('VERBOSE: adding +1 to total_status_other={0}, manifest_status={1} for accession_str={2}'.format(
                            total_status_other, manifest_status, accession_str), flush=True)
                # and ADD this other status to a list for the summary stats:
                if manifest_status not in other_statuses_found:
                    # TODO: consider adding a subcount to each unique other_status:
                    # NOTE: at least treat this as a set of distinct other_statuses:
                    if manifest_status not in other_statuses_found:
                        other_statuses_found.append(manifest_status)

            if self.locutus_settings.LOCUTUS_VERBOSE:
                # print a delimiting line between the Multi-UUIDs, especially useful between those found to NOT be actual Multi-UUIDs:
                print('- - - - - - - - - - - - - - - - - - - - - - - - - -     - - - - - - - - - - - - - - - - - - - - - - - - - - ', flush=True)

            try:
                # partB == tail-end manifest reading within and at end of the while not manifest_done loop
                if not self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
                    # NOTE:MANIFEST_READER=part3a-ALT-CSV:
                    curr_manifest_row = self.locutus_settings.get_next_nontrivial_row_via_manifest_CSV()
                else:
                    # NOTE:MANIFEST_READER=part3a-ALT-DB:
                    curr_manifest_row = self.locutus_settings.get_next_acc_via_batch_cursor()
                #print('{0}.Process(): partB tail of while not manifest_done parsing input manifest body line of "{1}"'.format(CLASS_PRINTNAME, curr_manifest_row), flush=True)
            except StopIteration as e:
                manifest_done = True
        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS and self.locutus_settings.LOCUTUS_VERBOSE:
            print('List of accession numbers provided : ')
            print(accession_list)
            print('====================================================', flush=True)


        ####################################################################
        # Summary statistics:

        # NOTE: and for the summary statistics at the end, go ahead and drop in a new commented header,
        # with enough empty columns such that the MANIFEST_HEADER_AS_OF_DATETIME aligns under ONPREM_MANIFEST_HEADER_MANIFEST_VER:
        empty_cols_before_datetime = ''
        if (self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper() == src_modules.settings.DICOM_MODULE_ONPREM):
            # ONPREM's needs 2x more commas to align to the 6th column for ONPREM_MANIFEST_HEADER_MANIFEST_VER.
            empty_cols_before_datetime = ',,'

        # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
        # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
        print('{0},###############'.format(
                    MANIFEST_OUTPUT_PREFIX), flush=True)

        #####
        # NOTE: redundantly adding the WORKSPACE and BATCH info down here at the top of the STATUS_OUT summary(as well as up top)

        # REDUNDANTLY at bottom: active Locutus module to Summarize:
        #print('{0},# MODULE:,{1}'.format(
        #            MANIFEST_OUTPUT_PREFIX,
        #            self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper()),
        #            flush=True)
        print('{0},{1},MODULE:{2}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE.upper()),
                    flush=True)

        # REDUNDANTLY at bottom: active Workspace to Summarize:
        #print('{0},# WORKSPACES_ENABLED:,{1}'.format(
        #            MANIFEST_OUTPUT_PREFIX,
        #            self.locutus_settings.LOCUTUS_WORKSPACES_ENABLE),
        #            flush=True)
        print('{0},{1},WORKSPACES_ENABLED:{2}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    self.locutus_settings.LOCUTUS_WORKSPACES_ENABLE),
                    flush=True)

        # REDUNDANTLY at bottom:  active Workspace to Summarize:
        #print('{0},# WORKSPACE_NAME:,{1}'.format(
        #            MANIFEST_OUTPUT_PREFIX,
        #            self.locutus_settings.LOCUTUS_WORKSPACE_NAME),
        #            flush=True)
        print('{0},{1},WORKSPACE_NAME:{2}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    self.locutus_settings.LOCUTUS_WORKSPACE_NAME),
                    flush=True)

        #######################################
        # NOTE: add tally of workspace COUNT(DISTINCT(accession_num)),
        # total counts in the current Workspace, to help debug unexpected results from the Summarizer:
        #####
        #print('{0},# TOTAL_WORKSPACE_ACCESSIONS:,{1}'.format(
        #            MANIFEST_OUTPUT_PREFIX,
        #            total_distinct_accessions_in_workspace),
        #            flush=True)
        #######################################

        #######################################
        # MANIFEST-ONCE:
        # REDUNDANTLY at bottom: bypass Manifest CSV to instead Batch from DB into Summarize:
        #WAS: print('{0},# BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:,{1}'.format(
        #####
        #print('{0},# LOAD_BATCH_FROM_DB_BYPASS_MANIFEST_CSV:,{1}'.format(
        #            MANIFEST_OUTPUT_PREFIX,
        #            self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB),
        #            flush=True)
        print('{0},{1},LOAD_BATCH_FROM_DB_BYPASS_MANIFEST_CSV:{2}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB),
                    flush=True)

        # WAS: if self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
        # r3m0: DEBUG: TEST FORCE IT ANYHOW, since it seems that the BATCH_NAME is still used for the Summarizer even if NOT BYPASSED.
        # So... Q: **if** NOT using the Preloader and NOT BYPASS_MANIFEST,
        #           should BATCH_NAME be RESET back to the DEFAULT?
        # OR..... at least WARN of it and potential confusion? OR..... what?????
        if True:
            # REDUNDANTLY at bottom: current Batch name to Summarize:
            #print('{0},# BATCH_NAME:,{1}'.format(
            #            MANIFEST_OUTPUT_PREFIX,
            #            self.locutus_settings.LOCUTUS_BATCH_NAME),
            #            flush=True)
            print('{0},{1},BATCH_NAME:{2}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    self.locutus_settings.LOCUTUS_BATCH_NAME),
                    flush=True)

            # NOTE: moved counts down to below the STATUS_OUT headers:
            # total counts in the BATCH of the current Workspace, to help debug unexpected results from the Summarizer:
            #print('{0},# {1}:,{2}'.format(
            #            MANIFEST_OUTPUT_PREFIX,
            #            MANIFEST_HEADER_ACCESSIONS_TOTAL_BATCH,
            #            total_distinct_accessions_in_batch),
            #            flush=True)
            ########
            # total counts in the BATCH of the current Workspace, to help debug unexpected results from the Summarizer:
            # NOTE: total_distinct_accessions_in_batch = self.locutus_settings.batch_cursor_MAX_counter_unfiltered
            # NOTE: down here in the Summary, only showing this total FILTERED, and showing that below.
            ########
            #print('{0},{1},# {2}:,{3}'.format(
            #        MANIFEST_OUTPUT_PREFIX,
            #        MANIFEST_HEADER_STATS_OUT,
            #        MANIFEST_HEADER_ACCESSIONS_TOTAL_BATCH,
            #        total_distinct_accessions_in_batch),
            #        flush=True)
            ########
            #print('{0},{1},# {2}:,{3}'.format(
            #        MANIFEST_OUTPUT_PREFIX,
            #        MANIFEST_HEADER_STATS_OUT,
            #        MANIFEST_HEADER_ACCESSIONS_COUNTED_FILTERED_BATCH,
            #        self.locutus_settings.batch_cursor_MAX_counter),
            #        flush=True)
            ########

            # NOTE: this could very well now be the FILTERED version:
            if self.locutus_settings.batch_cursor_clause_filter_only \
            or self.locutus_settings.batch_filter_counter_from \
            or self.locutus_settings.batch_filter_counter_to:

                # NEXT UP, the BATCH FILTER stuff, but only if set, as above:
                print('{0},{1},{2}:{3}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        MANIFEST_HEADER_STATS_OUT,
                        MANIFEST_HEADER_BATCH_FILTER,
                        self.locutus_settings.batch_cursor_clause_filter_only),
                        flush=True)
                print('{0},{1},{2}:{3}:{4}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        MANIFEST_HEADER_STATS_OUT,
                        MANIFEST_HEADER_BATCH_COUNTER_RANGE,
                        self.locutus_settings.batch_filter_counter_from,
                        self.locutus_settings.batch_filter_counter_to),
                        flush=True)


        #######################################
        #####

        # CORE START: following the above headers, preface the below summary results with a comment line, to highlight completeness:
        print('{0},###############'.format(
                    MANIFEST_OUTPUT_PREFIX), flush=True)

        # NOTE: not adding column after {4} for empty_cols_before_datetime, in case none are needed at all:
        print('{0},{1},{2},{3},{4}{5} (UTC)'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    MANIFEST_HEADER_COUNT,
                    MANIFEST_HEADER_STATUS,
                    empty_cols_before_datetime,
                    '{0}={1}'.format(MANIFEST_HEADER_AS_OF_DATETIME,nowtime)),
                    flush=True)

        #Step 1 - Find and print accession numbers not in manifest table
        # NOTE: leaving these here even if not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process(): Finding and printing accession_numbers not found in table `{1}`'.format(
                                CLASS_PRINTNAME,
                                self.manifest_table), flush=True)


        accession_list_str = '()'
        alpha_accession_list_str = ''
        alpha_accession_list_str_WHERE = 'is NULL'
        # NOTE: pre-handle the tuple(), since it will produce a SQL error if len==1,
        # due to an extraneous comma, since tuple(['one']) == '(1,)'
        # BUT, other cases, even len==0, are okay, as tuple([]) == '()'
        # NOPE, NOT TRUE, the len==0 will crash a "WHERE IN ()", soooo.
        if len(accession_list) == 0:
            # WARNING: don't want to fall into this one anymore if len()==0!
            print('**DEBUG**: WARNING: empty accession list; carrying on...', flush=True)
        else:
            # len(accession_list) > 0, yay:
            if len(accession_list) == 1:
                accession_list_str = '({0})'.format(accession_list[0])
                # NOTE: double escaping to make it through to the SQL WHERE clause:
                #alpha_accession_list_str = '\\\'{0}\\\''.format(accession_list[0])
                # NOTE: single escaping to fit in the SQL WHERE clause:
                #alpha_accession_list_str = '\'{0}\''.format(accession_list[0])
                alpha_accession_list_str_WHERE = '=\'{0}\' '.format(accession_list[0])
            elif len(accession_list) > 1:
                accession_list_str = tuple(accession_list)
                for (idx,acc) in enumerate(accession_list):
                    if idx > 0:
                        alpha_accession_list_str += ','
                    # NOTE: double escaping to make it through to the SQL WHERE clause:
                    #alpha_accession_list_str += '\\\'{0}\\\''.format(acc)
                    # NOTE: single escaping to fit in the SQL WHERE clause:
                    alpha_accession_list_str += '\'{0}\''.format(acc)
                alpha_accession_list_str_WHERE = 'in ({0}) '.format(alpha_accession_list_str)
            #alpha_accession_list_str = f"""('{"', '".join(accession_list)}')"""

            # NOTE: BEWARE that the following USED TO(?) FAIL on an EMPTY Preloader set of Accession!
            # DONE: made it more robust to an empty manifest by nesting w/in else, len(accession_list) > 0, yay!
            # BATCHES NOTE: filtering by batch_clause for the following accession query of manifest_status:
            manifest_check_accession_result = self.LocutusDBconnSession.execute('SELECT accession_num '\
                                    'FROM {0} '\
                                    'WHERE accession_num {1} '\
                                    'AND active {2} ;'.format(
                                    self.manifest_table,
                                    alpha_accession_list_str_WHERE,
                                    batch_clause))
            for row in manifest_check_accession_result:
                ####################
                # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
                ####################
                output_accession_list.append(row[0])
                ####################

        #placeholder for actual nicely written print statement
        empty_set = set()
        diff_set = set(accession_list) - set(output_accession_list)
        ###
        # 12/20/2024: try w/o this to see if the ACCESSIONS NOT FOUND row looks cleaner in the # STATUS_OUT without:
        #if diff_set == empty_set:
        #    diff_set = "n/a"
        ####
        # but delay printing out the ACCESSIONS NOT FOUND for diff_set until after their total_not_found counts, below....


        #Step 2 - get summary stats

        # 0th, a count of total accessions in the manifest:
        total_manifest_or_batch_header = MANIFEST_HEADER_ACCESSIONS_TOTAL_MANIFEST
        if self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
            total_manifest_or_batch_header = MANIFEST_HEADER_ACCESSIONS_TOTAL_BATCH
            # NOTE: Yes AND.... this could very well now be the FILTERED version:
            if self.locutus_settings.batch_cursor_clause_filter_only \
                or self.locutus_settings.batch_filter_counter_from \
                or self.locutus_settings.batch_filter_counter_to:
                total_manifest_or_batch_header = MANIFEST_HEADER_ACCESSIONS_COUNTED_FILTERED_BATCH

            # NOTE: perhaps print out the following regardless if the above if:
            # total counts in the BATCH of the current Workspace, to help debug unexpected results from the Summarizer:
            # NOTE: total_distinct_accessions_in_batch = self.locutus_settings.batch_cursor_MAX_counter_unfiltered
            # NOTE: down here in the Summary, only showing this total FILTERED, and showing that below.
            print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_distinct_accessions_in_batch,
                    MANIFEST_HEADER_ACCESSIONS_TOTAL_BATCH),
                    flush=True)

            print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    self.locutus_settings.batch_cursor_MAX_counter,
                    MANIFEST_HEADER_ACCESSIONS_FILTERED_BATCH),
                    flush=True)

        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_comparisons_checked,
                    total_manifest_or_batch_header),
                    flush=True)

        # NOTE: Keeping calm and carrying on w/ ACCESSIONS WITH TEXT, courtesy of the Juneteenth 2025 alpha-numeric accessions Upgrade.
        # As such, no more need to even show (but still doing so for information & backwards compatibility)
        alphas_removed_msg = ''
        if total_accessions_with_text_NOT_removed > 0:
            alphas_removed_msg = '[text no longer removed thanks to alpha-numeric upgrade]'
        print('{0},{1},{2},{3} {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_accessions_with_text_NOT_removed,
                    'ACCESSIONS WITH TEXT',
                    alphas_removed_msg),
                    flush=True)

        # first, a count of total duplicates encountering (noting that the first one is NOT included in this count)
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_duplicates,
                    MANIFEST_HEADER_ACCESSION_DUPLICATES),
                    flush=True)

        # and, count of the internal self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND, to lead the below NOT_FOUND missing list
        notfound_msg = ''
        if total_not_found > 0:
            notfound_msg = '(NOTE: consider running the Summarizer\'s Preloader to move these to PENDING_CHANGE)'
        print('{0},{1},{2},{3} {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_not_found,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_NOT_FOUND,
                    notfound_msg),
                    flush=True)
        # and...
        # ANOTHER count of total duplicates encountered, those as NOTFOUND (noting that the first one is NOT included in this count)
        duplicate_counts_msg = ''
        if total_not_found_duplicates > 0:
            duplicate_counts_msg = self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_OF_NOTFOUND_WARNING_VS_DYNAMIC_COUNTS
            print('{0},{1},{2},{3} {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_not_found_duplicates,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_NOTFOUND,
                    duplicate_counts_msg),
                    flush=True)

        # finally print out the ACCESSIONS NOT FOUND for diff_set after their total_not_found counts, above....
        # NOTE: leaving these here even if not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS:
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    'ACCESSIONS NOT FOUND:',
                    sorted(diff_set)),
                    flush=True)

        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST:
            # a preface to total_preloaded_accessions
            # if not LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES
            dryrun_prefix =''
            if not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                dryrun_prefix ='DRY-RUN SIMULATED '

            print('{0},{1},---------------,Pre-loaded Accessions:'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT),
                    flush=True)
            print('{0},{1},{2},{3}Accessions Pre-loaded for pre-processing, as below:'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_preloaded_accessions,
                    dryrun_prefix),
                    flush=True)

            # NOTE: all of the ZZZ-ONDECK prefaced counts for Preloader,
            # but only for those with actual counts:
            #
            # ONDECK_PENDING_CHANGE_PREFIX
            # ONDECK_PROCESSING_CHANGE_PREFIX
            # ONDECK_REPROCESSING_CHANGE_PREFIX
            # ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX
            # ONDECK_OTHER_PREFIX_PREFIX

            if total_preloaded_accessions_pending:
                print('{0},{1},{2},{3}{4}[*]'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_preloaded_accessions_pending,
                    dryrun_prefix,
                    self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX),
                    flush=True)
            if total_preloaded_accessions_processing:
                print('{0},{1},{2},{3}{4}[*]'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_preloaded_accessions_processing,
                    dryrun_prefix,
                    self.locutus_settings.ONDECK_PROCESSING_CHANGE_PREFIX),
                    flush=True)
            if total_preloaded_accessions_reprocessing:
                print('{0},{1},{2},{3}{4}[*]'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_preloaded_accessions_reprocessing,
                    dryrun_prefix,
                    self.locutus_settings.ONDECK_REPROCESSING_CHANGE_PREFIX),
                    flush=True)
            if total_preloaded_accessions_multiples:
                print('{0},{1},{2},{3}{4}[*]'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_preloaded_accessions_multiples,
                    dryrun_prefix,
                    self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX),
                    flush=True)
            if total_preloaded_accessions_other:
                print('{0},{1},{2},{3}{4}[*] {5}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_preloaded_accessions_other,
                    dryrun_prefix,
                    self.locutus_settings.ONDECK_OTHER_PREFIX,
                    '; '.join(other_ondecks_found)),
                    flush=True)

        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS:
            # a preface to total_preset_accessions
            # if not LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES
            dryrun_prefix =''
            if not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                dryrun_prefix ='DRY-RUN SIMULATED '

            print('{0},{1},---------------,Preset Accessions:'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT),
                    flush=True)
            print('{0},{1},{2},{3}Accessions Preset for re-processing'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_preset_accessions,
                    dryrun_prefix),
                    flush=True)

        if self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS:
            # a preface to each of the resolved_successfully, etc.
            # if not LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES
            dryrun_prefix =''
            if not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES:
                dryrun_prefix ='DRY-RUN SIMULATED '

            # and for multi-uuid resolutions:
            # * total_resolved_successfully,
            # * total_resolved_recently,
            # * total_resolved_formerly
            # * errors_resolving
            # .... w/ each of the above ^^^ expanded out to the following resolve cmds:
            # * MERGE_RADIOLOGY
            # * CONSOLIDATE_LOCALLY
            # * CHOOSE_LOCALLY
            # * DELETE_LOCALLY
            # * DELETE_STAGE_ONLY
            # TODO: definitely explore a better (ahem, actual) data structure for these.
            # AND: consider only showing those with non-zero values for a more condensed view?
            # Perhaps leave all of the first ones to highlight the subcmd options,
            # but then condense the remainder:
            print('{0},{1},---------------,multi-uuid successful resolutions:'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT),
                    flush=True)
            print('{0},{1},{2},{3}resolved_successfully via MERGE_RADIOLOGY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_successfully_viaRADIOLOGY_MERGE,
                    dryrun_prefix),
                    flush=True)
            print('{0},{1},{2},{3}resolved_successfully via RADIOLOGY_RESEND'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_successfully_viaRADIOLOGY_RESEND,
                    dryrun_prefix),
                    flush=True)
            print('{0},{1},{2},{3}resolved_successfully via CONSOLIDATE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_successfully_viaCONSOLIDATE,
                    dryrun_prefix),
                    flush=True)
            print('{0},{1},{2},{3}resolved_successfully via CHOOSE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_successfully_viaCHOICE,
                    dryrun_prefix),
                    flush=True)
            print('{0},{1},{2},{3}resolved_successfully via DELETE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_successfully_viaDELETE_LOCAL,
                    dryrun_prefix),
                    flush=True)
            print('{0},{1},{2},{3}resolved_successfully via DELETE_FROM_STAGE_ONLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_successfully_viaDELETE_STAGE,
                    dryrun_prefix),
                    flush=True)

            print('{0},{1},---------------,multi-uuid recent resolutions (such as other sub-accessions following the above):'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT),
                    flush=True)
            if total_resolved_recently_viaRADIOLOGY_MERGE:
                print('{0},{1},{2},{3}resolved_recently via MERGE_RADIOLOGY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_formerly_viaRADIOLOGY_MERGE,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_recently_viaRADIOLOGY_RESEND:
                print('{0},{1},{2},{3}resolved_recently via RADIOLOGY_RESEND'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_recently_viaRADIOLOGY_RESEND,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_recently_viaCONSOLIDATE:
                print('{0},{1},{2},{3}resolved_recently via CONSOLIDATE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_recently_viaCONSOLIDATE,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_recently_viaCHOICE:
                print('{0},{1},{2},{3}resolved_recently via CHOOSE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_recently_viaCHOICE,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_recently_viaDELETE_LOCAL:
                print('{0},{1},{2},{3}resolved_recently via DELETE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_recently_viaDELETE_LOCAL,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_recently_viaDELETE_STAGE:
                print('{0},{1},{2},{3}resolved_recently via DELETE_FROM_STAGE_ONLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_recently_viaDELETE_STAGE,
                    dryrun_prefix),
                    flush=True)

            print('{0},{1},---------------,multi-uuid former resolutions:'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT),
                    flush=True)
            if total_resolved_formerly_viaRADIOLOGY_MERGE:
                print('{0},{1},{2},{3}resolved_formerly via MERGE_RADIOLOGY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_formerly_viaRADIOLOGY_MERGE,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_formerly_viaRADIOLOGY_RESEND:
                print('{0},{1},{2},{3}resolved_formerly via RADIOLOGY_RESEND'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_formerly_viaRADIOLOGY_RESEND,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_formerly_viaCONSOLIDATE:
                print('{0},{1},{2},{3}resolved_formerly via CONSOLIDATE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_formerly_viaCONSOLIDATE,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_formerly_viaCHOICE:
                print('{0},{1},{2},{3}resolved_formerly via CHOOSE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_formerly_viaCHOICE,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_formerly_viaDELETE_LOCAL:
                print('{0},{1},{2},{3}resolved_formerly via DELETE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_formerly_viaDELETE_LOCAL,
                    dryrun_prefix),
                    flush=True)
            if total_resolved_formerly_viaDELETE_STAGE:
                print('{0},{1},{2},{3}resolved_formerly via DELETE_FROM_STAGE_ONLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_resolved_formerly_viaDELETE_STAGE,
                    dryrun_prefix),
                    flush=True)

            print('{0},{1},---------------,multi-uuid errored resolutions:'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT),
                    flush=True)
            if errors_resolving_viaRADIOLOGY_MERGE:
                print('{0},{1},{2},{3}errors_resolving via MERGE_RADIOLOGY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    errors_resolving_viaRADIOLOGY_MERGE,
                    dryrun_prefix),
                    flush=True)
            if errors_resolving_viaRADIOLOGY_RESEND:
                print('{0},{1},{2},{3}errors_resolving via RADIOLOGY_RESEND'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    errors_resolving_viaRADIOLOGY_RESEND,
                    dryrun_prefix),
                    flush=True)
            if errors_resolving_viaCONSOLIDATE:
                print('{0},{1},{2},{3}errors_resolving via CONSOLIDATE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    errors_resolving_viaCONSOLIDATE,
                    dryrun_prefix),
                    flush=True)
            if errors_resolving_viaCHOICE:
                print('{0},{1},{2},{3}errors_resolving via CHOOSE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    errors_resolving_viaCHOICE,
                    dryrun_prefix),
                    flush=True)
            if errors_resolving_viaDELETE_LOCAL:
                print('{0},{1},{2},{3}errors_resolving via DELETE_LOCALLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    errors_resolving_viaDELETE_LOCAL,
                    dryrun_prefix),
                    flush=True)
            if errors_resolving_viaDELETE_STAGE:
                print('{0},{1},{2},{3}errors_resolving via DELETE_FROM_STAGE_ONLY'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    errors_resolving_viaDELETE_STAGE,
                    dryrun_prefix),
                    flush=True)

            if errors_resolving_viaOTHER:
                print('{0},{1},{2},{3}errors_resolving via Other (e.g., unknown resolution sub-command)'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    errors_resolving_viaOTHER,
                    dryrun_prefix),
                    flush=True)

        # and provide counts of the known status values:
        # Header above all automatically grouped:
        print('{0},{1},---------------,Known Status:'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT),
                    flush=True)

        # ACCESSION_HAS_MULTIPLE_SPLITS
        # == self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX:
        print('{0},{1},{2},{3}[*]'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_multiple_splits,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX),
                    flush=True)


        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING:
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_pending,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING),
                    flush=True)
        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_MERGE:
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_pending_radiology_merge,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_MERGE),
                    flush=True)
        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_RESEND:
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_pending_radiology_resend,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_RESEND),
                    flush=True)
        # and... aggregating general duplicates from each of the above PENDINGs,
        # ANOTHER count of total duplicates encountered, those as PENDING (noting that the first one is NOT included in this count)
        duplicate_counts_msg = ''
        if total_status_pending_duplicates > 0:
            duplicate_counts_msg = self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_OF_PENDING_WARNING_VS_DYNAMIC_COUNTS
            print('{0},{1},{2},{3} {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_pending_duplicates,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_PENDING,
                    duplicate_counts_msg),
                    flush=True)

        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX:
        previous_counts_msg = ''
        if total_status_previous_processsing > 0:
            previous_counts_msg = self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_WARNING_VS_DYNAMIC_COUNTS
            print('{0},{1},{2},{3}[*] {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_previous_processsing,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX,
                    previous_counts_msg),
                    flush=True)
            # and...
            # ANOTHER count of total duplicates encountered, those as PREVIOUS_PROCESSING (noting that the first one is NOT included in this count)
            duplicate_counts_msg = ''
            if total_status_previous_processsing_duplicates > 0:
                duplicate_counts_msg = self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_OF_PREVIOUS_WARNING_VS_DYNAMIC_COUNTS
                print('{0},{1},{2},{3} {4}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        MANIFEST_HEADER_STATS_OUT,
                        total_status_previous_processsing_duplicates,
                        self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_PREVIOUS,
                        duplicate_counts_msg),
                        flush=True)

        # PROCESSED.  leave it simple, no message.
        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED:
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_processsed,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED),
                    flush=True)
        # and...
        # ANOTHER count of total duplicates encountered, those as PROCESSED (noting that the first one is NOT included in this count)
        duplicate_counts_msg = ''
        if total_status_processsed_duplicates > 0:
            duplicate_counts_msg = self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_OF_PROCESSED_WARNING_VS_DYNAMIC_COUNTS
            print('{0},{1},{2},{3} {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_processsed_duplicates,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_PROCESSED,
                    duplicate_counts_msg),
                    flush=True)

        """
        # NOTE: no need to show this estimate when the actual whole count follows,
        # it was a great first pass guesstimate, but was already found to differ (e.g., 403 instead of 404)
        #
        # DERIVED self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED_WHOLEACCESSIONS_EST:
        # as estimated by removing from the TOTAL the difference between TOTAL_SUBACESSIONS and TOTAL_MULTIPLE_SPLITS:
        estimate_total_whole_accs_processed = total_status_processsed
        if total_status_processsed_subaccessions > total_status_multiple_splits:
            estimate_total_whole_accs_processed -= (total_status_processsed_subaccessions - total_status_multiple_splits)
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    estimate_total_whole_accs_processed,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED_WHOLEACCESSIONS_EST),
                    flush=True)
        """
        # NOTE: Juneteenth 2025 upgrade: also deprecating SPLIT multi-UUIDs,
        # so go ahead and omit the following split-related stats,
        # to help reduce any unnecessary confusion:
        """
        total_whole_accs_processed = len(processed_ints_accession_list)
        # and the "actual" self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED_WHOLEACCESSIONS:
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_whole_accs_processed,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED_WHOLEACCESSIONS),
                    flush=True)

        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED_SUBACCESSIONS:
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_processsed_subaccessions,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSED_SUBACCESSIONS),
                    flush=True)
        """

        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSING:
        print('{0},{1},{2},{3}[*]'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_processing,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSING_PREFIX),
                    flush=True)
        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSING_MOMENTARILY:
        print('{0},{1},{2},{3}[*]'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_processing_momentarily,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PROCESSING_MOMENTARILY_PREFIX),
                    flush=True)

        # NOTE: all of the ZZZ-ONDECK prefaced counts are also here,
        # for any previously Preloaded:
        #
        # ONDECK_PENDING_CHANGE_PREFIX
        # ONDECK_PROCESSING_CHANGE_PREFIX
        # ONDECK_REPROCESSING_CHANGE_PREFIX
        # ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX
        # ONDECK_OTHER_PREFIX_PREFIX
        #
        # though shown *without* the DR-RUN SIMULATED prefix,
        # and regardless of the count,
        # for a full list of all possible:

        #if total_preloaded_accessions_pending:
        print('{0},{1},{2},{3}[*]'.format(
                MANIFEST_OUTPUT_PREFIX,
                MANIFEST_HEADER_STATS_OUT,
                total_preloaded_accessions_pending,
                self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX),
                flush=True)
        #if total_preloaded_accessions_processing:
        print('{0},{1},{2},{3}[*]'.format(
                MANIFEST_OUTPUT_PREFIX,
                MANIFEST_HEADER_STATS_OUT,
                total_preloaded_accessions_processing,
                self.locutus_settings.ONDECK_PROCESSING_CHANGE_PREFIX),
                flush=True)
        #if total_preloaded_accessions_reprocessing:
        print('{0},{1},{2},{3}[*]'.format(
                MANIFEST_OUTPUT_PREFIX,
                MANIFEST_HEADER_STATS_OUT,
                total_preloaded_accessions_reprocessing,
                self.locutus_settings.ONDECK_REPROCESSING_CHANGE_PREFIX),
                flush=True)
        #if total_preloaded_accessions_multiples:
        print('{0},{1},{2},{3}[*]'.format(
                MANIFEST_OUTPUT_PREFIX,
                MANIFEST_HEADER_STATS_OUT,
                total_preloaded_accessions_multiples,
                self.locutus_settings.ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX),
                flush=True)
        #if total_preloaded_accessions_other:
        print('{0},{1},{2},{3}[*] (other ONDECK): {4}'.format(
                MANIFEST_OUTPUT_PREFIX,
                MANIFEST_HEADER_STATS_OUT,
                total_preloaded_accessions_other,
                self.locutus_settings.ONDECK_OTHER_PREFIX,
                '; '.join(other_ondecks_found)),
                flush=True)
        # and... aggregating general duplicates from each of the above PRELOADEDs,
        # ANOTHER count of total duplicates encountered, those ONDECK as PRELOADED (noting that the first one is NOT included in this count)
        duplicate_counts_msg = ''
        if total_preloaded_accessions_duplicates > 0:
            duplicate_counts_msg = self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_OF_ONDECK_WARNING_VS_DYNAMIC_COUNTS
            print('{0},{1},{2},{3} {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_preloaded_accessions_duplicates,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_ONDECK,
                    duplicate_counts_msg),
                    flush=True)

        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX:
        print('{0},{1},{2},{3}[*]'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_multiuuids,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX),
                    flush=True)
        # and...
        # ANOTHER count of total duplicates encountered, those as MULTIUUIDS (noting that the first one is NOT included in this count)
        duplicate_counts_msg = ''
        if total_status_multiuuids_duplicates > 0:
            duplicate_counts_msg = self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_OF_MULTIUUIDS_WARNING_VS_DYNAMIC_COUNTS
            print('{0},{1},{2},{3} {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_multiuuids_duplicates,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_MULTIUUIDS,
                    duplicate_counts_msg),
                    flush=True)

        # self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_OTHER_PREFIX:
        other_errors_message = 'see individual accession rows for particular errors'
        if not self.locutus_settings.LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS:
            # only show the full other errors list for concise stats view, if not already shown above in accessions
            other_errors_message = '; '.join(other_errors_found)
        print('{0},{1},{2},{3}[*] (other errors): {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_error_other,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_OTHER_PREFIX,
                    other_errors_message),
                    flush=True)
        # and...
        # ANOTHER count of total duplicates encountered, those as OTHER ERRORS (noting that the first one is NOT included in this count)
        duplicate_counts_msg = ''
        if total_status_error_other_duplicates > 0:
            duplicate_counts_msg = self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_OF_ERROR_OTHERS_WARNING_VS_DYNAMIC_COUNTS
            print('{0},{1},{2},{3} {4}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_error_other_duplicates,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_DUPLICATES_ERROR_OTHERS,
                    duplicate_counts_msg),
                    flush=True)

        # and.... any OTHER statuses:
        print('{0},{1},{2},{3}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    total_status_other,
                    "other: "+'; '.join(other_statuses_found)),
                    flush=True)

        # select distinct(manifest_status), count(*) as subtotals from onprem_dicom_manifest group by manifest_status;
        # Header above all automatically grouped:
        print('{0},{1},---------------,Dynamically Grouped (to 100 chars) from the DB (which might differ from any {2} or DRYRUN/SAFEMODE mock updates):'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_STATS_OUT,
                    self.locutus_settings.MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX),
                    flush=True)

        # BATCHES NOTE: the following Dynamically Grouped SQL now includes a BATCH clause... DONE! :-D
        manifest_summ_status_result = self.LocutusDBconnSession.execute('SELECT substring(manifest_status from 0 for 100) as trunc_manifest_status, '\
                        'COUNT(*) as subtotals '\
                        'FROM {0} '\
                        'WHERE accession_num {1} '\
                        'AND active {2} '\
                        'GROUP BY trunc_manifest_status '\
                        'ORDER by trunc_manifest_status ;'.format(
                        self.manifest_table,
                        alpha_accession_list_str_WHERE,
                        batch_clause))

        for row in manifest_summ_status_result:
            print('{0},{1},{2},{3}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        MANIFEST_HEADER_STATS_OUT,
                        str(row[1]),
                        str(row[0])
                        ), flush=True)

        # CORE END: wrap up the above summary results with a comment line, to highlight completeness:
        print('{0},###############'.format(
                    MANIFEST_OUTPUT_PREFIX), flush=True)

        if processed_uuids_in_this_phase:
            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE batch'.format(nowtime), flush=True)
            print('{0}.Process(): END of PHASE: phase processing complete for this batch.'.format(CLASS_PRINTNAME), flush=True)
            print('subtotal_comparisons_not_staged = {0}'.format(subtotal_comparisons_not_staged), flush=True)
            print('subtotal_comparisons_found_staged = {0}'.format(subtotal_comparisons_found_staged), flush=True)
            print('total_comparisons_checked = {0}'.format(total_comparisons_checked), flush=True)
        else:
            print('Nothing checked.', flush=True)

        # NOTE:MANIFEST_READER=part0ab-ALT-CSV:
        if not self.locutus_settings.LOCUTUS_BYPASS_MANIFEST_LOAD_BATCH_FROM_DB:
            self.locutus_settings.close_manifest_CSV()
        else:
            self.locutus_settings.close_batch_cursor()

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process() says Goodbye'.format(CLASS_PRINTNAME), flush=True)
        return total_errors_encountered
        # end of Process()
