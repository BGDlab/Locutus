#!/usr/bin/env python3
# module_onprem_dicom.py:
#
# Encapsulation of applicable ONPREM-Dicom-Stage elements for Locutus to process
# in a DICOM-Manifest-driven fashion.
#
# Looking for all available uuids at each and every Phase of the process (i.e.,
# just Phase by Phase rather than running each uuid through the entire process,
# thereby more robustly catching any uuids which may have previously failed),
# but also taking each uuid on through the remaining Phases, to minimize the
# local disk space needed during processing of large batches.
#

# for target GCP GS, but must appear at top:
from __future__ import absolute_import

# for debugging with pdb.set_trace() as needed:
import pdb
import traceback


import csv
from decimal import Decimal
import os, errno
import shutil
import socket
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from subprocess import Popen, PIPE
import yaml
from zipfile import ZipFile
import src_3rdParty.OrthancRestToolbox as RestToolbox
from trigsecrets import TrigSecrets

# r3m0: NOTE: disabling AWS/boto until needed again, pending resolution of:
#   https://github.research.chop.edu/dbhi/Locutus/issues/412
#   "upgrade aws cli to v2 (from pip to curl&unzip install)"
# for target AWS s3:
#import boto3

# for target GCP GS:
from google.cloud import storage as GS_storage
import datetime
# and for the general settings in Locutus's settings.py:
import src_modules.settings
# for copy_local_directory_to_Orthanc():
import glob
import base64
import httplib2
import json
import requests
# NOT? import mimetypes
import magic

# file path delimeter:
PATH_DELIM = '/'
# ZIP file suffix, to best determine its destination dir sans suffix:
ZIP_SUFFIX = '.zip'
# infinte loop safety net
MAX_SAME_ACCESSION_ATTEMPTS = 5
# duplicates aside, this MAX_SAME is to help ensure that we don't get stuck
# in the while (accession in manifest) sort of loop
# attempting to do the same accession over and over,
# only to have that accession bailed on due to text in it,
# but an unenabled LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS
# that wasn't properly bailed, and didn't read the next line in the manifest, d'oh!

# NOTE: MIN_PROCESSING_PHASE = 2 is the initial phase before even downloaded,
# and completion of Phase03 would indicate partial processing progress:
#
MIN_PROCESSING_PHASE = 2
# SOON: MAX_PROCESSING_PHASE = self.locutus_settings.DICOM_ONPREM_MIN_PROCESSING_PHASE
# NOTE: but no sense of self yet here
# TODO: consider updating all references within to use self.locutus_settings.DICOM_ONPREM_MIN_PROCESSING_PHASE directly,
#
# NOTE: PROCESSING_PHASE = 3 is the download phase,
# NOTE: PROCESSING_PHASE = 4 is the de-identification phase,
DEIDENTIFIED_PHASE = 4
# NOTE: PROCESSING_PHASE = 5 is the upload to de-identified target phase,
# NOTE: MAX_PROCESSING_PHASE = 5 indicates completion of all the phases:
#
MAX_PROCESSING_PHASE = 5
# SOON: MAX_PROCESSING_PHASE = self.locutus_settings.DICOM_ONPREM_MAX_PROCESSING_PHASE
# NOTE: but no sense of self yet here
# TODO: consider updating all references within to use self.locutus_settings.DICOM_ONPREM_MIN_PROCESSING_PHASE directly,
#
FAILED_PROCESSING_PHASE = 999

UNDER_REVIEW_STATUS = 'UNDER_REVIEW:DEID_QC'
# NOTE: Implementing both a PASS:* (to reprocess without a DEIDQC_subject_ID_preface)
# and a PASS_FROM_DEIDQC:* (to pull directly from the ORTHANCDEIDQC, even if with such a preface)
UNDER_REVIEW_QC_OPTIONS = 'please choose from: PASS:info; PASS_FROM_DEIDQC:info; FAIL:info; or REPROCESS:info'


AWS_S3_TARGET_PREFIX = 's3://'
GOOGLE_STORAGE_TARGET_PREFIX = 'gs://'

# NOTE: these ONPREM_DICOM_* constants are also defined in the onprem-dicom-stage,
# but are left as redundantly defined rather than user-configurable, since they
# are internal aspects that really shouldn't change:
STAGER_REST_API_CFG_TABLE = 'dicom_rest_api_cfg'
STAGER_STABLESTUDY_TABLE = 'dicom_stablestudies'
STAGER_CHANGE_TYPE_STABLESTUDY = 'StableStudy'

# NOTE: the following are LOCUTUS-specific constants for this ONPREM_DICOM module:
DEF_LOCUTUS_ONPREM_DICOM_STATUS_TABLE = 'onprem_dicom_status'
DEF_LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE = 'onprem_dicom_manifest'
DEF_LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE = 'onprem_dicom_int_cfgs'
# w/ the above as initial default values to the following, to be used in the code:
LOCUTUS_ONPREM_DICOM_STATUS_TABLE = DEF_LOCUTUS_ONPREM_DICOM_STATUS_TABLE
LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE = DEF_LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE = DEF_LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE
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

# and the current internal configuration values for LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE:
# THIRD PASS IMPLEMENTATION:
#   now UPDATING at completion of Phase03, Phase04, OR Phase05,
#   and CHECKING at Phase03 (ONLY), to setup for it and any subsequent Phase04 Sweep and Phase05 Sweep.
#######
# TODO: consider if a new key for `recommend_reprocessing_if_changed` might be useful in each dictionary,
# OR if ALL are inherently recommended for reprocessing whenever changed (as is currently implemented).
# NOTE: see also the LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED setting
# in order to continue processing even when such configuration changes are encountered.
#######
# NOTE: when adding new internal config dictionaries into the following list of LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES,
#       and the LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE table already exists,
#       be sure to manually add any new configs via SQL such as:
#       SQL> INSERT INTO onprem_dicom_internal_configs (config_type, config_version, config_desc, date_activated, active, at_phase, status_field)
#           VALUES ('test_for_phase05', 'test_p5_ver001', 'testing phase05 version 01', now(), true, 5, 'test_for_phase05');
#
# NOTE: status_field is not defined in the above LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE record,
#       AND also manually add the corresponding status_field to the status table via SQL such as:
#       SQL> alter table onprem_dicom_status add column cfg_test_col text;
#
# NOTE: date_activated fields expected to be of the format 'YYYY-MM-DD' for comparison:

##############################
# NOTE: Download CFG for old default of flat DICOMDIR
########## ##########
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION_DICOMDIR = 'pre2025march24=DICOMDIR'
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC_DICOMDIR = 'download=DICOMDIR (flat IM### structure)'
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE_DICOMDIR = '2017-01-01'
# as current defaults:
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION_DICOMDIR
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC_DICOMDIR
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE_DICOMDIR
########## ##########
# NOTE: Download CFG for new OPTION (as of 3/25/2025, through LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE) with Series hierarchy:
# if LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE, Settings will update to the following LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION_ORIGINAL, et al:
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION_ZIPARCHIVE = '2025march25=ZipArchive'
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC_ZIPARCHIVE = 'download=ZipArchive (hierarchical Study/Series/*.dcm structure)'
LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE_ZIPARCHIVE = '2025-03-25'
##############################

##############################
# NOTE: INT CFG for the April 08 tweaks to DICOM_ANON itself....
#   ==..... ALIGNMENT_MODE.... Original -vs- GCP
########## ##########
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION_ORIGINAL = '2017-01-01=original'
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC_ORIGINAL = 'original mode w/ expanded AUDIT, CLEANED, & dates=19010101'
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE_ORIGINAL = '2017-01-01'
# as current defaults:
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION_ORIGINAL
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC_ORIGINAL
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE_ORIGINAL
########## ##########
# NOTE: Alignment Mode CFG new OPTION (as of 4/08/2025 through LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP) to align with the GCP module's De-ID Profile
# if LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP, Settings will update to the following LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION_GCP, et al:
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION_GCP = '2025-04-08=gcp'
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC_GCP = 'GCP-alignment mode w/ limited AUDIT, & blanks incl dates'
LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE_GCP = '2025-04-08'
##############################

########## ########## ########## ########## ##########
# any local OnPrem DEFAULT_DICOM_* configurations,
# for subsequent use in defining LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES:
########## ##########
# for new dicom_anon option to --exclude_series_descs (each processed as .strip().lower())
DICOM_SERIES_DESCS_TO_EXCLUDE = 'screen save, dose report, basic text SR'
DEFAULT_DICOM_SERIES2EXCLUDE_CFG_VER = '2018may01'
DEFAULT_DICOM_SERIES2EXCLUDE_CFG_DATE_ACTIVATED = '2018-05-01'
# NOTE: ^^^ above is the same as the current 22q_config.yaml, as of 24 June 2024:
# locutus_gcp_dicom_predeid_series_descs_to_exclude: 'screen save, dose report, basic text SR'
########## ##########
DEFAULT_DICOM_ANON_MODALITIES_STR = 'cr,ct,dx,mr,nm,ot,rf,us,xa,xr'
DEFAULT_DICOM_ANON_MODALITIES_CFG_VER = '2024june24a'
DEFAULT_DICOM_ANON_MODALITIES_CFG_DATE_ACTIVATED = '2024-06-24'
########## ##########
# DICOM_ANON_SPEC Rev00: Onprem module values via `dev_bgd_lab` branch as of 2024-06-04, with Contrast:
#DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/ext_keep_study_and_series_descs_replacePatientNameID_keepContrastAgents_andManufacturerInfoSoftwareFramesRowColBurnedinannotation.dat'
#DEFAULT_DICOM_ANON_MODALITIES_STR = 'mr,ct,cr,ot'
#####
# DICOM_ANON_SPEC Rev01: as from GCP module via `develop` branch, 24 June 2024:
# WARNING: ext_samsung.dat likely has no additional Contrast Bolus Agent metadata keeplisted;
# TODO? may need to merge in from abov ext_keep_study_and_series_descs_replacePatientNameID.date
#DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/ext_samsung.dat'
#DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/ext_samsung_asBGDwGCP_withContrastBolusAgent_asof2024july01.dat'
#DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/ext_samsung_asBGDwGCP_asof2024july03a.dat'
# NOTE: 7/08/24 WIP, testing the Overlays:
#DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/ext_samsung_asBGDwGCP_wOverlays_asof2024july08a.dat'
# NOTE: 7/10/24 WIP, with Overlay Data:
#DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/BGDwGCP_wOverlays_asof2024july10b3.dat'
# NOTE: 7/25/24 WIP, with additional BodyPartExamined, MagnetizationTransfer, and FrameType
# WAS: DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/BGDwGCP_wOverlays_BodyPartsExamined_andMagneticTransfers_asof2024july25a.dat'
# NOTE: reducing size of the corresponding INT CFG for cfg_dicom_anon_spec_ver, but splitting out into the DIR and FILENAMES:
#####
#DEFAULT_DICOM_ANON_SPEC_FILENAME = 'BGDwGCP_wOverlays_BodyPartsExamined_andMagneticTransfers_asof2024july25a.dat'
#DEFAULT_DICOM_ANON_SPEC_CFG_VER = '2024july25b'
#DEFAULT_DICOM_ANON_SPEC_CFG_DATE_ACTIVATED = '2024-07-25'
#####
DEFAULT_DICOM_ANON_SPEC_DIRNAME = './src_3rdParty/dicom_anon_spec_files'
#####
# 4/04/2025 == direct copy of the GCP spec.dat:
#WAS: DEFAULT_DICOM_ANON_SPEC_CFG_DATE_ACTIVATED = '2025-04-04'
#WAS: DEFAULT_DICOM_ANON_SPEC_CFG_VER = '2025april04a'
# DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/BGDwGCP_wOverlays_BodyPartsExamined_andMagneticTransfers_asof2024july25a.dat'
#####
# 4/30/25 == update for stand-alone:
#DEFAULT_DICOM_ANON_SPEC_CFG_DATE_ACTIVATED = '2025-04-30'
#DEFAULT_DICOM_ANON_SPEC_CFG_VER = '2025april30a'
#DEFAULT_DICOM_ANON_SPEC_FILENAME = 'BGDwGCP_asof' + DEFAULT_DICOM_ANON_SPEC_CFG_VER + '.dat'
#####
# 5/14/25 == finalize above updates for stand-alone w/ GCP-alignment:
DEFAULT_DICOM_ANON_SPEC_CFG_DATE_ACTIVATED = '2025-05-14'
DEFAULT_DICOM_ANON_SPEC_CFG_VER = '2025may14a'
DEFAULT_DICOM_ANON_SPEC_FILENAME = 'BGDwGCP_asof' + DEFAULT_DICOM_ANON_SPEC_CFG_VER + '.dat'
#####
DEFAULT_DICOM_ANON_SPEC_FILE = DEFAULT_DICOM_ANON_SPEC_DIRNAME + '/' + DEFAULT_DICOM_ANON_SPEC_FILENAME
########## ########## ########## ########## ##########

LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES = [
    ### ### ### ### ### ### ### ### ###
    # Phase 3 configs:
    ### ### ### ### ### ### ### ### ###
    # cfg_dicom_download_ver:
    # as per the previous default manner of downloading DICOM from Orthanc, == DICOMDIR:
    {'config_type'      : 'cfg_dicom_download_ver',
    'at_phase'          : 3,
    'config_version'    : LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION,
    'config_desc'       : LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC,
    'date_activated'    : LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE,
    'status_field'      : 'cfg_dicom_download_ver'
    },
    # NOTE: with the above LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_* initially set as LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_*_DICOMDIR
    # NOTE: but if the following configuration about this DICOM download has been set:
    # if  LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE (now w/ default=True)
    # then, dynamically update this INT_CFG in Settings, to:
    # {'config_type'      : 'cfg_dicom_download_ver',
    #   'at_phase'          : 3,
    #   'config_version'    : LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION_ZIPARCHIVE,
    #   'config_desc'       : LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC_ZIPARCHIVE,
    #   'date_activated'    : LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE_ZIPARCHIVE,
    #   'status_field'      : 'cfg_dicom_download_ver'
    #   },
    ### ### ### ### ### ### ### ### ###

    ### ### ### ### ### ### ### ### ###
    # Phase 4 configs:
    ### ### ### ### ### ### ### ### ###
    # cfg_dicom_download_ver:
    # as per the previous default manner of downloading DICOM from Orthanc, == DICOMDIR:
    {'config_type'      : 'cfg_dicom_anon_alignment_mode',
    'at_phase'          : 4,
    'config_version'    : LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION,
    'config_desc'       : LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC,
    'date_activated'    : LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE,
    'status_field'      : 'cfg_dicom_anon_alignment_mode'
    },
    # NOTE: with the above LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_* initially set as LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_*_ORIGINAL
    # NOTE: but if the following configuration about this DICOM download has been set:
    # if  LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP (now w/ default=True)
    # then, dynamically update this INT_CFG in Settings, to:
    # {'config_type'      : 'cfg_dicom_anon_alignment_mode',
    #   'at_phase'          : 4,
    #   'config_version'    : LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION_GCP,
    #   'config_desc'       : LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC_GCP,
    #   'date_activated'    : LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE_GCP,
    #   'status_field'      : 'cfg_dicom_anon_alignment_mode'
    #   },
    ### ### ### ### ### ### ### ### ###
    # cfg_dicom_anon_spec_ver:
    # NOTE: now directly referencing DEFAULT_DICOM_ANON_SPEC_FILE
    # WAS with:   'config_desc' : 'test w/ GCP ext_samsung.dat PRIOR to pass thru contrast_bolus_agent and set deid method',
    {'config_type'      : 'cfg_dicom_anon_spec_ver',
    'at_phase'          : 4,
    'config_version'    : DEFAULT_DICOM_ANON_SPEC_CFG_VER,
    'config_desc'       : DEFAULT_DICOM_ANON_SPEC_FILENAME,
    'date_activated'    : DEFAULT_DICOM_ANON_SPEC_CFG_DATE_ACTIVATED,
    'status_field'      : 'cfg_dicom_anon_spec_ver'
    },
    ### ### ### ### ### ### ### ### ###
    # cfg_dicom_anon_modalities_allowed:
    # NOTE: now directly referencing DEFAULT_DICOM_ANON_MODALITIES_STR:
    # WAS with:    'config_desc'       : 'cr,ct,dx,mr,nm,ot,rf,us,xa,xr',
    {'config_type'      : 'cfg_dicom_anon_modalities_allowed',
    'at_phase'          : 4,
    'config_version'    : DEFAULT_DICOM_ANON_MODALITIES_CFG_VER,
    'config_desc'       : DEFAULT_DICOM_ANON_MODALITIES_STR,
    'date_activated'    : DEFAULT_DICOM_ANON_MODALITIES_CFG_DATE_ACTIVATED,
    'status_field'      : 'cfg_dicom_anon_modalities_allowed'
    },
    ### ### ### ### ### ### ### ### ###
    # cfg_dicom_anon_seriesdesc_excludes:
    # NOTE: now directly referencing DICOM_SERIES_DESCS_TO_EXCLUDE:
    # WAS with: 'config_desc'       : 'screen save, dose report, basic text SR',
    {'config_type'      : 'cfg_dicom_anon_seriesdesc_excludes',
    'at_phase'          : 4,
    'config_version'    : DEFAULT_DICOM_SERIES2EXCLUDE_CFG_VER,
    'config_desc'       : DICOM_SERIES_DESCS_TO_EXCLUDE,
    'date_activated'    : DEFAULT_DICOM_SERIES2EXCLUDE_CFG_DATE_ACTIVATED,
    'status_field'      : 'cfg_dicom_anon_seriesdesc_excludes'
    }
#######
# PREVIOUS CONFIGS, for historical reference:
# per DEFAULT_DICOM_ANON_SPEC_FILE = './src_3rdParty/dicom_anon_spec_files/ext_keep_study_and_series_descs_replacePatientNameID_keepContrastAgents_andManufacturerInfoSoftwareFramesRowColBurnedinannotation.dat'
#    {'config_type'      : 'cfg_dicom_anon_spec_ver',
#    'at_phase'          : 4,
#    'config_version'    : '2024june04',
#    'config_desc'       : 'really really pass thru constrast_bolus_agent and set deid method',
#    'date_activated'    : '2024-06-04',
#    'status_field'      : 'cfg_dicom_anon_spec_ver'
#    },
# as per: DEFAULT_DICOM_ANON_MODALITIES_STR:
#   {'config_type'      : 'cfg_dicom_anon_modalities_allowed',
#   'at_phase'          : 4,
#   'config_version'    : '2018may01',
#   'config_desc'       : 'mr,ct,cr,ot',
#   'date_activated'    : '2018-05-01',
#   'status_field'      : 'cfg_dicom_anon_modalities_allowed'
#   },
#######
# TODO: DISABLE the following test cfgs prior to PRODUCTION
#   (but leave as easily re-enableable for testing)
# NOTE: TEMPORARY TESTING WITH additional temporary configs
#    ,
#    {'config_type'      : 'r3m0_TEST_config_p3',
#    'at_phase'          : 3,
#    'config_version'    : 'bogus_P3_test_for_r3m0',
#    'config_desc'       : 'test Phase03:ORTHANC config',
#    'date_activated'    : '2019-08-01',
#    'status_field'      : 'cfg_TEST_p3'
#    }
#    ,
#    {'config_type'      : 'r3m0_TEST_config_p4',
#    'at_phase'          : 4,
#    'config_version'    : 'bogus_P4_test_for_r3m0',
#    'config_desc'       : 'test Phase04:DICOM_ANON config',
#    'date_activated'    : '2019-08-01',
#    'status_field'      : 'cfg_TEST_p4'
#    }
#    ,
#    {'config_type'      : 'r3m0_TEST_config_p5',
#    'at_phase'          : 5,
#    'config_version'    : 'bogus_P5_test_for_r3m0',
#    'config_desc'       : 'test Phase05:AWS s3 config',
#    'date_activated'    : '2019-08-01',
#    'status_field'      : 'cfg_TEST_p5'
#    }
#######
]
# TODO: be sure to update the above LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVE_* list of dictionaries
# whenever the internal configuration has changed enough to warrant a new version.

# Input Manifest headers (compare against uppercase):
# moving towards a more generalizable dynamic manifest w/ hard-coded set of headers:
# (and remember to set all of these as uppercase except for the manifest_ver, since all others are compared in upper)
MANIFEST_NUM_HEADERS = 7
MANIFEST_HEADER_SUBJECT_ID = 'SUBJECT_ID'
MANIFEST_HEADER_OBJECT_INFO_01 = 'IMAGING_TYPE'
MANIFEST_HEADER_OBJECT_INFO_02 = 'AGE_AT_IMAGING_(DAYS)'
MANIFEST_HEADER_OBJECT_INFO_03 = 'ANATOMICAL_POSITION'
# Manual DeID QC:
MANIFEST_HEADER_DEID_QC_STATUS = 'DEID_QC_STATUS'
#######
# EVENTUALLY ALSO ADD: MANIFEST_HEADER_OBJECT_INFO_04 = 'OBJECT_INFO_04'
# EVENTUALLY THEN ALSO UPDATE: MANIFEST_NUM_HEADERS = 8
#######
MANIFEST_HEADER_ACCESSION_NUM = 'ACCESSION_NUM'
MANIFEST_HEADER_MANIFEST_VER = 'locutus_manifest_ver:locutus.onprem_dicom_deid_qc.2021march15'
# NOTE: the above are the MANIFEST_HEADERs as expected for such a hard-coded manifest
# TODO: also collect the ACTUAL dynamic input manifest headers
# (including any additional data-less ones conforming to an eventual manifest version number for internal validation)
# for re-use later when generating the MANIFEST_OUTPUT, as below:

# And, additional fields for generating a MANIFEST_OUTPUT in stdout via:
#    grep MANIFEST_OUTPUT [log] | sed 's/MANIFEST_OUTPUT:,//` > manifest_out.csv
MANIFEST_OUTPUT_PREFIX = "MANIFEST_OUTPUT:"

MANIFEST_NUM_HEADERS_WITH_QC = MANIFEST_NUM_HEADERS + 2
# optional additional INPUT fields beyond MANIFEST_HEADER_MANIFEST_VER iff LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS:
MANIFEST_HEADER_OUTPUT_PROCESSED_STATUS = 'PROCESSED_STATUS'
MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL = 'DEID_QC_STUDY_URL'

# and further output fields:
MANIFEST_HEADER_OUTPUT_TARGET = 'deid_bucket_path'
MANIFEST_HEADER_OUTPUT_FW_IMPORT_SESSION_ARG = 'flywheel_import_session_arg'
MANIFEST_HEADER_AS_OF_DATETIME = "ONPREM_DICOM-PROCESSED_AS_OF:"

# An internal configuration to allow the printing of commented lines into the MANIFEST_OUTPUT.
# Consider, for example, input manifests which might still have a pre-split accession number listed,
# but commented out, and is followed by its respective split accession nunmbers.
# We will want this included in the MANIFEST_OUTPUT such that all rows still align:
MANIFEST_OUTPUT_INCLUDE_COMMENT_LINES = True

# CFG_OUT:
# NOTE: support quick stdout log crumb to easily create a CFG output CSV via: grep CFG_OUT
# Listing here the most recent header values from src_modules.settings.py (& not yet in its Settings class)
# For here, though, only the CFG_OUT_PREFIX is actually needed, since the headers are starting in Settings:
####################################################
# src_modules.settings.CFG_OUT_PREFIX == "CFG_OUT:"
# src_modules.settings.CFG_OUT_HEADER_KEY_TYPE == 'KEY_TYPE'  # e.g., cfg, env, or hardcoded, etc.
# src_modules.settings.CFG_OUT_HEADER_KEY == 'KEY'
# src_modules.settings.CFG_OUT_HEADER_VAL = 'VALUE'

CLASS_PRINTNAME = 'OnPremDicom'
SYS_STAT_MODULENAME = 'DICOM_OnPrem'

class OnPrem_Dicom:

    def __init__(self,
                locutus_settings):
        self.locutus_settings = locutus_settings
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: {0}.init() says Hi'.format(CLASS_PRINTNAME), flush=True)
        self.onprem_dicom_config = []
        self.src_orthanc_vault_config_path = ''
        self.deid_qc_orthanc_vault_config_path = ''
        self.hvac_client = None
        self.src_onprem_dicom_orthanc_url = None
        self.deid_qc_onprem_dicom_orthanc_url = None
        self.onprem_dicom_target_db_name = ''
        self.onprem_dicom_target_db = ''
        self.this_hostname = ''
        self.manifest_infile = None
        self.manifest_reader = None


    ####################
    # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
    #
    # NOTE: be careful to respect any pre-existing SPLIT accession_nums w/ '###.001', '####.002', etc.
    # For example:
    # if Staged accession_str = VIRT1234, and Locutus previously used accession_num = 1234, then:
    #   if the curr Locutus accession_num = 1234, then UPGRADE Locutus acc to VIRT1234
    #   else if the Locutus accession_num = 1234.001, .002, etc., then UPGRADE to VIRT1234.001, .002, etc.
    ####################
    def upgrade_alphanum_accessions_for_Juneteenth(self, curr_status_table, curr_manifest_table):
        if curr_status_table == LOCUTUS_ONPREM_DICOM_STATUS_TABLE \
        and curr_manifest_table == LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE:
            print('{0}.upgrade_alphanum_accessions_for_Juneteenth() detected OnPrem STATUS & MANIFEST tables, '\
                'will aim to UPGRADE these w/ accession_num (BOTH tables)...'.format(CLASS_PRINTNAME), flush=True)
        else:
            print('{0}.upgrade_alphanum_accessions_for_Juneteenth() ERROR: OnPrem NOT recognizing EITHER '\
                'STATUS {1} nor MANIFEST {2} table, will NOT proceed'.format(
                    CLASS_PRINTNAME, curr_status_table, curr_manifest_table), flush=True)
            raise ValueError('Locutus {0}.upgrade_alphanum_accessions_for_Juneteenth() ERROR: recognized NEITHER'.format(
                                CLASS_PRINTNAME))

        # Gather up list from the Stager of the following:
        # trig_dicom_staging=# select count(*) from dicom_stablestudies where text(accession_num) != accession_str and accession_num > 0;
        #
        ###########
        # DEV NOTE: leave the StagerDB retired accession_nums with NUMERIC, to still easily retain the accession_num > 0 / POSITION('-',...) retirement indicator
        # AND, update the Stager and Locutus itself to use a new RETIRED flag, rather than checking for > 0, etc
        #
        # For this Upgrade Path itself, then:
        #   Keeping the StagerDB as accession_num BIGINT with accession_str TEXT
        #
        # NOTE: see also previous StagerDBconnSession examples referencing RETIRED,
        # to formerly limit to the (non-retired) active accessions via:
        #   WHERE accession_num > 0 AND change_seq_id > 0
        # Now, simply via:
        #   WHERE active
        #################
        # NOTE: ENHANCED 6/03/2025 to only list staged_changes as those which are the MAX(change_seq_id) for any UUID,
        # so as to align with the removal of as_change_seq_id, and as inspired by the new extra_changes_result, further below:
        #   extra_changes_result =  self.LocutusDBconnSession.execute('WITH temp_num_changes AS '\ [...] LOCUTUS_ONPREM_DICOM_STATUS_TABLE))
        # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
        # Leaving the Stager accession_num as BIGINT, so this can still use the older accession_num > 0, being deprecated for active.
        #############
        staged_alphas_result =  self.StagerDBconnSession.execute('SELECT accession_str, accession_num, uuid '\
                                                                    'FROM {0} '\
                                                                    'WHERE active '\
                                                                    'AND text(accession_num) != accession_str '\
                                                                    'ORDER by accession_str ;'.format(
                                                                    STAGER_STABLESTUDY_TABLE))

        print('r3m0 DEBUG: --------------------------------------------------------------', flush=True)
        staged_alphas_dicts = {}
        for row in staged_alphas_result:
            staged_str = row['accession_str']
            staged_num = row['accession_num']
            staged_uuid = row['uuid']
            staged_alphas_dicts[staged_str] = [staged_num, staged_uuid]

        num_staged_alphas_dicts = len(staged_alphas_dicts)
        curr_staged_alphas_dict = 0

        num_staged_alphas_dicts_remigrated_status = 0
        num_staged_alphas_dicts_remigrated_manifest = 0

        print('r3m0 DEBUG: --------------------------------------------------------------', flush=True)
        print('r3m0 DEBUG: Just added {0} distinct accession_str:_num pairs to staged_alphas to compare with: {1} & {2}. Here they be...'.format(
                num_staged_alphas_dicts, curr_status_table, curr_manifest_table), flush=True)
        for alpha_acc_key, alpha_acc_vals in staged_alphas_dicts.items():
            alpha_acc_numval = alpha_acc_vals[0]
            alpha_acc_uuid = alpha_acc_vals[1]
            curr_staged_alphas_dict += 1
            print('r3m0 DEBUG: - - - - - - - - - - - - - - - - - - -', flush=True)
            print('{0}.upgrade_alphanum_accessions_for_Juneteenth() '\
                'Preparing to RE-RE-MIGRATE alpha # {1} of {2} ...  STAGED ALPHA \'{3}\' -> NUM {4} (uuid=\'{5}\') '\
                'into the Locutus DB '\
                '{6} & {7} as accession_num alpha-numeric \'{3}\''.format(
                CLASS_PRINTNAME,
                curr_staged_alphas_dict, num_staged_alphas_dicts,
                alpha_acc_key, alpha_acc_numval, alpha_acc_uuid,
                curr_status_table, curr_manifest_table))

            #################
            # FOLLOWING the alpha-numeric upgrade of Juneteenth 2025, in which accession_num_src might have been used to detect any previous SPLITS....
            # NOTE: Deprecating the antiquated SPLITS approach (previously using accession_num_src to handle multi-UUIDs), prior to merging with the Radiology team.
            # NOTE: BEFORE actually migrating,
            # ENSURE that its Locutus accession_num is NOT a split one,
            # and IF SO, be sure to add the split suffix as well.
            #
            # This can ONLY be on the STATUS table, via UUID and w/ accession_num_src
            # this upgrade_alphanum_...() method should be FIRST update the STATUS table,
            # and THEN the MANIFEST record, once its accession_num & accession_num_src is known from the UUID
            #
            # FIRST, from STATUS via:
            num_status_records = 0
            curr_status_results = []
            status_migrated_alpha_results =  self.LocutusDBconnSession.execute('SELECT accession_num, uuid '\
                                                                    'FROM {0} '\
                                                                    'WHERE uuid=\'{1}\' '\
                                                                    '    AND active ;'.format(
                                                                    curr_status_table,
                                                                    alpha_acc_uuid))
            for status_select_row in status_migrated_alpha_results.fetchall():
                num_status_records += 1
                curr_status_results.append(status_select_row)
                status_staged_num = status_select_row['accession_num']
                status_staged_uuid = status_select_row['uuid']

            if num_status_records <= 0:
                print('r3m0 DEBUG: ERROR: Just found {0} STATUS records for uuid=\'{1}\' in STATUS table {1}. If none.. Perhaps these have ALREADY been Upgraded?'.format(
                    num_status_records, alpha_acc_uuid, curr_status_table), flush=True)
            else:
                # NOTE: num_status_records > 1 might merely be an antiquated SPLIT
                for idx, status_select_row in enumerate(curr_status_results):

                    # From the STATUS table...
                    status_staged_num = status_select_row['accession_num']
                    status_staged_uuid = status_select_row['uuid']
                    print('r3m0 DEBUG: found # {0} of {1} STATUS records for uuid=\'{2}\' in STATUS table {3}:  '\
                        'status_staged_num=\'{4}\''.format(
                        (idx+1), num_status_records, alpha_acc_uuid, curr_status_table, status_staged_num), flush=True)

                    status_staged_alphanum = self.locutus_settings.itoa(status_staged_num)

                    new_acc_alphanum = alpha_acc_key
                    new_acc_alphanum_src = alpha_acc_key

                    if status_staged_alphanum == new_acc_alphanum:
                        print('r3m0 INFO: ======== PREVIOUSLY UPDATED uuid=\'{0}\' in STATUS table {1} '\
                            'TO: new_acc_alphanum=\'{2}\''.format(
                            alpha_acc_uuid, curr_status_table,
                            new_acc_alphanum), flush=True)
                    else:
                        print('r3m0 INFO: ========> ABOUT TO UPDATE uuid=\'{0}\' in STATUS table {1} '\
                            'TO: new_acc_alphanum=\'{2}\''.format(
                            alpha_acc_uuid, curr_status_table,
                            new_acc_alphanum), flush=True)

                        # NOW, UPDATE the STATUS table
                        status_updated_alpha_results =  self.LocutusDBconnSession.execute('UPDATE {0} '\
                                                                    'SET accession_num=\'{1}\' '\
                                                                    'WHERE uuid=\'{2}\' '\
                                                                    '    AND active ;'.format(
                                                                    curr_status_table,
                                                                    new_acc_alphanum,
                                                                    alpha_acc_uuid))

                        # up our tally of those re-migrated to the STATUS table:
                        num_staged_alphas_dicts_remigrated_status += 1

                    # NOTE: the SECOND, check in on the MANIFEST table WAS HERE within the else (i.e., NOT if num_status_records <= 0:), within the above UPDATE STATUS
                    # and should be tested independently, especially for those where the STATUS has already been updated, but the MANIFEST remains needing update.
                    # BUT BUT BUT, the above STATUS record is needed to convert from the UUID to the accession_num, right?
                    # NOPE, can just use alpha_acc_numval directly:

            ##########
            # SECOND, check in on the MANIFEST table, NOTING that we can expect a sparsely incomplete set,
            # and should be robust to missing MANIFEST records, even if not so w/ the above STATUS records.
            # NOTE: in the case of MULTI-UUIDs, the first MANIFEST table SELECT & UPDATE will likely find all that match the accession_num,
            # which should STILL only be one row, even if with manifest_status=ERROR_MULTIPLE_UUIDS.....
            # NOTE: NOTE: BUT in the case of ACTUAL accessions with overlapping atoi()s, such as: OSH12345 & VIRT12345 & 12345,
            # the following method is a bit too brute force, and will mistakenly convert the 12345 to OSH12345 or VIRT12345, whichever is currently being tested.
            # Only allowing this to slide because I can't foresee a way around this at the moment, and this seems applicable only for one such overlap, a VIRT8%26
            manifest_migrated_alpha_results =  self.LocutusDBconnSession.execute('SELECT COUNT(accession_num) as num_recs '\
                                                                    'FROM {0} '\
                                                                    'WHERE accession_num=\'{1}\' '\
                                                                    '    AND active ;'.format(
                                                                    curr_manifest_table,
                                                                    alpha_acc_numval))
            manifest_migrated_alpha_results_row = manifest_migrated_alpha_results.fetchone()
            manifest_migrated_alpha_results_count = manifest_migrated_alpha_results_row['num_recs']

            # NOTE: if a corresponding MANIFEST record exists, update the MANIFEST table:
            if manifest_migrated_alpha_results_count <= 0:
                print('r3m0 DEBUG: INFO: Found {0} MANIFEST records for accession_num=\'{1}\' in MANIFEST table {1}. If none.. Perhaps these have ALREADY been Upgraded, perhaps?'.format(
                    manifest_migrated_alpha_results_count, alpha_acc_numval, curr_manifest_table), flush=True)
            elif manifest_migrated_alpha_results_count > 0:
                new_acc_alphanum = alpha_acc_key
                print('r3m0 INFO: ======== & ABOUT TO UPDATE {0} row(s) in MANIFEST table {1} '\
                    'FROM: accession_num={2} '\
                    'TO: new_acc_alphanum=\'{3}\''.format(
                    manifest_migrated_alpha_results_count, curr_manifest_table,
                    alpha_acc_numval,
                    new_acc_alphanum), flush=True)

                if manifest_migrated_alpha_results_count > 1:
                    # NOTE: just a WARNING condition, no need to throw a ValueError:
                    print('r3m0 DEBUG: WARNING: >1 active manifest_migrated_alpha_results_count == {0}; UPDATING each of them.'.format(manifest_migrated_alpha_results_count), flush=True)

                # NOW, UPDATE the MANIFEST table
                #WAS: manifest_reupdated_alpha_results =  self.LocutusDBconnSession.execute('UPDATE {0} '\
                #                                            'SET accession_num=\'{1}\' '\
                #                                            'WHERE accession_num=\'{2}\' '\
                #                                            '    AND active ;'.format(
                #                                            curr_manifest_table,
                #                                            new_acc_alphanum,
                #                                            status_staged_num))
                # TESTING WHY IT DOESN'T SEEM TO BE actuallllllly UPDATING: AHHHHH, because it had status_staged_num INSTEAD OF new_acc_alphanum
                update_cmd = 'UPDATE {0} '\
                                                            'SET accession_num=\'{1}\' '\
                                                            'WHERE accession_num=\'{2}\' '\
                                                            '    AND active ;'.format(
                                                            curr_manifest_table,
                                                            new_acc_alphanum,
                                                            alpha_acc_numval)
                print('r3m0 DEBUG: about to UPDATE the MANIFEST TABLE with the following SQL: [ {0} ]'.format(update_cmd), flush=True)
                manifest_reupdated_alpha_results =  self.LocutusDBconnSession.execute(update_cmd)
                print('r3m0 DEBUG: MANIFEST UPDATE complete.... hopefully!', flush=True)

                # up our tally of those re-migrated to the MANIFEST table:
                num_staged_alphas_dicts_remigrated_manifest += 1

        print('r3m0 DEBUG: - - - - - - - - - - - - - - - - - - -', flush=True)

        print('r3m0 DEBUG: --------------------------------------------------------------', flush=True)
        print('r3m0 DEBUG: And that is ALL FOLX.  {0} distinct accession_str:_num pairs RE-MIGRATED: {1} & {2}'.format(
                num_staged_alphas_dicts, curr_status_table, curr_manifest_table), flush=True)

        if num_staged_alphas_dicts_remigrated_status >= num_staged_alphas_dicts:
            print('r3m0 DEBUG: NOTE: ALL {0} of the {1} distinct accession_str:_num pairs have been RE-MIGRATED into {2}, & {3} into {4}'.format(
                num_staged_alphas_dicts_remigrated_status, num_staged_alphas_dicts, curr_status_table,
                num_staged_alphas_dicts_remigrated_manifest, curr_manifest_table), flush=True)
        else:
            print('r3m0 DEBUG: WARNING: ONLY {0} of the {1} distinct accession_str:_num pairs have been RE-MIGRATED into {2}, & {3} into {4}'.format(
                num_staged_alphas_dicts_remigrated_status, num_staged_alphas_dicts, curr_status_table,
                num_staged_alphas_dicts_remigrated_manifest, curr_manifest_table), flush=True)

        # As well as some MANIFEST_OUTPUT stats from the Stager, the STATUS, and the MANIFEST tables:
        print('{0},# Juneteenth2025Upgrade,alpha-numeric accessions:,{1},staged_from_STAGER,{2}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    num_staged_alphas_dicts,
                    STAGER_STABLESTUDY_TABLE),
                    flush=True)
        print('{0},# Juneteenth2025Upgrade,alpha-numeric accessions:,{1},re-migrated_to_STATUS,{2}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    num_staged_alphas_dicts_remigrated_status,
                    curr_status_table),
                    flush=True)
        print('{0},# Juneteenth2025Upgrade,alpha-numeric accessions:,{1},re-migrated_to_MANIFEST,{2}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    num_staged_alphas_dicts_remigrated_manifest,
                    curr_manifest_table),
                    flush=True)

        print('r3m0 DEBUG: --------------------------------------------------------------', flush=True)
        # end o' upgrade_alphanum_accessions_for_Juneteenth()


    def walk_and_redact_zip_archive_patient_study_levels(self, dicom_uuid_topdir, enable_redact_subdirs):
        # finds and returns the Patient level, regardless,
        # and if enable_redact_subdirs, will also redact the Patient and Study levels
        # NOTE: very primitive redaction of subdirs, may also want to consider mod to dicom_anon to introduce the UIDs at each level
        patient_top_level_dir = 'UNKNOWN_patient_top_level_dir_pre:walk_and_redact_zip-archive...()'
        REDACTED_PATIENT_LEVEL = 'PATIENT'
        REDACTED_STUDY_LEVEL = 'STUDY'
        # MOVED up top: PATH_DELIM = '/'
        level_names = {0 : "UUID", 1 : "Patient", 2 : "Study", 3 : "Series", 4: "TooDeep" , 4: "WayTooDeepLOL" }

        print('DEBUG walk_and_redact_zip_archive_patient_study_levels(): hello, about to os.walk({0})'.format(dicom_uuid_topdir), flush=True)
        level_num=0

        num_topmost_delimeters = dicom_uuid_topdir.count(PATH_DELIM)

        print('DEBUG walk_and_redact_zip_archive_patient_study_levels(): # top delimeters = ', num_topmost_delimeters, ' in the top-level: ', dicom_uuid_topdir)
        print('- - - - - - - - - - - - - - - -')

        level_patient_from=''
        level_patient_to=''
        level_study_from=''
        level_study_to=''

        for root, dirs, files in os.walk(dicom_uuid_topdir):
            # NOTE: need to break this up into 2 parts, since the rename at PATENT will break the walk
            # So instead, cache the values down first, merely doing the renames thereafter
            # REDACT: print('r3m0 DEBUG, at root=',root)

            num_this_root_delimeters = root.count(PATH_DELIM)
            # REMOVED FROM OUTPUT LOG: print('r3m0 DEBUG: # top delimeters = ', num_this_root_delimeters, ' in this root: ', root)
            num_diff_delimeters = num_this_root_delimeters - num_topmost_delimeters
            print('DEBUG: # DIFF delimeters = ', num_diff_delimeters, ' in this root-top. ')
            level_num = num_diff_delimeters
            level_name =  level_names.get(level_num)

            #print("level ", level_num, "==", level_names.get(level_num), ": root=", root, "consumes", end=" ")
            # REMOVED FROM OUTPUT LOG: print("level ", level_num, "==", level_name, ": root=", root, "consumes", end=" ")
            #print(sum(getsize(join(root, name)) for name in files), end=" ")
            #print("bytes in", len(files), "non-directory files, with: ", len(dirs), " dirs")

            if level_name == "Patient":
                # at PATIENT root: rename last level from current to REDACTED_PATIENT_LEVEL
                last_root_delim = root.rfind(PATH_DELIM)
                new_patient_dirname = root[:last_root_delim] + PATH_DELIM + REDACTED_PATIENT_LEVEL
                level_patient_from = root
                level_patient_to = new_patient_dirname
                #####
                # REMOVED FROM OUTPUT LOG: print('DELAY... RENAMING Patient_root...\n FROM =', root, '\n TO = ', new_patient_dirname)
                # REMOVED FROM OUTPUT LOG: print('patient_level to rename FROM: ', level_patient_from, ' TO: ', level_patient_to)
                #####
                # and SET the return patient_top_level_dir to return; leaving as the _from in case redact subdirs not enabled
                patient_top_level_dir = level_patient_from

            elif level_name == "Study":
                # at STUDY root: rename last level from current to REDACTED_STUDY_LEVEL
                last_root_delim = root.rfind(PATH_DELIM)
                new_study_dirname = root[:last_root_delim] + PATH_DELIM + REDACTED_STUDY_LEVEL
                level_study_from = root
                level_study_to = new_study_dirname
                #####
                # WARNING: even with only showing the TO redacted Study name,
                # it would still include the UNREDACTED Patient name level,
                # so don't show these at all:
                ##############################
                # REMOVED FROM OUTPUT LOG: print('DELAY... RENAMING Study_root...\n FROM =', root, '\n TO = ', new_study_dirname)
                # REMOVED FROM OUTPUT LOG: print('study_level to rename FROM: ', level_study_from, ' TO: ', level_study_to)
                #print('study_level to rename TO: ', level_study_to)
                #####
                # found the level with all Series.... but what IF there is only 1 Series? Think Butterball! :-)
                #####
                print('Bailing at this level == ', level_names.get(level_num))
                print('Series Dirs: \n*', '\n* '.join(dirs))
                break
        print('- - - - - - - - - - - - - - - -')

        # at end of the walk, now do the renames working back upwards:
        # such that the upstream paths remain constant
        # but... ONLY if enable_redact_subdirs:
        if enable_redact_subdirs:
            print('DEBUG walk_and_redact_zip_archive_patient_study_levels(w/ enable_redact_subdirs=True): carrying on with subdir redaction ', flush=True)

            # 1. rename STUDY level
            # REMOVED FROM OUTPUT LOG: print('NOW... RENAMING Study_root...\n FROM =', level_study_from, '\n TO = ', level_study_to )
            print('RENAMING Study_root... ', flush=True)
            # NOTE: CHECK that a previously such redacted STUDY doesn't already exist in this tree:
            if os.path.isdir(level_study_to):
                # and ONLY shutil.rmtree() if it DOES exist
                print('REMOVING OLD Study_root.', flush=True)
                shutil.rmtree(level_study_to)
            os.rename(level_study_from, level_study_to)
            print('RENAMED Study_root.', flush=True)

            # 2. rename PATIENT level
            # REMOVED FROM OUTPUT LOG: print('NOW... RENAMING Patient_root...\n FROM =', level_patient_from, '\n TO = ', level_patient_to )
            print('RENAMING Patient_root... ', flush=True)
            # NOTE: CHECK that a previously such redacted PATIENT doesn't already exist in this tree:
            if os.path.isdir(level_patient_to):
                # and ONLY shutil.rmtree() if it DOES exist
                print('REMOVING OLD Patient_root.', flush=True)
                shutil.rmtree(level_patient_to)
            os.rename(level_patient_from, level_patient_to)
            print('RENAMED Patient_root.', flush=True)
            # and SET the return patient_top_level_dir to return; changing to the _to since subdirs IS enabled
            patient_top_level_dir = level_patient_to

        return patient_top_level_dir
        # end of walk_and_redact_zip_archive_patient_study_levels()


    def get_study_uuid_via_instance_from_Orthanc(self, http_headers, orthanc_url, uploaded_instance_suburl):
        ###################################################################################
        # SAVE the following for one final one, once all have been transferred....
        # NOTE: MAY want to just save it for the end, and use a DoGet(), perhaps?
        #################
        # AND: can have an initial look for ParentStudy in the first returned JSON content,
        # before resorting to the more tedious (but readily available) traversal up from the instance to its ParentSeries and its ParentStudy.
        #################

        orthanc_get_study_url = None
        orthanc_explorer_study_url = None

        orthanc_get_instance_url = '{0}{1}'.format(orthanc_url, uploaded_instance_suburl)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.get_study_uuid_via_instance_from_Orthanc(): querying SeriesPath at \'{1}\'...'.format(
                        CLASS_PRINTNAME,
                        orthanc_get_instance_url),
                        flush=True)

        # NOTE: be sure to instantiate a new http_obj for each and every new request;
        # otherwise may encounter an [Errno 32] Broken pipe:
        http_obj = httplib2.Http()

        instance_content = ''
        instance_resp, returned_instance_content = http_obj.request(orthanc_get_instance_url, 'GET',
                    body = instance_content,
                    headers = http_headers)

        orthanc_instance_retval = instance_resp.status
        returned_instance_json = json.loads(returned_instance_content)
        returned_series_suburl = None
        if 'ParentSeries' not in returned_instance_json:
            # TODO: Q: throw an exception, or just this quiet little WARNING?
            print('{0}.get_study_uuid_via_instance_from_Orthanc(): DEBUG WARNING: no \'ParentSeries\' found in uploaded Orthanc instance '\
                            'returned instance JSON content of: {1}'.format(
                            CLASS_PRINTNAME,
                            returned_instance_json),
                            flush=True)
        else:
            returned_series_suburl = '/series/{0}'.format(returned_instance_json['ParentSeries'])

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.get_study_uuid_via_instance_from_Orthanc() =====> the above call to Orthanc just '\
                                        'returned a status of: {1}, with returned series sub-URL of {2} from JSON content of: {3}'.format(
                                        CLASS_PRINTNAME,
                                        orthanc_instance_retval,
                                        returned_series_suburl,
                                        returned_instance_json),
                                        flush=True)

            # And now that the instance GET was succesful, try a GET of its series, to find its parent study:
            # NOTE: no need for a '/' delimiter between the orthanc_url and returned_series_suburl
            orthanc_get_series_url = '{0}{1}'.format(orthanc_url, returned_series_suburl)

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.get_study_uuid_via_instance_from_Orthanc(): NOW querying SeriesPath at \'{1}\'...'.format(
                            CLASS_PRINTNAME,
                            orthanc_get_series_url),
                            flush=True)

            series_content = ''
            series_resp, returned_series_content = http_obj.request(orthanc_get_series_url, 'GET',
                        body = series_content,
                        headers = http_headers)

            orthanc_series_retval = series_resp.status
            returned_series_json = json.loads(returned_series_content)
            returned_study_suburl = None
            if 'ParentStudy' not in returned_series_json:
                # TODO: Q: throw an exception, or just this quiet little WARNING?
                print('{0}.get_study_uuid_via_instance_from_Orthanc(): DEBUG WARNING: no \'ParentSeries\' found in uploaded Orthanc instance '\
                            'returned series JSON content of: {1}'.format(
                            CLASS_PRINTNAME,
                            returned_series_json),
                            flush=True)
            else:
                returned_study_suburl = '/studies/{0}'.format(returned_series_json['ParentStudy'])
                # NOTE: no need for a '/' delimiter between the orthanc_url and returned_study_suburl
                orthanc_get_study_url = '{0}{1}'.format(orthanc_url, returned_study_suburl)
                #####
                # the API endpoint to this parent study, to download its DICOMDIR (flat structure of: IMAGES/IMG####)
                orthanc_get_study_media_url = '{0}/media'.format(orthanc_get_study_url)
                # NOTE: -r3m0 3/24/2025 = download of study FROM HEREON to be retrieved as a zip file (w/ subdirs for each Series/MR####.csv),
                # as per: https://orthanc.uclouvain.be/book/users/rest.html#downloading-studies
                # DON'T AUTOMATICALLY REPLACE w/ archive, unless at subsequently check if LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE:
                #   orthanc_get_study_media_url = '{0}/archive'.format(orthanc_get_study_url)
                ###############################################################
                # NOTE: NOT really sure that this orthanc_get_study_media_url is even being used anywhere, whether here in orthanc_get_study_media_url() or otherwise.
                # NOTE: prior to 3/24/2025 LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE was absent ~= False == DICOMDIR (flat IMAGES/IM** structure) = LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE
                if self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE:
                    # first, emit a notice of the impending override, whether or not LOCUTUS_VERBOSE:
                    print('{0}.Process(): orthanc_get_study_media_url(): INFO: LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE '\
                        'Overridding uuid\'s url_dicomdir *TO* zip Archive endpoint ({1}), *FROM* DICOMDIR endpoint ({2}) of: {3}'.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.ORTHANC_DOWNLOAD_ZIP_ENDPOINT,
                                self.locutus_settings.ORTHANC_DOWNLOAD_DICOMDIR_ENDPOINT,
                                orthanc_get_study_media_url),
                                flush=True)
                    # Rather than changing any of the Staging tables at this time,
                    # merely programmatically override from the DICOMDIR media endpoint
                    # to use the zip Archive archive endpoint:
                    orthanc_get_study_media_url = orthanc_get_study_media_url.replace(self.locutus_settings.ORTHANC_DOWNLOAD_DICOMDIR_ENDPOINT, self.locutus_settings.ORTHANC_DOWNLOAD_ZIP_ENDPOINT)
                #####
                # and the GUI explorer interface URL to it:
                orthanc_explorer_study_url = '{0}/app/explorer.html#study?uuid={1}'.format(orthanc_url, returned_series_json['ParentStudy'])

                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.get_study_uuid_via_instance_from_Orthanc(): the above call to Orthanc just '\
                                            'returned a status of: {1}, with returned study API URL of {2} (and explorer URL of {3}) '\
                                            'from JSON content of: {4}'.format(
                                            CLASS_PRINTNAME,
                                            orthanc_instance_retval,
                                            orthanc_get_study_url,
                                            orthanc_explorer_study_url,
                                            returned_series_json),
                                            flush=True)

        return (orthanc_get_study_url, orthanc_explorer_study_url)
        # end of get_study_uuid_via_instance_from_Orthanc()


    # copy_local_directory_to_Orthanc()
    # As sourced and modified from module_gcp_dicom.py:copy_local_directory_to_GS()
    #
    # Helper function to copy entire directory tree of DICOM files up to Orthanc, as from:
    # https://stackoverflow.com/questions/48514933/how-to-copy-a-directory-to-google-cloud-storage-using-google-cloud-python-api
    # TODO: eventually move this into a general Locutus helper function,
    # but for now, starting with it right here in GCPDicom (and now, ONPREM_Dicom), where it is needed:
    #
    # RETURNS: False if success; True (and/or, eventually a non-zero HTTP status) if any error(s) encountered
    #   AND: last_returned_api_study_url
    #   AND: last_returned_explorer_study_url
    # NOTE: caller may want to wrap in a try/catch as this may throw an exception.
    #
    # TODO: test a more efficient PUT using Orthanc-Book's recommendation for Expect, but perhaps not as applicable outside of curl:
    #   "Note that in the case of curl, setting the Expect HTTP Header will significantly reduce the execution time of POST requests:"
    # TODO: make more efficient by just perhaps checking on the very first and/or last study url
    #   NOTE: currently only grabs the parent study ID of the first image uploaded; introduce last as a safety check
    # (eventually to expand to include further error details, once available)
    def copy_local_directory_to_Orthanc(self, top_local_path, local_path, orthanc_url, orthanc_user, orthanc_password, image_counter):
        """Recursively copy a directory of files to Orthanc.

        local_path should be a directory and not have a trailing slash.
        """
        # TODO: consider commenting out some of the VERBOSE prints,
        # as, even w/ LOCUTUS_VERSION, some of these recursive upload messages might be MUCH TOO verbose!
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.copy_local_directory_to_Orthanc(): preparing to upload from top_local_path=\'{1}\' '\
                        'with local_path=\'{2}\', '\
                        'to Orthanc url=\'{3}\' with image_counter={4}'.format(
                        CLASS_PRINTNAME,
                        top_local_path,
                        local_path,
                        orthanc_url,
                        image_counter),
                        flush=True)

        orthanc_err = False
        upload_count_success = 0
        upload_count_error = 0
        last_returned_api_study_media_url = 'UNKNOWN_API_STUDY_URL'
        last_returned_explorer_study_url = 'UNKNOWN_EXPLORER_STUDY_URL'
        DICOM_MAGIC_TYPE = 'DICOM medical imaging data'

        # setup http for each file upload:
        headers = {'Content-Type': 'application/dicom',}
        # As from: https://hg.orthanc-server.com/orthanc/file/default/OrthancServer/Resources/Samples/ImportDicomFiles/ImportDicomFiles.py
        # This is a custom reimplementation of the
        # "Http.add_credentials()" method for Basic HTTP Access
        # Authentication (for some weird reason, this method does
        # not always work)
        # http://en.wikipedia.org/wiki/Basic_access_authentication
        creds_str = orthanc_user + ':' + orthanc_password
        creds_str_bytes = creds_str.encode("ascii")
        creds_str_bytes_b64 = b'Basic ' + base64.b64encode(creds_str_bytes)
        headers['authorization'] = creds_str_bytes_b64.decode("ascii")
        orthanc_post_url = '{0}/instances'.format(orthanc_url)

        # For more control over creating a non-empty exception, replacing:
        if not os.path.isdir(local_path):
            # TODO: consider checking self.locutus_settings.LOCUTUS_FORCE_SUCCESS,
            # but since this is required to upload into GS for GCP-based DeID,
            # it really is a show stopper at this point:
            raise ValueError('Directory not found for copy_local_directory_to_Orthanc() local path of "{0}".'.format(
                                local_path))

        # NOTE: to TEST with a bogus non-DICOM file in the upload dir, pause here
        # BEFORE the glob in order to add the file into the interim directory,
        # which could be done locally as simply as via something like the following:
        #   echo "TESTING non-DICOM" > /private/tmp/locutus_prod2dev/onprem-dicom/phase04_dicom_deids/uuid_[UUID]/nonDICOM.txt
        #pdb.set_trace()

        for local_file in glob.glob(local_path + '/**'):
            if os.path.isfile(local_file):
                image_counter += 1

                # r3m0: TODO: disable even this local_file along with the mighty verbose DEBUG INFO messages once all good:
                """
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.copy_local_directory_to_Orthanc(): uploading local_file=\'{1}\'...'.format(
                                CLASS_PRINTNAME,
                                local_file),
                                flush=True)
                elif image_counter == 1:
                        # for NON-verbose, create a minimal running output header, at the first image only:
                        print('{0}.copy_local_directory_to_Orthanc(): approximate image upload counter: [{1}]'.format(
                                    CLASS_PRINTNAME,
                                    image_counter),
                                    end = '',
                                    flush=True)
                """

                if (image_counter % 10) == 0:
                    # first an image-uploading status update to stdout:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.copy_local_directory_to_Orthanc(): at approximately image #{1} pinging the DB to keep alive'.format(
                                    CLASS_PRINTNAME,
                                    image_counter),
                                    flush=True)
                    else:
                        # for NON-verbose, bump the minimal running output (still need to flush to see "live"):
                        print('[{0}]'.format(
                            image_counter),
                            end = '', flush=True)

                    # NOTE: after every 10th (or so!) image, workaround a DB connection timeout issue by pinging the DB:
                    # TODO: CONSIDER a corresponding OnPrem option, but until then, try without:
                    ############################################################################
                    #if self.locutus_settings.LOCUTUS_GCP_DICOM_PING_DB_DURING_OPERATIONPOLLS:
                    #    self.LocutusDBconnSession.execute('SELECT 1;')
                    #    if self.locutus_settings.LOCUTUS_VERBOSE:
                    #        print('{0}.copy_local_directory_to_Orthanc(): {1} [poll-loop DB-Ping]'.format(
                    #            CLASS_PRINTNAME,
                    #            msg_prefix),
                    #            flush=True)
                    #    else:
                    #        print(',', end = '', flush=True)
                    ############################################################################

                ###############################################################
                # TEST of the local file type to filter out any non-DICOM before attempting upload,
                # otherwise Orthanc seems to throw an exception AND close its connection,
                # prohibiting the further upload of any remaining files.
                #
                # MIME Types guess_type(), as recommended at:
                # https://stackoverflow.com/questions/43580/how-to-find-the-mime-type-of-a-file-in-python
                #local_file_mimetype = mimetypes.guess_type(local_file)
                # NOTE: the above doesn't seem to recognize a DICOM, and just gives an empty mimetype
                # BUT it might not be looking at the content.
                #
                # TODO: try python-magic, as recommended at:
                # https://stackoverflow.com/questions/10937350/how-to-check-type-of-files-without-extensions-in-python/24433682#24433682
                local_file_magictype = magic.from_file(local_file)
                # be warned that this extra check could slow things down a wee little bit,
                # but that improved confidence is still probably worth any performance trade offs.
                #
                # TODO: and/or, (in addition to the above MIME detection which works),
                # could (also) inspect the file names, to either....
                # (a) end in a suffix .dcm/.DCM, or
                # (b) to match the regexp IM\d+
                #
                ###############################################################

                if local_file_magictype != DICOM_MAGIC_TYPE:
                    # TODO: Q: throw an exception, or just this quiet little WARNING?
                    print('{0}.copy_local_directory_to_Orthanc(): DEBUG WARNING: found non-DICOM file while attempting '\
                                    'to upload instance to Manual DeID QC Orthanc; '\
                                    'expected \'{1}\', but found {2} to be of type: \'{3}\''.format(
                                    CLASS_PRINTNAME,
                                    DICOM_MAGIC_TYPE,
                                    local_file,
                                    local_file_magictype),
                                    flush=True)
                    # NOTE: otherwise, continue attempting to upload others
                    upload_count_error += 1
                else:
                    ##########
                    # TODO: consider an additional try/catch block around all of the following,
                    #
                    fin_dicom = open(local_file, "rb")
                    upload_content = fin_dicom.read()
                    fin_dicom.close()

                    # NOTE: be sure to instantiate a new http_obj for each and every new request;
                    # otherwise may encounter an [Errno 32] Broken pipe:
                    http_obj = httplib2.Http()

                    resp, returned_content = http_obj.request(orthanc_post_url, 'POST',
                                    body = upload_content,
                                    headers = headers)

                    orthanc_upload_instance_retval = resp.status
                    returned_uploaded_instance_json = json.loads(returned_content)
                    returned_uploaded_instance_suburl = 'UNKNOWN_INSTANCE_SUBURL'

                    if 'Path' in returned_uploaded_instance_json:
                        returned_uploaded_instance_suburl = returned_uploaded_instance_json['Path']
                        # and otherwise, see what happens with the resp.status, below
                        # but at least print out a WARNING:
                        # r3m0: TODO: disable these mighty verbose DEBUG INFO messages once all good:
                        """
                        print('{0}.copy_local_directory_to_Orthanc(): DEBUG INFO: upload instance call to Orthanc just '\
                                        'returned a status of: {1}, with returned JSON content of: {2}'.format(
                                        CLASS_PRINTNAME,
                                        orthanc_upload_instance_retval,
                                        returned_uploaded_instance_json),
                                        flush=True)
                        """
                        upload_count_error += 1
                    else:
                        # TODO: Q: throw an exception, or just this quiet little WARNING?
                        print('{0}.copy_local_directory_to_Orthanc(): DEBUG WARNING: no \'Path\' found in uploaded Orthanc instance '\
                                        'returned JSON content of: {1}'.format(
                                        CLASS_PRINTNAME,
                                        returned_uploaded_instance_json),
                                        flush=True)
                    #
                    # TODO: end a try/catch block around all of the above.
                    # but for now, the caller Process_Phase04_DeidentifyAndZip() does wrap this in a try/catch.
                    ##########

                    if orthanc_upload_instance_retval == 200:
                        # Success!
                        upload_count_success += 1
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            # r3m0: TODO: disable these mighty verbose DEBUG INFO messages once all good:
                            """
                            print('{0}.copy_local_directory_to_Orthanc(): DEBUG INFO: the above copy_local_directory_to_Orthanc() call to Orthanc just '\
                                                    'returned a SUCCESS of: {1}, with returned instance sub-URL of {2} from JSON content of: {3}'.format(
                                                    CLASS_PRINTNAME,
                                                    orthanc_upload_instance_retval,
                                                    returned_uploaded_instance_suburl,
                                                    returned_uploaded_instance_json),
                                                    flush=True)
                            """
                        # force a non-error zero for thia 200 response:
                        orthanc_upload_instance_retval = 0

                        # Obtain the study uuid in Orthanc,
                        # as streamlined by just getting the very FIRST image of such an entire study of series of instances!
                        if image_counter == 1:
                            # TODO: consider a safety check of also looking at the study of the LAST image to be uploaded,
                            # but... how to best determine if a file is the last, for this local_file from:
                            #         for local_file in glob.glob(local_path + '/**'):
                            #####
                            # First, have an initial look for ParentStudy in the first returned JSON content,
                            if 'ParentStudy' in returned_uploaded_instance_json:
                                returned_study_suburl = '/studies/{0}'.format(returned_uploaded_instance_json['ParentStudy'])
                                # set both last_returned_* urls:
                                last_returned_api_study_url = returned_study_suburl
                                last_returned_explorer_study_url = '{0}/app/explorer.html#study?uuid={1}'.format(orthanc_url,
                                                                        returned_uploaded_instance_json['ParentStudy'])
                            else:
                                # otherwise, must resort to the more tedious (but readily available) traversal up from the instance
                                # to its ParentSeries and, ultimately, its ParentStudy
                                # via a GET of the instance itself, to find its parent series...
                                (last_returned_api_study_url, last_returned_explorer_study_url) = \
                                        self.get_study_uuid_via_instance_from_Orthanc(headers, orthanc_url, returned_uploaded_instance_suburl)

                                # TODO: consider ensuring that the returned study_urls are not None,
                                # and if so, WARNING and...
                                #   decrementing upload_count_success -= 1
                                #   incrementing upload_count_error += 1
                                #"""
                    else:
                        # ERROR: FAILURE uploading (Is it NOT a DICOM file? invalid credentials?)
                        upload_count_error += 1
                        # TODO: save the previous Orthanc call's stderr in case NOT in VERBOSE mode and needing it for this ERROR:
                        print('{0}.copy_local_directory_to_Orthanc(): DEBUG INFO: the above copy_local_directory_to_Orthanc() call to Orthanc just '\
                                                    'returned a non-SUCCESS of: {1}, with returned content of: {2}'.format(
                                                    CLASS_PRINTNAME,
                                                    orthanc_upload_instance_retval,
                                                    returned_content),
                                                    flush=True)
                        # TODO: parse and strip out any errors from the above orthanc_upload_instance_retval, to determine orthanc_err!!!
                        # NOTE: normally orthanc_upload_instance_retval==None
                        # (likely no return value at all, and needing an internal try/except block),
                        # noting that this entire Phase05 method is usually wrapped in a try/except block.
                        # NOTE: retain the http status:
                        orthanc_err = orthanc_upload_instance_retval
                        # NOTE: keep on trying next files, though, tallying up into: upload_count_success & upload_count_error
            elif os.path.isdir(local_file):
                # NOTE: here is the missing recursion: :-)
                sublocal_path = os.path.join(local_path, local_file)
                (sub_orthanc_err, last_returned_api_study_url, last_returned_explorer_study_url) = \
                        self.copy_local_directory_to_Orthanc(top_local_path, sublocal_path, orthanc_url, orthanc_user, orthanc_password, image_counter)

                # keeping return orthanc_err as False unless ANY subdirs return True:
                if sub_orthanc_err:
                    orthanc_err = sub_orthanc_err

        if not self.locutus_settings.LOCUTUS_VERBOSE:
            # for NON-verbose, terminate (at least this level of recursion)
            # & flush the single-line running polling output
            # (which may result in a few levels of flushing):
            print('', flush=True)
        nowtime = datetime.datetime.utcnow()
        print('@ {0} UTC: UPLOADED {1} DICOM image file(s) at sub-level == {2}'.format(
                    nowtime,
                    image_counter,
                    local_path.replace(top_local_path,'.')), flush=True)

        # TODO: consider also looking at &/or returning the following counters:
        #   upload_count_success
        #   upload_count_error

        return (orthanc_err, last_returned_api_study_url, last_returned_explorer_study_url)
        # end of copy_local_directory_to_Orthanc()


    def generate_manifest_status_for_previous_processed_used(self,
                                                            prev_subject_id,
                                                            prev_object_info_01,
                                                            prev_object_info_02,
                                                            prev_object_info_03,
                                                            curr_subject_id,
                                                            curr_object_info_01,
                                                            curr_object_info_02,
                                                            curr_object_info_03,
                                                            int_cfgs_previous_msg,
                                                            int_cfgs_active_msg,
                                                            preretired_prev_manifest_attributes):
        # returns the whynot_manifest_status string:
        ###########################################################
        # NOTE: for each PREVIOUS_PROCESSING_USED_* possibility, either of 2, for Phase03....
        # This is for both....
        #   the _PRERETIRED_SO_NOW_REPROCESSING_
        #   and _PRERETIRE_OR_PREDELETE_TO_REPROCESS_
        # versions
        ###########################################################
        whynot_manifest_status_retstr = 'PREVIOUS_PROCESSING_USED_'


        # TODO: create a function for a single such comparison place of all attributes:
        # TODO: be sure to update for an object_info_04 if/when used from input manifest:
        same_manifest_attributes = False
        if (curr_subject_id == prev_subject_id) \
            and (curr_object_info_01 == prev_object_info_01) \
            and (curr_object_info_02 == prev_object_info_02) \
            and (curr_object_info_03 == prev_object_info_03):
                same_manifest_attributes = True


        if same_manifest_attributes:
            # PREVIOUS for only differing cfgs, don't bother including the manifest
            whynot_manifest_status_retstr +='[{0}]'.format(int_cfgs_previous_msg)
        else:
            # PREVIOUS for different manifest attributes (and possibly the cfgs as well)
            whynot_manifest_status_retstr +='[{0}/{1}/{2}/{3}{4}]'.format(prev_subject_id, prev_object_info_01,
                                                            prev_object_info_02, prev_object_info_03,
                                                            int_cfgs_previous_msg)

        if preretired_prev_manifest_attributes:
            whynot_manifest_status_retstr += '_PRERETIRED_SO_NOW_REPROCESSING_'
        else:
            whynot_manifest_status_retstr += '_PRERETIRE_OR_PREDELETE_TO_REPROCESS_'

        if same_manifest_attributes:
            # CURRENT for only differing cfgs, don't bother including the manifest
            whynot_manifest_status_retstr +='[{0}]'.format(int_cfgs_active_msg)
        else:
            # CURRENT for different manifest attributes (and possibly the cfgs as well)
            whynot_manifest_status_retstr +='[{0}/{1}/{2}/{3}{4}]'.format(curr_subject_id, curr_object_info_01,
                                                            curr_object_info_02, curr_object_info_03,
                                                            int_cfgs_active_msg)
        return whynot_manifest_status_retstr


    def generate_create_status_fields_for_internal_configs(self,
                                                        int_cfgs):
        # returns a LIST of 'status_field TEXT' strings,
        #   for use at CREATE TABLE LOCUTUS_ONPREM_DICOM_STATUS_TABLE
        # NOTE: the joining of these items (and any optionally preceding commas)
        #   are left to the calling code.
        #####################################
        # TODO: eventually add further support for user-configurable overrides.
        #####################################
        # TODO: move this into main_locutus.py, for all modules to use, given the int_cfgs and active gcp/onprem tablename
        #########################

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('generate_create_status_fields_for_internal_configs(): welcome.', flush=True)

        create_fields = []
        status_field_data_type = 'TEXT'

        for idx_cfg, cfg in enumerate(int_cfgs):
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('------------------------------------------------', flush=True)

            # NOTE: might want to also add try/catch blocks around the below cfg[] references
            # (such as cfg['config_type'])in case any keys are missing,
            # BUT these are internally defined, so no worries:

            create_fields_str = '{0} {1}'.format(cfg['status_field'], status_field_data_type)
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('generate_create_status_fields_for_internal_configs(): adding internal cfg update field #{0} : {1}'.format(
                        (idx_cfg+1), create_fields_str), flush=True)

            create_fields.append(create_fields_str)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('------------------------------------------------', flush=True)
            print('generate_create_status_fields_for_internal_configs(): bye.', flush=True)
        return create_fields


    def generate_update_status_fields_for_internal_configs(self,
                                                        int_cfgs,
                                                        curr_at_phase=0,
                                                        reset_others=False):
        # returns a LIST of 'status_field=\'config_version\'' strings,
        #   for use at UPDATE LOCUTUS_ONPREM_DICOM_STATUS_TABLE
        # Optional parameters include:
        #       * curr_at_phase [default=0]: to reference only the corresponding at_phase field(s) in each dictionary
        #               NOTE: curr_at_phase default of 0 to include ALL configs, regardless of their at_phase
        #       * reset_others [default=False]
        #               NOTE: reset_others is only applicable with curr_at_phase != 0 (typically only for Phase03)
        #               and will generate a reset to NULL for all other at_phase configs
        # NOTE: the joining of these items (and any optionally preceding commas)
        #   are left to the calling code.
        #####################################
        # TODO: eventually add further support for user-configurable overrides.
        #####################################
        # TODO: move this into main_locutus.py, for all modules to use, given the int_cfgs and active gcp/onprem tablename
        #########################

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('generate_update_status_fields_for_internal_configs(curr_at_phase={0}, reset_others={1}): welcome.'.format(
                            curr_at_phase, reset_others), flush=True)

        update_fields = []

        for idx_cfg, cfg in enumerate(int_cfgs):
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('------------------------------------------------', flush=True)

            # NOTE: might want to also add try/catch blocks around the below cfg[] references
            # (such as cfg['config_type'])in case any keys are missing,
            # BUT these are internally defined, so no worries:

            ######
            # TODO:
            # Add checks any module-specific cfg['config_type'] that support user-configurable overrides,
            #   and would therefore not automatically utilize the default.
            # For example, although the ONPREM DICOM module does not yet expose its dicom_anon configurations,
            # consider GCP DICOM modules's user-configurable options for:
            #   * gcp_image_mode, and,
            #   * (eventually) gcp_keeplist_ver
            ######
            # For all other cases, though, merely use the RAM-active version, appending its key & value:
            append_this_cfg = False
            append_msg = ''

            if (curr_at_phase == 0) or (curr_at_phase == cfg['at_phase']):
                # add update field for this current phase:
                append_this_cfg = True
                append_msg = "ADDING"
                update_field_key_value_str = '{0}=\'{1}\''.format(cfg['status_field'], cfg['config_version'])
            elif (reset_others) and (curr_at_phase > 0) and (curr_at_phase != cfg['at_phase']):
                # add reset field for this other phase:
                append_this_cfg = True
                append_msg = "RESETTING"
                update_field_key_value_str = '{0}=NULL'.format(cfg['status_field'])

            if append_this_cfg:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('generate_update_status_fields_for_internal_configs(curr_at_phase={0}, reset_others={1}): '\
                            '{2} internal cfg update field #{3} : {4} at_phase={5}'.format(
                            curr_at_phase, reset_others,
                            append_msg,
                            (idx_cfg+1), update_field_key_value_str, cfg['at_phase']), flush=True)
                update_fields.append(update_field_key_value_str)
            else:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('generate_update_status_fields_for_internal_configs(curr_at_phase={0}, reset_others={1}): '\
                            'IGNORING internal cfg update field #{2} : NOT ADDING type {3} at_phase={4} (not currently applicable)'.format(
                            curr_at_phase, reset_others,
                            (idx_cfg+1), cfg['config_type'], cfg['at_phase']), flush=True)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('------------------------------------------------', flush=True)
            print('generate_update_status_fields_for_internal_configs(): bye.', flush=True)
        return update_fields


    def compare_active_internal_configs_vs_processed(self,
                                                    curr_acc_num,
                                                    curr_uuid,
                                                    curr_phase_processed,
                                                    int_cfgs,
                                                    curr_at_phase=0,
                                                    check_prev=False):
        # returns (int_cfgs_all_match, int_cfgs_active_mismatches, int_cfgs_previous_mismatches, max_matched_phase)
        #
        # sample usage:
        # (int_cfgs_match, int_cfgs_active_mismatches, int_cfgs_previous_mismatches, max_matched_phase) =
        #           self.compare_active_internal_configs_vs_processed(
        #               curr_accession_num,
        #               curr_uuid,
        #               curr_phase_processed,
        #               LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES)
        # Optional parameters include:
        #       * curr_at_phase [default=0]: to compare only against the corresponding cfg['at_phase'] field(s) in each dictionary
        #               NOTE: curr_at_phase default of 0 to compare against ALL configs, regardless of their cfg['at_phase']
        #               BEWARE: a partially processed accession may still have NULLS,
        #               and might therefore not match a full curr_at_phase=0 comparison for a partially processed accession
        #               (just limit such a partial processed comparison cuur_at to its phase_processed, with check_prev=True).
        #       * check_prev=False
        #               NOTE: check_prev is only applicable with curr_at_phase != 0
        #               and will ensure that all previous at_phase configs also match
        #               NOTE: by default, this will ignore any cfgs with cfg['at_phase'] > curr_at_phase
        #               NOTE: if not check_prev, max_matched_phase can only be:
        #                   == curr_at_phase (if matching all such cfgs)
        #                   OR == MIN_PROCESSING_PHASE (if mismatch with any such cfgs)
        ######
        # NOTE: see also the LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED setting
        # in order to continue processing even when such configuration changes are encountered.
        #######
        # NOTE: as inspired by set_active_internal_configs(),
        # This compares against the particular fields stored with the session's DB status.
        # This method is expected to only be used for those sessions which have already been processed,
        # as only PROCESSED accessions will have their internal config saved, much like their de-id target paths.
        #####################################
        # TODO: return not just a True/False for matching but ALSO a previous vs current version &/or date, perhaps?
        # and separately, for the PREVIOUS_PROCESSING_USED to split much like it does with the manifest attributes.
        #####################################
        # TODO: move this into main_locutus.py, for all modules to use, given the int_cfgs and active gcp/onprem tablename
        #########################
        # NOTE: comparison refers to the internally defined int_cfgs as RAM or RAM-ACTIVE versions
        #   as -vs- the ACTIVE-DB version.
        #########################

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('compare_active_internal_configs_vs_processed(curr_at_phase={0}, check_prev={1}): welcome.'.format(
                            curr_at_phase, check_prev), flush=True)

        # NOTE: working down from the curr_at_phase of this potentially partially processed accession,
        # through any mismatches, down to a minimum "max matched phase" of MIN_PROCESSING_PHASE
        # to help inform subsequent continued/re-processing attempts:
        max_matched_phase = curr_at_phase
        if max_matched_phase == 0:
            # i.e., if comparing against all phases, start at the top,
            max_matched_phase = MAX_PROCESSING_PHASE
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('compare_active_internal_configs_vs_processed(): '\
                'STARTING with max_matched_phase at {0}'.format(
                max_matched_phase), flush=True)

        active_int_cfgs=[]
        active_int_cfgs_mismatches=[]
        previously_processed_int_cfgs=[]
        previously_processed_int_cfgs_mismatches=[]
        int_cfgs_all_match=True
        for idx_cfg, cfg in enumerate(int_cfgs):
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('------------------------------------------------', flush=True)
            int_cfg_curr_match=True
            active_cfg_type_found = False
            active_cfg_type_match = False
            num_active_cfg_type = 0

            # NOTE: might want to also add try/catch blocks around the below cfg[] references
            # (such as cfg['config_type'])in case any keys are missing,
            # BUT these are internally defined, so no worries:

            check_this_cfg = False

            if (curr_at_phase == 0) or (curr_at_phase == cfg['at_phase']):
                # check this current phase:
                check_this_cfg = True
            elif (check_prev) and (curr_at_phase > 0) and (curr_at_phase > cfg['at_phase']):
                # check this previous phase:
                check_this_cfg = True


            if not check_this_cfg:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('compare_active_internal_configs_vs_processed(): NOT comparing against internal cfg #{0} : '\
                            'type=\'{1}\' at_phase={2} (because curr_at_phase={3}, check_prev={4}).'.format(
                            (idx_cfg+1), cfg['config_type'], cfg['at_phase'],
                            curr_at_phase, check_prev), flush=True)
            else:
                # check_this_cfg:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('compare_active_internal_configs_vs_processed(): comparing internal cfg #{0} : {1}'.format((idx_cfg+1), cfg), flush=True)
                    print('compare_active_internal_configs_vs_processed(): cfg #{0} : type=\'{1}\', ver=\'{2}\', at_phase={3}, status_field=\'{4}\''.format(
                            (idx_cfg+1), cfg['config_type'], cfg['config_version'], cfg['at_phase'], cfg['status_field']), flush=True)

                # TODO: wrap the following in a try/catch block to more robustly handle and
                # provide WARNINGs of missing status_fields not yet manually added into the DB,
                # as these would currently result in errors such as:
                # sqlalchemy.exc.ProgrammingError: (psycopg2.ProgrammingError) column "cfg_test_col" does not exist
                # LINE 1: SELECT cfg_test_col FROM onprem_dicom_status WHERE accessio...
                prev_procesed_results = self.LocutusDBconnSession.execute('SELECT {0} '\
                                                    'FROM {1} '\
                                                    'WHERE accession_num=\'{2}\' '\
                                                    '    AND active '\
                                                    'AND uuid = \'{3}\' '\
                                                    ';'.format(
                                                    cfg['status_field'],
                                                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                    curr_acc_num,
                                                    curr_uuid))

                prev_processed_row = prev_procesed_results.fetchone()

                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('compare_active_internal_configs_vs_processed(): found prev_procesed_row[\'{0}\']=\'{1}\''.format(
                                cfg['status_field'], prev_processed_row[0]), flush=True)

                if cfg['config_version'] != prev_processed_row[0]:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('compare_active_internal_configs_vs_processed(): '\
                                'WARNING: compare_active_internal_configs_vs_processed: '\
                                'found MISMATCH in active-vs-previous for type \'{0}\', '\
                                'active version (\'{1}\') != previously processed version (\'{2}\').'.format(
                                cfg['config_type'], cfg['config_version'], prev_processed_row[0]), flush=True)
                    # Set the current cfg as a mismatch regardless:
                    int_cfg_curr_match = False
                    # And decrease the "max matched phase" to one phase before this mismatch's at_phase,
                    # IF that is earlier than the current "max matched phase":
                    if (cfg['at_phase']-1) < max_matched_phase:
                        last_max_matched_phase = max_matched_phase
                        max_matched_phase = cfg['at_phase']-1
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('compare_active_internal_configs_vs_processed(): '\
                                'REDUCING max_matched_phase from {0} down to {1}'.format(
                                last_max_matched_phase, max_matched_phase), flush=True)
                    # But allow the overall ALL int_cfgs_all_match to bypass if not > MIN_PROCESSING_PHASE:
                    if curr_phase_processed <= MIN_PROCESSING_PHASE:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('compare_active_internal_configs_vs_processed(): '\
                                'Since this accession was not even partially previously processed (still at minimum '\
                                'phase {0}), and Locutus does not monitor these until at least PARTIALLY PROCESSED, '\
                                'treating as a match, for now)'.format(
                                curr_phase_processed), flush=True)
                    else:
                        int_cfgs_all_match = False
                        curr_processed_partiality = "COMPLETELY"
                        if curr_phase_processed < MAX_PROCESSING_PHASE:
                            curr_processed_partiality = "PARTIALLY"
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('compare_active_internal_configs_vs_processed(): '\
                                'since this accession was previously {0} PROCESSED to '\
                                'phase {1}, treating this as a mismatch.'.format(
                                curr_processed_partiality,
                                curr_phase_processed), flush=True)
                else:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('compare_active_internal_configs_vs_processed(): '\
                                'found a MATCH in active-vs-previous for type \'{0}\', '\
                                'both versions == \'{1}\'.'.format(
                                cfg['config_type'], cfg['config_version']), flush=True)

                # whether or not they match, include them both in the build-up for return:
                active_int_cfgs.append('{0}={1}'.format(cfg['config_type'], cfg['config_version']))
                previously_processed_int_cfgs.append('{0}={1}'.format(cfg['config_type'], prev_processed_row[0]))
                # but only when there is a mismatch, include them both in a minimal mismatch build-up for return:
                if not int_cfg_curr_match:
                    active_int_cfgs_mismatches.append('{0}={1}'.format(cfg['config_type'], cfg['config_version']))
                    previously_processed_int_cfgs_mismatches.append('{0}={1}'.format(cfg['config_type'], prev_processed_row[0]))

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('------------------------------------------------', flush=True)
            print('compare_active_internal_configs_vs_processed(): returning max_matched_phase={0}.'.format(max_matched_phase), flush=True)
            print('compare_active_internal_configs_vs_processed(): bye.', flush=True)
        return (int_cfgs_all_match, active_int_cfgs_mismatches, previously_processed_int_cfgs_mismatches, max_matched_phase)


    def set_active_internal_configs(self,
                                    int_cfgs):
        # compare the internally known internal configuations against those in the internal configs table,
        # setting the most active within the DB.
        #########################
        # TODO: move this into main_locutus.py, for all modules to use, given the int_cfgs and active gcp/onprem tablename
        #########################
        # NOTE: the LOCUTUS_*DICOM_INT_CFGS_TABLE is assumed to have already been created by this point,
        #   but might be empty, or perhaps already populated with active (or previously active) cfgs.
        #   Compare against those internally defined, ensure that they are the active,
        #   and de-activate any previously active settings with an activation date prior to the internal.
        #########################
        # NOTE: comparison refers to the internally defined int_cfgs as RAM or RAM-ACTIVE versions
        #   as -vs- the ACTIVE-DB version.
        #########################

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('set_active_internal_configs(): welcome.', flush=True)

        for idx_cfg, cfg in enumerate(int_cfgs):
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('------------------------------------------------', flush=True)
            active_cfg_type_found = False
            active_cfg_type_match = False
            num_active_cfg_type = 0

            # NOTE: might want to also add try/catch blocks around the below cfg[] references
            # (such as cfg['config_type'])in case any keys are missing,
            # BUT these are internally defined, so no worries:

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('set_active_internal_configs(): comparing internal cfg #{0} : {1}'.format(
                        (idx_cfg+1), cfg), flush=True)

            # NOTE: date_activated fields expected to be of the format 'YYYY-MM-DD' for comparison:
            cfg_date_ram = datetime.datetime.strptime(str(cfg['date_activated']), '%Y-%m-%d')

            # 1) CHECK if there is already an ACTIVE-DB cfg matching this RAM-based type:
            cfg_results = self.LocutusDBconnSession.execute('SELECT config_type, config_version, '\
                                                        'config_desc, date_activated, '\
                                                        'at_phase, status_field '\
                                                        'FROM {0} WHERE config_type=\'{1}\' '\
                                                        '    AND active ;'.format(
                                                        LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                                        cfg['config_type']))

            for idx_row, row in enumerate(cfg_results):
                active_cfg_type_found = True
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('----------', flush=True)
                    print('set_active_internal_configs(): for config_type=\'{0}\', '\
                            'ACTIVE-DB config_version = \'{1}\', DB config_desc = \'{2}\', DB date_activated = {3}'.format(
                                                row['config_type'],
                                                row['config_version'],
                                                row['config_desc'],
                                                row['date_activated']),
                                                flush=True)
                    print('set_active_internal_configs(): for config_type=\'{0}\', '\
                            'RAM-ACTIVE config_version = \'{1}\', DB config_version = \'{2}\', is match = {3}'.format(
                                                cfg['config_type'],
                                                cfg['config_version'],
                                                row['config_version'],
                                                (cfg['config_version']==row['config_version'])),
                                                flush=True)
                    print('set_active_internal_configs(): for config_type=\'{0}\', '\
                            'RAM-ACTIVE config_desc = \'{1}\', DB config_desc = \'{2}\', is match = {3}'.format(
                                                cfg['config_type'],
                                                cfg['config_desc'],
                                                row['config_desc'],
                                                (cfg['config_desc']==row['config_desc'])),
                                                flush=True)
                    print('set_active_internal_configs(): for config_type=\'{0}\', '\
                            'RAM-ACTIVE date_activated = \'{1}\', DB date_activated = \'{2}\', is match = {3}'.format(
                                                cfg['config_type'],
                                                cfg['date_activated'],
                                                row['date_activated'],
                                                (str(row['date_activated'])==str(cfg['date_activated']))),
                                                flush=True)

                # 2) Q: DOES the RAM-ACTIVE version match the same version and date as the ACTIVE-DB version?
                # Compare each of the fields, and if all fields match, then leave it as is;
                # When all fields do NOT match, check for most recently activated,
                #   IF RAM-active is the most recently activated, update the DB with it;
                #   ELSE if its DB version's date is more recent than this RAM-cfg,
                #       THEN perhaps emit a WARNING?
                #       (while stil supporting such manual updates)
                if ((row['config_type'] == cfg['config_type']) and \
                    (row['config_version'] == cfg['config_version']) and \
                    (row['config_desc'] == cfg['config_desc']) and \
                    (str(row['date_activated']) == str(cfg['date_activated']))):
                    # RAM-ACTIVE and ACTIVE-DB versions match:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('set_active_internal_configs(): for config_type=\'{0}\', '\
                                'FOUND the RAM active one matches the DB active one, yay'.format(
                                    cfg['config_type']), flush=True)
                    active_cfg_type_match = True
                    num_active_cfg_type += 1
                else:
                    # RAM-ACTIVE and ACTIVE-DB versions do NOT match:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('set_active_internal_configs(): for config_type=\'{0}\', BEWARE - '\
                            'the RAM active one does NOT match this DB active one, okay...'.format(
                                cfg['config_type']), flush=True)
                    # BUT: is the ACTIVE-DB one more recent that this RAM-active one?
                    # NOTE: date_activated fields expected to be of the format 'YYYY-MM-DD' for comparison:
                    cfg_date_db = datetime.datetime.strptime(str(row['date_activated']), '%Y-%m-%d')
                    if cfg_date_db <= cfg_date_ram:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('set_active_internal_configs(): for config_type=\'{0}\', '\
                                'DB activation date ({1}) <=  RAM activation date ({2}).  '\
                                'SO, setting DB cfg active to false'.format(
                                    cfg['config_type'], cfg_date_db, cfg_date_ram), flush=True)
                        # 3) IF NOT (i.e., RAM and DB config do NOT match the same version and date)
                        # 3b)   AND the DB version is earlier than the RAM-active...
                        #       THEN de-activate the old DB one(s) matching that config_type.
                        ######
                        # set this DB cfg to inactive, for even if a matching active is also found,
                        # since we should only have the one most recent, from RAM-ACTIVE.
                        # and since de-activating this old ACTIVE-DB one, no need to increment num_active_cfg_type.
                        # NOTE: the latest RAM version will be INSERTed further below,
                        #   at the outer "if not active_cfg_type_match"
                        ######
                        self.LocutusDBconnSession.execute('UPDATE {0} '\
                                    'SET active=False '\
                                    'WHERE config_type=\'{1}\' '\
                                    'AND config_version=\'{2}\' '\
                                    '    AND active ;'.format(
                                    LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                    row['config_type'],
                                    row['config_version']))
                    else:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('set_active_internal_configs(): for config_type=\'{0}\', '\
                                    'WARNING: DB activation date ({1}) >  RAM activation date ({2}).  '\
                                    'SO, LEAVING DB AS ACTIVE. '\
                                    '(Please update the code to match, thank you!)'.format(
                                        cfg['config_type'], cfg_date_db, cfg_date_ram ), flush=True)
                            print('set_active_internal_configs(): AIMING to set RAM-ACTIVE '\
                                    'to the version in ACTIVE-DB, fingers crossed!', flush=True)
                        # activate it in RAM:
                        cfg['config_version'] = row['config_version']
                        cfg['config_desc'] = row['config_desc']
                        cfg['date_activated'] = row['date_activated']

                        # effectively setting the newer DB cfg as a match:
                        active_cfg_type_match = True
                        # and since leaving this as active, do increment num_active_cfg_type
                        num_active_cfg_type += 1

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('----------', flush=True)

            if not active_cfg_type_match:
                # 4) IF RAM and DB config do NOT match the same version and date
                #   (AND a DB version did not exist that was later than the RAM-active...
                #       THEN add this RAM-active config as a new active one into the DB:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('set_active_internal_configs(): updating config_type=\'{0}\', '\
                        'adding & activating new version=\'{1}\'.'.format(
                        cfg['config_type'], cfg['config_version']), flush=True)
                # go ahead and insert the new cfg for this config_type:
                if not self.locutus_settings.LOCUTUS_TEST:
                    num_active_cfg_type += 1
                    # NOTE: to ensure no duplicates added into the INT CFGS table, especially possibly with manual testing,
                    # 1st CHECK if such a config_type/config_version already exists, as well as its current active status
                    # and if so, merely ensure that the active status is True, WARNING if for some reason it wasn't already:
                    #####
                    # 1) CHECK if there is already a cfg (active or not!) matching this RAM-based type:
                    new_cfg_results = self.LocutusDBconnSession.execute('SELECT active FROM {0} '\
                                                        'WHERE config_type=\'{1}\' '\
                                                        'AND config_version=\'{2}\' '\
                                                        'AND config_desc=\'{3}\' '\
                                                        'AND date_activated=\'{4}\' '\
                                                        'AND at_phase=\'{5}\' '\
                                                        'AND status_field=\'{6}\' '\
                                                        ' '.format(
                                                            LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                                            cfg['config_type'],
                                                            cfg['config_version'],
                                                            cfg['config_desc'],
                                                            cfg['date_activated'],
                                                            cfg['at_phase'],
                                                            cfg['status_field']
                                                        ))

                    # NOTE: rather than doing a separate SELECT COUNT() on the above,
                    # enumerate through the needed result to perform the count check,
                    # and using the last such enumerated new_cfg_row thereafter:
                    new_cfg_numrows = 0
                    staged_change_row = None
                    for new_cfg_rownum, new_cfg_row in enumerate(new_cfg_results):
                        new_cfg_numrows += 1

                    if new_cfg_numrows >= 1:
                        print('set_active_internal_configs(): WARNING: found an already existing '\
                                'workspace INT CFG in \'{0}\': '\
                                'config_type=\'{1}\' ({2} of em), w/ version=\'{3}\, '\
                                'desc=\'{4}\', date=\'{5}\', '\
                                'at_phase=\'{6}\', & status_field=\'{7}\', AND '\
                                'active=\'{8}\'.'.format(
                                    LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                    cfg['config_type'], new_cfg_numrows, cfg['config_version'],
                                    cfg['config_desc'], cfg['date_activated'],
                                    cfg['at_phase'], cfg['status_field'],
                                    new_cfg_row['active'] ), flush=True)
                        if new_cfg_numrows > 1:
                            print('set_active_internal_configs(): ERROR: more than 1 row ({0} in total!) '\
                                'workspace INT CFGs found in \'{1}\': '\
                                'config_type=\'{2}\', w/ version=\'{3}\, etc.; '\
                                'may need to MANUALLY RESOLVE this.'.format(
                                    new_cfg_numrows,
                                    LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                    cfg['config_type'],
                                    cfg['config_version'])
                                )

                        if new_cfg_row['active']:
                            # TODO: emit a WARNING of the active one already found to match, wacky!
                            # NOTE: even if not VERBOSE
                            print('set_active_internal_configs(): WARNING: found an already existing '\
                                'DB-ACTIVE workspace INT CFG in \'{0}\': '\
                                'config_type=\'{1}\', w/ version=\'{2}\, '\
                                'desc=\'{3}\', date=\'{4}\', '\
                                'at_phase=\'{5}\', & status_field=\'{6}\'; '\
                                'leaving it as ACTIVE (and unsure of how this WARNING came to be!)'.format(
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                cfg['config_type'], cfg['config_version'],
                                cfg['config_desc'], cfg['date_activated'],
                                cfg['at_phase'], cfg['status_field'] ), flush=True)
                        else:
                            # update this existing config to BE active
                            print('set_active_internal_configs(): INFO: found an already existing '\
                                '(but NOT YET active!) workspace INT CFG in \'{0}\': '\
                                'config_type=\'{1}\', w/ version=\'{2}\, '\
                                'desc=\'{3}\', date=\'{4}\', '\
                                'at_phase=\'{5}\', & status_field=\'{6}\'; '\
                                'UPDATING it to ACTIVE (so as to not redundantly re-insert)'.format(
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                cfg['config_type'], cfg['config_version'],
                                cfg['config_desc'], cfg['date_activated'],
                                cfg['at_phase'], cfg['status_field'] ), flush=True)
                            # pre-NOTE: that the older DB-active INT CFG has already been set to inactive above, at.....
                            ######
                            # set this DB cfg to inactive, for even if a matching active is also found,
                            # since we should only have the one most recent, from RAM-ACTIVE.
                            # and since de-activating this old ACTIVE-DB one, no need to increment num_active_cfg_type.
                            ######
                            # post-NOTE: so here, finally, the latest RAM version will be INSERTed further below,
                            #   within this "if not active_cfg_type_match"
                            ######
                            self.LocutusDBconnSession.execute('UPDATE {0} '\
                                                        'SET active=True '\
                                                        'WHERE config_type=\'{1}\' '\
                                                        'AND config_version=\'{2}\' '\
                                                        'AND config_desc=\'{3}\' '\
                                                        'AND date_activated=\'{4}\' '\
                                                        'AND at_phase=\'{5}\' '\
                                                        'AND status_field=\'{6}\' '\
                                                        ';'.format(
                                                            LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                                            cfg['config_type'],
                                                            cfg['config_version'],
                                                            cfg['config_desc'],
                                                            cfg['date_activated'],
                                                            cfg['at_phase'],
                                                            cfg['status_field']
                                                        ))
                    else:
                        # no existing INT CFG found that matches, go ahead and INSERT it, hardcoding to active=TRUE:
                        self.LocutusDBconnSession.execute('INSERT INTO {0} ('\
                                            'config_type, '\
                                            'config_version, '\
                                            'config_desc, '\
                                            'date_activated, '\
                                            'active, '\
                                            'at_phase, '\
                                            'status_field) '\
                                            'VALUES ('\
                                            '\'{1}\', '\
                                            '\'{2}\', '\
                                            '\'{3}\', '\
                                            '\'{4}\', '\
                                            'True, '\
                                            '\'{5}\', '\
                                            '\'{6}\') ;'.format(
                                            LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE,
                                            cfg['config_type'],
                                            cfg['config_version'],
                                            cfg['config_desc'],
                                            cfg['date_activated'],
                                            cfg['at_phase'],
                                            cfg['status_field']))

            if num_active_cfg_type == 1:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('set_active_internal_configs(): Congratulations: DB now has {0} '\
                        'and only {0} active version of config_type=\'{1}\': '\
                        'version=\'{2}\', description=\'{3}\', activation date={4}'.format(
                        num_active_cfg_type,
                        cfg['config_type'],
                        cfg['config_version'],
                        cfg['config_desc'],
                        cfg['date_activated']
                        ), flush=True)
            else:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('set_active_internal_configs(): WARNING: DB currently has {0} '\
                        'active versions of config_type=\'{1}\'! '\
                        'Please manually resolve these, each more recent than the current '\
                        'RAM cfg activation date of {2}'.format(
                        num_active_cfg_type,
                        cfg['config_type'],
                        cfg['date_activated']
                        ), flush=True)

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('------------------------------------------------', flush=True)
            print('set_active_internal_configs(): bye.', flush=True)
        return


    def remove_trailing_zeros(self,
                            in_val):
        # TODO: move this into main_locutus.py, for all to use.
        # NOTE: sqlalchemy is loading a numeric(15,3) [UPDATED to (18,3)] as type Decimal,
        # so must support that type as well as strings.
        #
        # helper method expecting a string of the form "xxxx.yyy",
        # to return the same in_str if not matching that form,
        # or if the trailing "yyy" has any non-zeros;
        # otherwise, if only zeros beyond the decimal, merely return the "xxxx"
        #
        if isinstance(in_val, str):
            # String support:
            tmp_str = in_val
            while (tmp_str.find('.') > -1) and (tmp_str[-1] == "0"):
                tmp_str = tmp_str[:-1]
            if tmp_str[-1] == ".":
                tmp_str = tmp_str[:-1]
            return tmp_str
        elif isinstance(in_val, int):
            # Integer support:
            return in_val
        elif isinstance(in_val, float) or isinstance(in_val, Decimal):
            # Floating point / Decimal support:
            if (in_val == int(in_val)):
                # i.e., int-truncated version is the same, just return it:
                return int(in_val)
            else:
                # WARNING: Decimal may still have trailing zeros IF a digit beyond decimal place,
                # e.g., 12345.600 will still return as 12345.600, oh well.
                return in_val
        else:
            # unknown/unsupported type, just return as is:
            return in_val


    def rollback_accession_status_phase_processed(self,
                                        accession_num,
                                        last_phase_processed):
        # helper method to update ONLY the corresponding STATUS record's phase_processed for an accession_num
        # to roll it back to the last matching config phase,
        # pending results from compare_active_internal_configs_vs_processed()
        self.LocutusDBconnSession.execute('UPDATE {0} '\
                                        'SET phase_processed={1} '\
                                        'WHERE accession_num=\'{2}\' '\
                                        '    AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        last_phase_processed,
                                        accession_num))


    def preretire_change_status_only(self, change_seq_id):
        # helper method to update and retire ONLY the corresponding STATUS records for a change_seq_id only
        # (for where the corresponding status record in the current workspace is no longer valid,
        # such as following a workspace migration's removal of a zombie change from another workspace...)
        # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
        # and still setting the wacky accession-retirement math for backwards compatibility, while...
        # FOLLOWING the alpha-numeric upgrade of Juneteenth 2025, deprecating accession_num_src as used in antiquated approach of SPLITS for multi-UUIDs....
        self.LocutusDBconnSession.execute('UPDATE {0} '\
                                        'SET change_seq_id=(-1*change_seq_id), '\
                                        '    accession_num=concat(\'-\', accession_num), '\
                                        '    active=False '\
                                        'WHERE change_seq_id = {1} '\
                                        '   AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        change_seq_id))


    def preretire_accession_manifest_only(self,
                                        accession_num):
        # helper method to update and retire ONLY the corresponding MANIFEST records for an accession_num
        # (keeping status record in place for cases where the current status record is valid and possibly mid-processing,
        #  but for which the manifest record has been changed in the meantime and only it needs updating...)
        # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
        # and still setting the wacky accession-retirement math for backwards compatibility, while...
        self.LocutusDBconnSession.execute('UPDATE {0} '\
                                        'SET '\
                                        '    accession_num=concat(\'-\', accession_num), '\
                                        '    active=False '\
                                        'WHERE accession_num=\'{1}\' '\
                                        '   AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                        accession_num))


    def reset_accession_status_for_reprocessing(self, accession_num, new_subject_id, new_object_info_01, new_object_info_02, new_object_info_03):
        # as from module_gcp_dicom.py:
        # helper method to reset the MANIFEST & STATUS records for an accession_num for reprocessing,
        # but only if already previousluy processed, at least partially;
        # allow all PENDING_CHANGE and PENDING_CHANGE_RADIOLOGY_MERGE to remain as they are.
        #################################
        # NOTE: COULD consider setting this method up general purpose enough to bring in both
        # the OnPrem + GCP savvy versions of the attribute updates for either:
        #    GCP: subject_id, object_info
        #    OnPrem: subject_id, object_info_01, object_info_02, object_info_03, etc.
        # (much as done for the Summarizer and its various preloader () preset helper methods)
        # but.... since this is specifically in the GCP module, we can be GCP-specific ;-)
        #################################

        # TODO: add constants up top for all such statuses:
        REPROCESSING_CHANGE_MOMENTARILY="RE-PROCESSING_CHANGE_MOMENTARILY"
        new_manifest_status = REPROCESSING_CHANGE_MOMENTARILY
        new_phase_processed = MIN_PROCESSING_PHASE

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process().reset_accession_status_for_reprocessing(): '\
                'resetting  manifest_status=\'{1}\', phase_processed={2}, '\
                'subject_id=\'{3}\', '\
                'object_info_01=\'{4}\', object_info_02=\'{5}\', object_info_03=\'{6}\', '\
                'for Accession# \'{7}\' ...'.format(
                CLASS_PRINTNAME,
                new_manifest_status, new_phase_processed,
                new_subject_id,
                new_object_info_01, new_object_info_02, new_object_info_03,
                accession_num),
                flush=True)

        self.LocutusDBconnSession.execute('UPDATE {0} '\
                                        'SET manifest_status=\'{1}\', '\
                                        'subject_id=\'{2}\', '\
                                        'object_info_01=\'{3}\', object_info_02=\'{4}\', object_info_03=\'{5}\'  '\
                                        'WHERE accession_num=\'{6}\' '\
                                        '    AND active '\
                                        'AND manifest_status not like \'PENDING%\''.format(
                                        LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                        new_manifest_status,
                                        new_subject_id,
                                        new_object_info_01, new_object_info_02, new_object_info_03,
                                        accession_num))

        self.LocutusDBconnSession.execute('UPDATE {0} '\
                                        'SET phase_processed={1}, '\
                                        'subject_id=\'{2}\', '\
                                        'object_info_01=\'{3}\', object_info_02=\'{4}\', object_info_03=\'{5}\'  '\
                                        'WHERE accession_num=\'{6}\' '\
                                        '    AND active '\
                                        'AND phase_processed >= {1}'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        new_phase_processed,
                                        new_subject_id,
                                        new_object_info_01, new_object_info_02, new_object_info_03,
                                        accession_num))


    def reset_accession_phase_processed(self,
                        accession_num,
                        clear_deid_qc=True):
        # helper method to update ONLY the corresponding STATUS record for an accession_num,
        # and since a full reset back to phase_processed=MIN_PROCESSING_PHASE,
        # NO LONGER must also reset as_change_seq_id to NULL for the initial sweep to find it:
        clear_deid_qc_cols = ''
        if clear_deid_qc:
            # precede with comma since this list of cols is optional and could be empty:
            clear_deid_qc_cols = ',deid_qc_status=NULL, '\
                            'deid_qc_api_study_url=NULL, '\
                            'deid_qc_explorer_study_url=NULL '
        self.LocutusDBconnSession.execute('UPDATE {0} set phase_processed={1} '\
                                        '{2} '\
                                        'WHERE accession_num=\'{3}\' '\
                                        '   AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        MIN_PROCESSING_PHASE,
                                        clear_deid_qc_cols,
                                        accession_num))


    def set_accession_phase_processed(self,
                        accession_num,
                        phase_processed):
        # helper method to update ONLY the corresponding STATUS record for an accession_num:
        if phase_processed <= MIN_PROCESSING_PHASE:
            # be sure to use the proper reset method:
            self.reset_accession_phase_processed(accession_num)
        else:
            self.LocutusDBconnSession.execute('UPDATE {0} set phase_processed={1} '\
                                        'WHERE accession_num=\'{2}\' '\
                                        '    AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        phase_processed,
                                        accession_num))


    def set_accession_phase_processed_to_greatest(self,
                        accession_num,
                        great_test_phase_processed):
        # helper method to update ONLY the corresponding STATUS record for an accession_num:
        self.LocutusDBconnSession.execute('UPDATE {0} set phase_processed=greatest(phase_processed, {1}) '\
                                        'WHERE accession_num=\'{2}\' '\
                                        '    AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        great_test_phase_processed,
                                        accession_num))


    def preretire_accession(self,
                        accession_num):
        # helper method to update and retire ALL corresponding MANIFEST and STATUS records for an accession_num:
        self.preretire_accession_manifest_only(accession_num)
        self.reset_accession_phase_processed(accession_num)


    def predelete_accession(self,
                            accession_num):
        # helper method to update and delete corresponding records for an accession_num:
        self.LocutusDBconnSession.execute('DELETE FROM {0} '\
                                        'WHERE accession_num=\'{1}\' '\
                                        '    AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                        accession_num))
        self.LocutusDBconnSession.execute('UPDATE {0} set phase_processed={1} '\
                                        'WHERE accession_num=\'{2}\' '\
                                        '    AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        MIN_PROCESSING_PHASE,
                                        accession_num))


    def Setup_Input_Manifest(self, run_iteration_num):
        ###################################
        # setup ONPREM-DICOM DICOM input manifest CSV:
        print('{0}.Setup_Input_Manifest(): Opening Locutus ONPREM-DICOM input DICOM Manifest CSV = {1}!'.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV),
                                flush=True)
        self.manifest_infile = open(self.locutus_settings.LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV, 'r')
        self.manifest_reader = csv.reader(self.manifest_infile, delimiter=',')
        # ensure that the input CSV headers are as expected:
        csv_headings_row = next(self.manifest_reader)
        while len(csv_headings_row) == 0 or \
                (not any(field.strip() for field in csv_headings_row)) or \
                csv_headings_row[0] == '' or \
                csv_headings_row[0][0] == '#':
            # read across any blank manifest lines (or lines with only blank fields, OR lines starting with a comment, '#'):
            print('{0}.Setup_Input_Manifest(): ignoring input manifest header line of "{1}"'.format(CLASS_PRINTNAME, csv_headings_row), flush=True)
            if MANIFEST_OUTPUT_INCLUDE_COMMENT_LINES:
                # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                #if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0},{1}'.format(
                            MANIFEST_OUTPUT_PREFIX,
                            ','.join(csv_headings_row)),
                            flush=True)
            csv_headings_row = next(self.manifest_reader)
        print('{0}.Setup_Input_Manifest(): parsing input manifest header line of "{1}"'.format(CLASS_PRINTNAME, csv_headings_row), flush=True)
        # TODO: rather than hardcoding the following CSV field #s, consider creating a dictionary:
        csv_header0 = csv_headings_row[0].strip().upper() if (len(csv_headings_row) >= 1) else ''
        csv_header1 = csv_headings_row[1].strip().upper() if (len(csv_headings_row) >= 2) else ''
        csv_header2 = csv_headings_row[2].strip().upper() if (len(csv_headings_row) >= 3) else ''
        csv_header3 = csv_headings_row[3].strip().upper() if (len(csv_headings_row) >= 4) else ''
        csv_header4 = csv_headings_row[4].strip().upper() if (len(csv_headings_row) >= 5) else ''
        csv_header5 = csv_headings_row[5].strip().upper() if (len(csv_headings_row) >= 6) else ''
        ###
        # NOTE: keeping the final header field as lower case, to highlight it as a data-less version header
        # NOPE! time to open up the case insensitivity for the manifest_ver after all,
        # to support streamlined re-use of output manifests from the Summarizer, AS-IS.
        ###
        #WAS: csv_header6 = csv_headings_row[6].strip().lower() if (len(csv_headings_row) >= 7) else ''
        csv_header6 = csv_headings_row[6].strip().upper() if (len(csv_headings_row) >= 7) else ''
        ###
        if ((len(csv_headings_row) < MANIFEST_NUM_HEADERS) or
            (csv_header0 != MANIFEST_HEADER_SUBJECT_ID) or
            (csv_header1 != MANIFEST_HEADER_OBJECT_INFO_01) or
            (csv_header2 != MANIFEST_HEADER_OBJECT_INFO_02) or
            (csv_header3 != MANIFEST_HEADER_OBJECT_INFO_03) or
            (csv_header4 != MANIFEST_HEADER_ACCESSION_NUM) or
            (csv_header5 != MANIFEST_HEADER_DEID_QC_STATUS) or
            (csv_header6 != MANIFEST_HEADER_MANIFEST_VER.upper())):
            print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV headers (\'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\') '\
                                'for {8} do NOT match those expected (\'{9}\', \'{10}\', \'{11}\', \'{12}\', \'{13}\', \'{14}\', \'{15}\')!'.format(
                                CLASS_PRINTNAME,
                                csv_header0,
                                csv_header1,
                                csv_header2,
                                csv_header3,
                                csv_header4,
                                csv_header5,
                                csv_header6,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV,
                                MANIFEST_HEADER_SUBJECT_ID,
                                MANIFEST_HEADER_OBJECT_INFO_01,
                                MANIFEST_HEADER_OBJECT_INFO_02,
                                MANIFEST_HEADER_OBJECT_INFO_03,
                                MANIFEST_HEADER_ACCESSION_NUM,
                                MANIFEST_HEADER_DEID_QC_STATUS,
                                MANIFEST_HEADER_MANIFEST_VER.upper()),
                                flush=True)
            #########
            # NOTE: Add further info around non-compliant manifest headers
            # to help better clarify  ambiguous errors such as:
            # #######
            # ONPREM_Dicom.Setup(): Opening Locutus input DICOM Manifest CSV = onprem_dicom_images_manifest.csv!
            # ONPREM_Dicom.Setup(): parsing input manifest header line of "['subject_id', 'imaging_type', 'age_at_imaging_(days)\t', 'anatomical_position', 'accession_number', 'locutus_manifest_ver:locutus.onprem_dicom.2020july20']"
            # ONPREM_Dicom.Setup(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV headers ('SUBJECT_ID', 'IMAGING_TYPE', 'AGE_AT_IMAGING_(DAYS)', 'ANATOMICAL_POSITION', 'ACCESSION_NUMBER', 'locutus_manifest_ver:locutus.onprem_dicom.2020july20') for onprem_dicom_images_manifest.csv do NOT match those expected ('SUBJECT_ID', 'IMAGING_TYPE', 'AGE_AT_IMAGING_(DAYS)', 'ANATOMICAL_POSITION', 'ACCESSION_NUM', 'locutus_manifest_ver:locutus.onprem_dicom.2020july20')!
            # #######
            # In addition to the above, aim to also highlight the particularly non-compliant headers, even if so simply as:
            #########
            if (len(csv_headings_row) < MANIFEST_NUM_HEADERS):
                print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV number of headers ({1}) is not at least that expected ({2})'.format(
                    CLASS_PRINTNAME, len(csv_headings_row), MANIFEST_NUM_HEADERS), flush=True)
            if (csv_header0 != MANIFEST_HEADER_SUBJECT_ID):
                print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV subject_id header (`{1}`) is not as expected (`{2}`)'.format(
                    CLASS_PRINTNAME, csv_header0, MANIFEST_HEADER_SUBJECT_ID), flush=True)
            if (csv_header1 != MANIFEST_HEADER_OBJECT_INFO_01):
                print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV object_info_01 header (`{1}`) is not as expected (`{2}`)'.format(
                    CLASS_PRINTNAME, csv_header1, MANIFEST_HEADER_OBJECT_INFO_01), flush=True)
            if (csv_header2 != MANIFEST_HEADER_OBJECT_INFO_02):
                print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV object_info_02 header (`{1}`) is not as expected (`{2}`)'.format(
                    CLASS_PRINTNAME, csv_header2, MANIFEST_HEADER_OBJECT_INFO_02), flush=True)
            if (csv_header3 != MANIFEST_HEADER_OBJECT_INFO_03):
                print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV object_info_03 header (`{1}`) is not as expected (`{2}`)'.format(
                    CLASS_PRINTNAME, csv_header3, MANIFEST_HEADER_OBJECT_INFO_03), flush=True)
            if (csv_header4 != MANIFEST_HEADER_ACCESSION_NUM):
                print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV accession_number header (`{1}`) is not as expected (`{2}`)'.format(
                    CLASS_PRINTNAME, csv_header4, MANIFEST_HEADER_ACCESSION_NUM), flush=True)
            if (csv_header5 != MANIFEST_HEADER_DEID_QC_STATUS):
                print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV deid_qc_status header (`{1}`) is not as expected (`{2}`)'.format(
                    CLASS_PRINTNAME, csv_header5, MANIFEST_HEADER_DEID_QC_STATUS), flush=True)
            if (csv_header6 != MANIFEST_HEADER_MANIFEST_VER.upper()):
                print('{0}.Setup_Input_Manifest(): ERROR: Locutus ONPREM-DICOM input DICOM Manifest CSV manifest_version header (`{1}`) is not as expected (`{2}`)'.format(
                    CLASS_PRINTNAME, csv_header6, MANIFEST_HEADER_MANIFEST_VER.upper()), flush=True)
            #########
            # TODO: eventually also perhaps take all of the above possibilites
            # and provide them to the below ValueError for appending?
            # but first, close the current infile and DBconnSess...
            #########
            # 9/24/2024: NOTE: added these close() before the raise() (highlighting in case needing to alter):
            self.manifest_infile.close()
            self.LocutusDBconnSession.close()
            self.StagerDBconnSession.close()
            raise ValueError('Locutus {0}.Setup_Input_Manifest() ONPREM-DICOM input DICOM Manifest CSV headers (\'{1}\', \'{2}\', \'{3}\', \'{4}\', \'{5}\', \'{6}\', \'{7}\') '\
                                'for {8} do NOT match those expected (\'{9}\', \'{10}\', \'{11}\', \'{12}\', \'{13}\', \'{14}\', \'{15}\')!'.format(
                                CLASS_PRINTNAME,
                                csv_header0,
                                csv_header1,
                                csv_header2,
                                csv_header3,
                                csv_header4,
                                csv_header5,
                                csv_header6,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV,
                                MANIFEST_HEADER_SUBJECT_ID,
                                MANIFEST_HEADER_OBJECT_INFO_01,
                                MANIFEST_HEADER_OBJECT_INFO_02,
                                MANIFEST_HEADER_OBJECT_INFO_03,
                                MANIFEST_HEADER_ACCESSION_NUM,
                                MANIFEST_HEADER_DEID_QC_STATUS,
                                MANIFEST_HEADER_MANIFEST_VER.upper() ))

        if (self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS):
            # NOTE: optional MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL fields
            # to follow MANIFEST_HEADER_MANIFEST_VER iff LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS:
            csv_header7 = csv_headings_row[7].strip().upper() if (len(csv_headings_row) >= 8) else ''
            csv_header8 = csv_headings_row[8].strip().upper() if (len(csv_headings_row) >= 9) else ''

            if ((len(csv_headings_row) < MANIFEST_NUM_HEADERS_WITH_QC) or
                (csv_header7 != MANIFEST_HEADER_OUTPUT_PROCESSED_STATUS) or
                (csv_header8 != MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL)):
                # TODO: add an additional check for the optional MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL field
                # to follow MANIFEST_HEADER_MANIFEST_VER iff LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS
                print('{0}.Setup_Input_Manifest(): ERROR: ADDITIONAL LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS '\
                                'Manifest CSV headers of (\'{1}\', \'{2}\') '\
                                'for {3} do NOT match those expected (\'{4}\', \'{5}\')!'.format(
                                CLASS_PRINTNAME,
                                csv_header7,
                                csv_header8,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV,
                                MANIFEST_HEADER_OUTPUT_PROCESSED_STATUS,
                                MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL),
                                flush=True)
                #########
                # TODO: eventually also perhaps take all of the above possibilites and provide them to the below ValueError for appending?
                raise ValueError('Locutus {0}.Setup_Input_Manifest() ADDITIONAL LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS '\
                                'Manifest CSV headers of (\'{1}\', \'{2}\') '\
                                'for {3} do NOT match those expected (\'{4}\', \'{5}\')!'.format(
                                CLASS_PRINTNAME,
                                csv_header7,
                                csv_header8,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV,
                                MANIFEST_HEADER_OUTPUT_PROCESSED_STATUS,
                                MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL ))

        print('{0}.Setup_Input_Manifest(): Using Locutus ONPREM-DICOM input DICOM Manifest CSV = {1}!'.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV),
                                flush=True)

        # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
        # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
        # TODO: will also want to eventually use the actual headers as provided, as we move towards support of a more dynamic manifest definition:
        # NOTE: be sure to put MANIFEST_HEADER_MANIFEST_VER at the end of the normally expected headers,
        # and BEFORE additional output fields (MANIFEST_HEADER_OUTPUT_PROCESSED_STATUS, etc.)
        nowtime = datetime.datetime.utcnow()
        print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12} (UTC)'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    MANIFEST_HEADER_SUBJECT_ID,
                    MANIFEST_HEADER_OBJECT_INFO_01,
                    MANIFEST_HEADER_OBJECT_INFO_02,
                    MANIFEST_HEADER_OBJECT_INFO_03,
                    MANIFEST_HEADER_ACCESSION_NUM,
                    MANIFEST_HEADER_DEID_QC_STATUS,
                    MANIFEST_HEADER_MANIFEST_VER,
                    MANIFEST_HEADER_OUTPUT_PROCESSED_STATUS,
                    MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL,
                    MANIFEST_HEADER_OUTPUT_TARGET,
                    MANIFEST_HEADER_OUTPUT_FW_IMPORT_SESSION_ARG,
                    '{0}={1}'.format(MANIFEST_HEADER_AS_OF_DATETIME,nowtime)), flush=True)

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
        # LOCUTUS_TEST:
        print('{0},# LOCUTUS,TEST_MODE:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    self.locutus_settings.LOCUTUS_TEST),
                    flush=True)
        # as well as the current run iteration,
        # as applicable w/ LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE:
        print('{0},# RUN,ITERATION:,{1}'.format(
                    MANIFEST_OUTPUT_PREFIX,
                    run_iteration_num),
                    flush=True)
        ################
        # end of Setup_Input_Manifest()


    def Setup(self,
            trig_secrets,
            src_orthanc_vault_config_path):
        print('{0}.Setup() initializing...'.format(CLASS_PRINTNAME), flush=True)

        global LOCUTUS_ONPREM_DICOM_STATUS_TABLE
        global LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
        global LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE
        ##########################################
        # TODO: ponder the following, or not, at least decorating them as globals even if unsure of the seeming inconsistencies.
        # Q: why does the following global for LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION also need to be done:
        global LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION
        # but such a global was NOT needed for a very similar such CFG, LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION
        # so.... go ahead and add it here anyhow, for consistency, and further head scratching:
        global LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION
        ##########################################

        # introducing... Locutus Workspaces:
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
                print('{0},onprem-dicom-workspace,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_STATUS_TABLE", LOCUTUS_ONPREM_DICOM_STATUS_TABLE), flush=True)
                print('{0},onprem-dicom-workspace,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE", LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE), flush=True)
                print('{0},onprem-dicom-workspace,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE", LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE), flush=True)

        # NOTE: the src_orthanc_vault_config_path parameter represents the initial source instance of Orthanc,
        # and usually comes from main_locutus as Settings.ONPREM_DICOM_STAGE_VAULT_PATH,
        # and Settings.ONPREM_DICOM_STAGE_VAULT_PATH itself doesn't otherwise make it into this module.
        # In case needed, however, store it as src_orthanc_vault_config_path:
        self.src_orthanc_vault_config_path = src_orthanc_vault_config_path

        # However, since the De-ID QC instance of Orthanc is a bit more optional,
        # obtain its vault path by more traditional means,via locutus_settings (which could be done for the above as well)
        self.deid_qc_orthanc_vault_config_path = self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_QC_ORTHANC_CONFIG_VAULT_PATH

        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT

        if self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE:
            # NOTE: UPDATE the active LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES for 'config_type' : 'cfg_dicom_download_ver':
            # to find the previous value as found through the following config_type=cfg_dicom_download_ver:
            intcfg_key_of_interest_type = 'config_type'
            intcfg_value_of_interest_type = 'cfg_dicom_download_ver'
            ##
            # with the following to update:
            ##
            intcfg_key_of_interest_version = 'config_version'
            intcfg_value_of_interest_version_old = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION_DICOMDIR
            intcfg_value_of_interest_version_new = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION_ZIPARCHIVE
            LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION_ZIPARCHIVE
            ##
            intcfg_key_of_interest_desc = 'config_desc'
            intcfg_value_of_interest_desc_old = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC_DICOMDIR
            intcfg_value_of_interest_desc_new = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC_ZIPARCHIVE
            LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DESC_ZIPARCHIVE
            ##
            intcfg_key_of_interest_date = 'date_activated'
            intcfg_value_of_interest_date_old = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE_DICOMDIR
            intcfg_value_of_interest_date_new = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE_ZIPARCHIVE
            LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE = LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_DATE_ZIPARCHIVE
            ##
            for idx_cfg, cfg_dict in enumerate(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES):
                # and iterate through each key/value pair of each dictionary:
                for idx_key, cfg_key in enumerate(cfg_dict.keys()):
                    if (LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_type] == intcfg_value_of_interest_type) \
                    and (LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_version] == intcfg_value_of_interest_version_old):
                        print('{0}.Setup(): LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE '\
                            'found INT_CFGS_ACTIVES idx_cfg={1}, cfg_key={2}, cfg_value={3} to update.'.format(
                                CLASS_PRINTNAME,
                                idx_key, cfg_key,
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][cfg_key]
                            ))
                        ###
                        # intcfg_key_of_interest_version:
                        # NOTE: *is* already checking that the old value is intcfg_value_of_interest_version_old = 'pre2025march24=DICOMDIR'
                        print('{0}.Setup(): LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE updating VERSION of LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES for {1}-{2}-{3} FROM: \"{4}\" TO: \"{5}\"'.format(
                                CLASS_PRINTNAME,
                                "INT_CFGS_ACTIVES",
                                (idx_cfg+1),
                                cfg_key,
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_version],
                                intcfg_value_of_interest_version_new), flush=True)
                        LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_version] = intcfg_value_of_interest_version_new
                        ##
                        # intcfg_key_of_interest_desc:
                        # TODO: could also check that the old value is intcfg_value_of_interest_desc_old = 'download=DICOMDIR'
                        print('{0}.Setup(): LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE updating DESC of LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES for {1}-{2}-{3} FROM: \"{4}\" TO: \"{5}\"'.format(
                                CLASS_PRINTNAME,
                                "INT_CFGS_ACTIVES",
                                (idx_cfg+1),
                                cfg_key,
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_desc],
                                intcfg_value_of_interest_desc_new), flush=True)
                        LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_desc] = intcfg_value_of_interest_desc_new
                        ##
                        # intcfg_key_of_interest_date:
                        # TODO: could also check that the old value is intcfg_value_of_interest_date_old = '2017-01-01'
                        print('{0}.Setup(): LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE updating DATE of LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES for {1}-{2}-{3} FROM: \"{4}\" TO: \"{5}\"'.format(
                                CLASS_PRINTNAME,
                                "INT_CFGS_ACTIVES",
                                (idx_cfg+1),
                                cfg_key,
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_date],
                                intcfg_value_of_interest_date_new), flush=True)
                        LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_date] = intcfg_value_of_interest_date_new

        #####
        # NOTE: ^C/^V from the above LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE, as for LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP
        # TODO: abstract these into a function/method to assist.
        #####

        if self.locutus_settings.LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP:
            # NOTE: UPDATE the active LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES for 'config_type' : 'cfg_dicom_anon_alignment_mode':
            # to find the previous value as found through the following config_type=cfg_dicom_download_ver:
            intcfg_key_of_interest_type = 'config_type'
            intcfg_value_of_interest_type = 'cfg_dicom_anon_alignment_mode'
            ##
            # with the following to update:
            ##
            intcfg_key_of_interest_version = 'config_version'
            intcfg_value_of_interest_version_old = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION_ORIGINAL
            intcfg_value_of_interest_version_new = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION_GCP
            LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION_GCP
            ##
            intcfg_key_of_interest_desc = 'config_desc'
            intcfg_value_of_interest_desc_old = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC_ORIGINAL
            intcfg_value_of_interest_desc_new = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC_GCP
            LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DESC_GCP
            ##
            intcfg_key_of_interest_date = 'date_activated'
            intcfg_value_of_interest_date_old = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE_ORIGINAL
            intcfg_value_of_interest_date_new = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE_GCP
            LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE = LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_DATE_GCP
            ##
            for idx_cfg, cfg_dict in enumerate(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES):
                # and iterate through each key/value pair of each dictionary:
                for idx_key, cfg_key in enumerate(cfg_dict.keys()):
                    if (LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_type] == intcfg_value_of_interest_type) \
                    and (LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_version] == intcfg_value_of_interest_version_old):
                        print('{0}.Setup(): LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP '\
                            'found INT_CFGS_ACTIVES idx_cfg={1}, cfg_key={2}, cfg_value={3} to update.'.format(
                                CLASS_PRINTNAME,
                                idx_key, cfg_key,
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][cfg_key]
                            ))
                        ###
                        # intcfg_key_of_interest_version:
                        # NOTE: *is* already checking that the old value is intcfg_value_of_interest_version_old = '2017-01-01=original'
                        print('{0}.Setup(): LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP updating VERSION of LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES for {1}-{2}-{3} FROM: \"{4}\" TO: \"{5}\"'.format(
                                CLASS_PRINTNAME,
                                "INT_CFGS_ACTIVES",
                                (idx_cfg+1),
                                cfg_key,
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_version],
                                intcfg_value_of_interest_version_new), flush=True)
                        LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_version] = intcfg_value_of_interest_version_new
                        ##
                        # intcfg_key_of_interest_desc:
                        # TODO: could also check that the old value is intcfg_value_of_interest_desc_old = 'original mode w/ expanded AUDIT, CLEANED, & dates=19010101'
                        print('{0}.Setup(): LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP updating DESC of LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES for {1}-{2}-{3} FROM: \"{4}\" TO: \"{5}\"'.format(
                                CLASS_PRINTNAME,
                                "INT_CFGS_ACTIVES",
                                (idx_cfg+1),
                                cfg_key,
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_desc],
                                intcfg_value_of_interest_desc_new), flush=True)
                        LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_desc] = intcfg_value_of_interest_desc_new
                        ##
                        # intcfg_key_of_interest_date:
                        # TODO: could also check that the old value is intcfg_value_of_interest_date_old = '2017-01-01'
                        print('{0}.Setup(): LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP updating DATE of LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES for {1}-{2}-{3} FROM: \"{4}\" TO: \"{5}\"'.format(
                                CLASS_PRINTNAME,
                                "INT_CFGS_ACTIVES",
                                (idx_cfg+1),
                                cfg_key,
                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_date],
                                intcfg_value_of_interest_date_new), flush=True)
                        LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][intcfg_key_of_interest_date] = intcfg_value_of_interest_date_new

        # LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES:
        # iterate through the list of dictionaries:
        # NOTE: moved into Process() immediately after Phase00 CREATE TABLE LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE
        # and it subsequent call to: self.set_active_internal_configs(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES
        # to ensure that any latest ACTIVE-DB configs are taken into account.
        ##########
        #for idx_cfg, cfg_dict in enumerate(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES):
        #    # and iterate through each key/value pair of each dictionary:
        #    for idx_key, cfg_key in enumerate(cfg_dict.keys()):
        #        print('{0},onprem-dicom-hardcoded,{1}-{2}-{3},\"{4}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "INT_CFGS_ACTIVES", (idx_cfg+1), cfg_key, LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][cfg_key]), flush=True)
        ##########

        # DICOM_SERIES_DESCS_TO_EXCLUDE: (which is is itself a CSV-list, so be sure to wrap it in quotes)
        print('{0},onprem-dicom-hardcoded,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "DICOM_SERIES_DESCS_TO_EXCLUDE", DICOM_SERIES_DESCS_TO_EXCLUDE), flush=True)
        # DEFAULT_DICOM_ANON_SPEC_FILE:
        print('{0},onprem-dicom-hardcoded,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "DEFAULT_DICOM_ANON_SPEC_FILE", DEFAULT_DICOM_ANON_SPEC_FILE), flush=True)
        # DEFAULT_DICOM_ANON_MODALITIES_STR: (which is is itself a CSV-list, so be sure to wrap it in quotes)
        print('{0},onprem-dicom-hardcoded,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "DEFAULT_DICOM_ANON_MODALITIES_STR", DEFAULT_DICOM_ANON_MODALITIES_STR), flush=True)
        ##
        # NOTE: for consistency, go ahead and wrap these all in quotes as well, even if just the INT CFG versions... yeah?
        ## LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION:
        print('{0},onprem-dicom-current,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION", LOCUTUS_ONPREM_DICOM_INT_CFG_DOWNLOAD_VERSION), flush=True)
        ## LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION:
        print('{0},onprem-dicom-current,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION", LOCUTUS_ONPREM_DICOM_ANON_ALIGNMENT_MODE_CFG_VERSION), flush=True)

        self.trig_secrets = trig_secrets
        self.hvac_client = trig_secrets.get_hvac_client()

        ##################
        # Source Orthanc:
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Setup(): reading sub-config for ONPREM src-Orthanc server from Vault sub-path \'{1}\'...'.format(
                                    CLASS_PRINTNAME,
                                    self.src_orthanc_vault_config_path), flush=True)
        src_orthanc_input_yaml_str = self.hvac_client.read(self.src_orthanc_vault_config_path)['data']['value']
        self.src_onprem_dicom_config = yaml.safe_load(src_orthanc_input_yaml_str)

        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},onprem-dicom-via-vault,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX,
                                                        "src_orthanc_hostname",
                                                        self.src_onprem_dicom_config['orthanc_hostname']),
                                                        flush=True)

        self.src_onprem_dicom_orthanc_url = 'http://%s:%d' % (self.src_onprem_dicom_config['orthanc_hostname'],
                                    int(self.src_onprem_dicom_config['orthanc_portnum']))
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: {0}.Setup(): giving Vault-based ONPREM_Dicom src-Orthanc CFG[orthanc_hostname] = {1}'.format(
                                    CLASS_PRINTNAME,
                                    self.src_onprem_dicom_config['orthanc_hostname']),
                                    flush=True)
            print('DEBUG: {0}.Setup(): giving Vault-based ONPREM_Dicom src-Orthanc CFG[orthanc_portnum] = {1}'.format(
                                    CLASS_PRINTNAME,
                                    self.src_onprem_dicom_config['orthanc_portnum']),
                                    flush=True)
            print('DEBUG: {0}.Setup(): giving Vault-based ONPREM_Dicom src-Orthanc CFG[orthanc_user] = {1}'.format(
                                    CLASS_PRINTNAME,
                                    self.src_onprem_dicom_config['orthanc_user']),
                                    flush=True)
            print('DEBUG: {0}.Setup(): giving Vault-based ONPREM_Dicom src-Orthanc CFG[dicom_stage_DB_vault_path] = {1}'.format(
                                    CLASS_PRINTNAME,
                                    self.src_onprem_dicom_config['dicom_stage_DB_vault_path']),
                                    flush=True)

        # TODO: Q: is there a DISCONNECT that we need to use with Orthanc
        # between each of the DICOM modules, ONPREM & GCP?
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Setup(): initially authenticating with src-Orthanc server...'.format(CLASS_PRINTNAME), flush=True)
        RestToolbox.SetCredentials(self.src_onprem_dicom_config['orthanc_user'],
                                    self.src_onprem_dicom_config['orthanc_password'])

        self.qc_onprem_dicom_config = ''
        if self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE:
            print('{0}.Setup(): WARNING: Manual QC via the ONPREM De-ID QC Orthanc server has been disabled via '\
                                'LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE={1}.  Bypassing'.format
                                    (CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE),
                                    flush=True)
        else:
            #if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE:
            print('{0}.Setup(): NOTE: Manual QC via the ONPREM De-ID QC Orthanc server is enabled since '\
                                'LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE={1}.'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE),
                                    flush=True)

            ##################
            # Optional Manual De-ID QC Orthanc:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Setup(): reading sub-config for ONPREM qc-Orthanc server from Vault sub-path \'{1}\'...'.format(
                                    CLASS_PRINTNAME,
                                    self.deid_qc_orthanc_vault_config_path), flush=True)
            qc_input_yaml_str = self.hvac_client.read(self.deid_qc_orthanc_vault_config_path)['data']['value']
            self.qc_onprem_dicom_config = yaml.safe_load(qc_input_yaml_str)

            # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
            print('{0},onprem-dicom-via-vault,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX,
                                                            "qc_orthanc_hostname",
                                                            self.qc_onprem_dicom_config['orthanc_hostname']),
                                                            flush=True)

            self.deid_qc_onprem_dicom_orthanc_url = 'http://%s:%d' % (self.qc_onprem_dicom_config['orthanc_hostname'],
                                        int(self.qc_onprem_dicom_config['orthanc_portnum']))
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('DEBUG: {0}.Setup(): giving Vault-based ONPREM_Dicom QC-Orthanc CFG[orthanc_hostname] = {1}'.format(
                                        CLASS_PRINTNAME,
                                        self.qc_onprem_dicom_config['orthanc_hostname']),
                                        flush=True)
                print('DEBUG: {0}.Setup(): giving Vault-based ONPREM_Dicom QC-Orthanc CFG[orthanc_portnum] = {1}'.format(
                                        CLASS_PRINTNAME,
                                        self.qc_onprem_dicom_config['orthanc_portnum']),
                                        flush=True)
                print('DEBUG: {0}.Setup(): giving Vault-based ONPREM_Dicom QC-Orthanc CFG[orthanc_user] = {1}'.format(
                                        CLASS_PRINTNAME,
                                        self.qc_onprem_dicom_config['orthanc_user']),
                                        flush=True)
                # NOTE: De-ID QC Orthanc does not get staged in the same fashion as the source instance of Orthanc:
                # no need for: qc_onprem_dicom_config['dicom_stage_DB_vault_path']

            ##################################
            # CONFIRM that this is not the same as the src-Orthanc, via:
            if (self.src_onprem_dicom_config['orthanc_hostname'] == self.qc_onprem_dicom_config['orthanc_hostname']) \
                and (self.src_onprem_dicom_config['orthanc_portnum'] == self.qc_onprem_dicom_config['orthanc_portnum']):
                print('{0}.Setup(): ERROR: Locutus ONPREM-DICOM src-Orthanc and qc-Orthanc appear to be the same '\
                                    ' (host=\'{1}\', port={2})!'.format(
                                    CLASS_PRINTNAME,
                                    self.qc_onprem_dicom_config['orthanc_hostname'],
                                    self.qc_onprem_dicom_config['orthanc_portnum']),
                                    flush=True)
                raise ValueError('Locutus {0}.Setup() ERROR: Locutus ONPREM-DICOM src-Orthanc and qc-Orthanc appear to be the same '\
                                    ' (host=\'{1}\', port={2})!'.format(
                                    CLASS_PRINTNAME,
                                    self.qc_onprem_dicom_config['orthanc_hostname'],
                                    self.qc_onprem_dicom_config['orthanc_portnum'] ))
            else:
                # NOTE: re-authenticate via the RestToolbox whenever accessing EITHER Orthanc:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.Setup(): initially authenticating with qc-Orthanc server...'.format(CLASS_PRINTNAME), flush=True)
                RestToolbox.SetCredentials(self.qc_onprem_dicom_config['orthanc_user'],
                                            self.qc_onprem_dicom_config['orthanc_password'])

        ###################################
        # setup ONPREM-DICOM's Local DICOM Stager database credentials:
        #
        # Dicom Target Stager Database credentials for TRiG Data Warehouse via Vault:
        onprem_dicom_target_db_host = self.hvac_client.read(
                    self.src_onprem_dicom_config['dicom_stage_DB_vault_path']+'/db_host')['data']['value']
        # TODO: eventually replace the existing Vault db_host keys from FQDN into simply "production" & "dev",
        # but until then....
        # NOTE: convert the above FQDN db_host into a format that TrigSecrets can understand, namely:
        trig_secrets_dw_host = 'UNKNOWN'
        if onprem_dicom_target_db_host == 'eigdw.research.chop.edu':
            trig_secrets_dw_host = 'production'
        elif onprem_dicom_target_db_host == 'eigdwdev.research.chop.edu':
            trig_secrets_dw_host = 'dev'
        else:
            # TODO: consider eventually switching the Vault-based db_hosts directly into production & dev, but for now:
            raise ValueError('Locutus {0}.Setup(): dicom_stage_DB_vault_path of '\
                                '{1} contains an unknown db_host ({2}); '\
                                'and currently expecting only [eigdw|eigdwdev].research.chop.edu ; exiting.'.format(
                                CLASS_PRINTNAME,
                                self.src_onprem_dicom_config['dicom_stage_DB_vault_path'],
                                onprem_dicom_target_db_host))
        # But do still load the actual DB name for logging:
        self.onprem_dicom_target_db_name = self.hvac_client.read(
                    self.src_onprem_dicom_config['dicom_stage_DB_vault_path']+'/db_name')['data']['value']

        # and, if so defined, also append the optional dev_suffix to the db_name:
        # Support EITHER boolean (unquoted) OR string (quoted) representations of True for *_use_dev_suffix:
        db_use_dev_suffix_raw = self.src_onprem_dicom_config['dicom_stage_DB_use_dev_suffix']
        db_use_dev_suffix = False
        if db_use_dev_suffix_raw and str(db_use_dev_suffix_raw).upper() == "TRUE":
            db_use_dev_suffix = True
            # WARNING: assumes that the TRiGSecrets implementation of 'DB_use_dev_suffix'
            # is the same as our (now purely optional) 'DB_dev_suffix',
            # which is typically a `_dev` suffix:
            self.onprem_dicom_target_db_name += self.src_onprem_dicom_config['dicom_stage_DB_dev_suffix']
        # finally generate the entire target DB connection string:
        # NOTE: Now using TrigSecrets to generate the DB connection string:
        # TODO: eventually replace the existing config VAULT_PATH keys to no longer have the 'secret/dbhi/eig/databases' prefix:
        # but for now, merely check and remove up to the last '/', if included:
        trig_secrets_db_vault_path =  self.src_onprem_dicom_config['dicom_stage_DB_vault_path']
        if trig_secrets_db_vault_path.find('/') > -1:
            trig_secrets_db_vault_path = trig_secrets_db_vault_path[trig_secrets_db_vault_path.rfind('/')+1:]
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Setup(): About to generate DB credentials from TrigSecrets using vault_db_name={1}, dw_host={2}, use_dev_suffix={3}'.format(
                                CLASS_PRINTNAME,
                                trig_secrets_db_vault_path,
                                trig_secrets_dw_host,
                                db_use_dev_suffix),
                                flush=True)
        self.onprem_dicom_target_db = trig_secrets.get_db_credentials(
                                                    vault_db_name=trig_secrets_db_vault_path,
                                                    dw_host=trig_secrets_dw_host,
                                                    dev=db_use_dev_suffix)

        #####
        # Setup the connection engine for the ONPREM DICOM staging DB connection string
        #
        # TODO: consider only opening self.StagerDBconn where needed for Phase1 & Phase2 aspects,
        # closing the connection once completed with any internal migration from the Stage,
        # to leave the majority of the time focused on the following LocutusDBconnSession for processing.
        # NOTE: much like being explored for Settings.get_Locutus_system_status() to reduce the count
        # of unnecessary DB connections opened to TDW, the Translational Data Wherehouse,
        # to better support scaling up to HPC deployments
        #####
        # NOTE: renamed these to better more clearly represent it as the Stager:
        # WAS: OnPremDicomDBengine = create_engine(self.onprem_dicom_target_db)
        # WAS: self.OnPremDicomDBconn = OnPremDicomDBengine.connect()
        #####
        # TODO: evaluate if onprem_dicom_target_db likewise needs a name revision,
        # to better indicate that it is for the Stager_DB?  Proceeding with it as is for now...
        #####
        StagerDBengine = create_engine(self.onprem_dicom_target_db)
        self.StagerDBconn = StagerDBengine.connect()
        StagerSession = sessionmaker(bind=self.StagerDBconn, autocommit=True)
        self.StagerDBconnSession = StagerSession()
        #
        ###################################
        print('{0}.Setup(): Using Locutus onprem_dicom_target_db_name = {1}!'.format(
                                    CLASS_PRINTNAME,
                                    self.onprem_dicom_target_db_name),
                                    flush=True)
        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},onprem-dicom-via-vault,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX, "onprem_dicom_target_db_name", self.onprem_dicom_target_db_name), flush=True)

        # Setup the connection engine for the Locutus DB connection string
        LocutusDBengine = create_engine(self.locutus_settings.locutus_target_db)
        self.LocutusDBconn = LocutusDBengine.connect()
        Session = sessionmaker(bind=self.LocutusDBconn, autocommit=True)
        self.LocutusDBconnSession = Session()
        # NOTE: since the above new LocutusDBconnSession approach works in module_gcp_dicom.py,
        # for preventing unexpected timeouts, then....
        # introducing this to the other Locutus modules as well (such as this!),
        # TODO: somebody complete with all the .close() calls, as paired with manifest_infiile.close(), wherever clever.
        #####
        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},onprem-dicom-via-vault,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX, "Locutus_dicom_target_db_name", self.locutus_settings.locutus_target_db_name), flush=True)

        ###################################
        # NOTE: now AFTER the above LocutusDBconnSession part of Setup(),
        # since Setup_Input_Manifest() will close said connection upon failure (including mismatched headers)
        # setup GCP-DICOM DICOM input manifest CSV:
        # with current iteration to be emitted in the commented header:
        run_iteration_num = 0
        self.Setup_Input_Manifest(run_iteration_num)
        ###################################

        # determine current hostname, for prefacing stored paths to outputs:
        self.this_hostname = socket.getfqdn()
        # NOTE: locally on a laptop, the above returns something like:
        #    1.0.0.127.in-addr.arpa
        # But on the Dev Server, returns: reslndbhiops02.research.chop.edu
        # And on Int Prod, returns merely proper FQDN as well.
        # Whereas:
        #   self.this_hostname = socket.gethostname()
        # Does return the actual hostname locally (and on Dev),
        # though not fully qualified (except on Int Prod, strangely)
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Setup(): Noting that this is running on hostname: {1}'.format(
                                    CLASS_PRINTNAME,
                                    self.this_hostname),
                                    flush=True)
        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},onprem-dicom,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX, "this_hostname", self.this_hostname), flush=True)

        # NOTE: If in Docker, the above grabs the docker container's info, e.g,:
        #   one run might give: b0bd488d28a3, and another: a5346c62a5b2
        #   since the docker container may be -rm'd after running.
        # Refinement to determine the docker container's host-node's hostname,
        # AND will likewise need the Docker deployment to mount a host volumne
        # for the processing output files/dirs, otherwise not very accessible!
        #
        # Allow an override of the hostname through Docker env vars, if set:
        if os.environ.get('DOCKERHOST_HOSTNAME'):
            self.this_hostname = os.environ.get('DOCKERHOST_HOSTNAME')
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Setup(): Overriding with supplied DOCKERHOST_HOSTNAME: {1}'.format(
                                    CLASS_PRINTNAME,
                                    self.this_hostname),
                                    flush=True)
        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},onprem-dicom-env,{1},{2}'.format(src_modules.settings.CFG_OUT_PREFIX, "DOCKERHOST_HOSTNAME", os.environ.get('DOCKERHOST_HOSTNAME')), flush=True)

        ######################################
        # NOTE: pre-check of LOCUTUS_ONPREM_DICOM_ZIP_DIR as inspired by issues troubleshooting
        # to volume mounts made available as well as the permissions to makedir, via a Jenkins service account.
        #
        # TODO: Q: shouldn't we likewise Ensure for LOCUTUS_ONPREM_DICOM_DEID_DIR what the below does for LOCUTUS_ONPREM_DICOM_ZIP_DIR?
        #
        # NOTE: currently LOCUTUS_ONPREM_DICOM_DEID_DIR is left to fend for itself in Process_Phase04_DeidentifyAndZip(),
        # with perhaps the idea here for Setup to at least check the first-needed subdir, LOCUTUS_ONPREM_DICOM_ZIP_DIR, is available,
        # and if not able to make it so, bailing and highlighting a bigger picture permissions issue anyhow.
        ######################################

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Setup(): Ensuring LOCUTUS_ONPREM_DICOM_ZIP_DIR exists as: {1}'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR),
                                    flush=True)
        zip_dir_exists = True
        try:
            #if not os.path.exists(self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR):
            # NOTE: if not at the expected dir, check each parent path until something is found:
            curr_parent_dir = self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR
            # Q: not yet sure why the initial directory checks twice in the below while,
            # EXCEPT of course if there is merely a trailing PATH_DELIM, e.g., /mnt/stuff/a/b/c/d/
            # So, start with a premliminary removing of any trailing-only PATH_DELIM.
            len_parent_dir = len(curr_parent_dir)
            last_parent_delim = curr_parent_dir.rfind(PATH_DELIM)
            if last_parent_delim == (len_parent_dir - 1):
                curr_parent_dir = curr_parent_dir[:last_parent_delim]
                print('{0}.Setup(): removing trailing PATH_DELIM (\'{1}\'), for updated curr_parent_dir: {2}'.format(
                    CLASS_PRINTNAME,
                    PATH_DELIM,
                    curr_parent_dir),
                    flush=True)
            while curr_parent_dir and not os.path.exists(curr_parent_dir):
                # as soon as we have to go up a full level, we can confirm absence of the expected zip dir:
                zip_dir_exists = False
                last_parent_delim = curr_parent_dir.rfind(PATH_DELIM)
                next_parent_dir = curr_parent_dir[:last_parent_delim]
                print('{0}.Setup(): curr_parent_dir={0} NOT FOUND, next looking for parent dir: {1}'.format(
                    CLASS_PRINTNAME,
                    curr_parent_dir,
                    next_parent_dir),
                    flush=True)
                curr_parent_dir = next_parent_dir
            if not curr_parent_dir:
                # TODO: throw an exception for this this message?
                print('{0}.Setup(): ERROR: LOCUTUS_ONPREM_DICOM_ZIP_DIR could not find even '\
                        'the top-most parent dir of: {1}'.format(
                            CLASS_PRINTNAME,
                            self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR),
                            flush=True)
                raise ValueError('Locutus {0}.Setup(): LOCUTUS_ONPREM_DICOM_ZIP_DIR could not find even '\
                                'the top-most parent dir of: {1}; '\
                                'PLEASE INVESTIGATE VOLUME MOUNT and PERMISSIONS.'.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR))
            else:
                # finally found a directory level that exists:
                print('{0}.Setup(): Found a parent dir of LOCUTUS_ONPREM_DICOM_ZIP_DIR to exist at: {1}'.format(
                                    CLASS_PRINTNAME,
                                    curr_parent_dir),
                                    flush=True)
                print('{0}.Setup(): file contents of directory {1}: {2}'.format(
                                    CLASS_PRINTNAME,
                                    curr_parent_dir,
                                    os.listdir(curr_parent_dir)),
                                    flush=True)
                if not zip_dir_exists:
                    # and emit the following, regaredless of LOCUTUS_VERBOSE,
                    # and REGARDLESS of how many levels deeper to be made:
                    print('{0}.Setup(): ZIP DIR not found; creating LOCUTUS_ONPREM_DICOM_ZIP_DIR: {1}'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR),
                                    flush=True)
                    os.makedirs(self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR)
        except OSError as e:
            ######################################
            #############################
            traceback_clues = traceback.format_exc()
            print('{0}.Setup() ERROR DEBUG: suppressed os.makedirs() traceback MAY look like: {1}'.format(
                        CLASS_PRINTNAME,
                        traceback_clues), flush=True)
            #############################
            ######################################
            if e.errno != errno.EEXIST:
                print('{0}.Setup() re-raising makedirs exception: {1}.'.format(
                    CLASS_PRINTNAME,
                    e),
                    flush=True)
                # 9/24/2024: NOTE: added these close() before the raise() (highlighting in case needing to alter)":
                self.manifest_infile.close()
                self.LocutusDBconnSession.close()
                self.StagerDBconnSession.close()
                raise

        ######################
        # NOTE: even though config will remain in this class,
        # go ahead and return the src_onprem_dicom_config as well, in case the caller wants to access it
        ######################
        # NOTE: also returning the qc_onprem_dicom_config,
        # whether or not LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE:
        ######################
        return (self.src_onprem_dicom_config, self.qc_onprem_dicom_config)
        # end of Setup()


    def Process_Phase03_DownloadFromOrthanc(self,
                                        prev_phase_processed,
                                        curr_uuid,
                                        curr_subject_id,
                                        curr_object_info_01,
                                        curr_object_info_02,
                                        curr_object_info_03,
                                        curr_accession_num,
                                        manifest_deid_qc_status,
                                        processRemainingPhases):

        errors_encountered = 0
        errors_message2return = ''
        curr_deid_qc_api_study_url = ''
        print('{0}.Process(): PHASE03: Process_Phase03_DownloadFromOrthanc() called with '\
                        'unprocessed uuid == {1} with prev_phase_processed={2} and manifest_deid_qc_status=\'{3}\'!'.format(
                        CLASS_PRINTNAME,
                        curr_uuid,
                        prev_phase_processed,
                        manifest_deid_qc_status),
                        flush=True)

        # PROGRESS BAR UPDATES via MANIFEST_STATUS:
        interim_manifest_status = 'PROCESSING_CHANGE_at_PHASE03'
        self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                    'last_datetime_processed=now(), '\
                                    'manifest_status=\'{1}\' '\
                                    'WHERE accession_num=\'{2}\' '\
                                    '    AND active ;'.format(
                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                    interim_manifest_status,
                                    curr_accession_num))

        # Processing Phase03a:
        # read this unprocessed uuid's max change_seq_id
        # from LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
        # SELECTing COALESCE(MAX(change_seq_id),0) across initial MIN_PROCESSING_PHASE,
        # NO LONGER: loading step towards consolidated/collapsed/compressed/aggregated via as_change_seq_id).
        # [See also: CHANGE-SEQUENCE CONSOLIDATION:]
        max_change_result = self.LocutusDBconnSession.execute('SELECT COALESCE(MAX(change_seq_id),0) '\
                                                        'as max_seq_id FROM {0} '\
                                                        'WHERE phase_processed = {1} '\
                                                        'AND uuid=\'{2}\' '\
                                                        '    AND active ;'.format(
                                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                        prev_phase_processed,
                                                        curr_uuid))
        max_change_row = max_change_result.fetchone()
        curr_uuid_max_change_seq_id = max_change_row['max_seq_id']
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process(): found uuid\'s MAX(unprocessed change_seq_id, WHERE active) == {1}!'.format(
                        CLASS_PRINTNAME,
                        curr_uuid_max_change_seq_id),
                        flush=True)

        if curr_uuid_max_change_seq_id == 0:
            print('{0}.Process(): WARNING: uuid\'s MAX(unprocessed change_seq_id) == {1} '\
                        'while checking WHERE active '\
                        'for this uuid, both expected for an UN-PROCESSED accession!'.format(
                        CLASS_PRINTNAME,
                        curr_uuid_max_change_seq_id),
                        flush=True)

        # Processing Phase03b:
        # Get the DICOMDir for this uuid, from STAGER_STABLESTUDY_TABLE:
        # NOTE: only need change_seq_id, but uuid also included for safety:
        staged_url_result = self.StagerDBconnSession.execute('SELECT url_dicomdir '\
                                                    'FROM {0} WHERE change_seq_id={1} '\
                                                    'AND uuid=\'{2}\' '\
                                                    '    AND active ;'.format(
                                                    STAGER_STABLESTUDY_TABLE,
                                                    curr_uuid_max_change_seq_id,
                                                    curr_uuid))
        row = staged_url_result.fetchone()


        internal_DEBUG_simulate_failed_phase03 = False
        # Enable the following internal DEBUG_ to force fail on Phase03
        #internal_DEBUG_simulate_failed_phase03 = True
        if internal_DEBUG_simulate_failed_phase03:
            row = None
            print('{0}.Process(): PHASE03: DEBUG: setting internal_DEBUG_simulate_failed_phase03={1} '\
                                                    'AND row={2} to test phase ERROR!'.format(
                                                    CLASS_PRINTNAME,
                                                    internal_DEBUG_simulate_failed_phase03,
                                                    row),
                                                    flush=True)

        if row is None:
            print('{0}.Process(): PHASE03: ERROR: found NO url_dicomdir for change_seq_id={1} '\
                                                    'AND uuid={2} in {3}!'.format(
                                                    CLASS_PRINTNAME,
                                                    curr_uuid_max_change_seq_id,
                                                    curr_uuid,
                                                    STAGER_STABLESTUDY_TABLE),
                                                    flush=True)
            # NOTE: return without any further updates or processing of this uuid:
            errors_encountered += 1
            if errors_encountered > 1:
                errors_message2return += ';'
            errors_message2return += 'ERROR_PHASE03=[found NO url_dicomdir for this uuid change_seq_id={0}]'.format(curr_uuid_max_change_seq_id)
        else:
            # Processing Phase03c:
            # download the actual DICOMDir of ALL this patient's DICOM images
            staged_url = row['url_dicomdir']

            # NOTE: prior to 3/24/2025 LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE was absent ~= False == DICOMDIR (flat IMAGES/IM** structure) = LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE
            if self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE:
                # first, emit a notice of the impending override, whether or not LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE03c: INFO: LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE Overridding uuid\'s url_dicomdir to zip Archive endpoint ({1}) from DICOMDIR endpoint ({2}) of: {3}'.format(
                            CLASS_PRINTNAME,
                            self.locutus_settings.ORTHANC_DOWNLOAD_ZIP_ENDPOINT,
                            self.locutus_settings.ORTHANC_DOWNLOAD_DICOMDIR_ENDPOINT,
                            staged_url),
                            flush=True)
                # Rather than changing any of the Staging tables at this time,
                # merely programmatically override from the DICOMDIR media endpoint
                # to use the zip Archive archive endpoint:
                staged_url = staged_url.replace(self.locutus_settings.ORTHANC_DOWNLOAD_DICOMDIR_ENDPOINT, self.locutus_settings.ORTHANC_DOWNLOAD_ZIP_ENDPOINT)

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE03c: found uuid\'s url_dicomdir == {1}'.format(
                            CLASS_PRINTNAME,
                            staged_url),
                            flush=True)

            dicomdir_zip_filename = '{0}/uuid_{1}.zip'.format(
                                        self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR, curr_uuid)
            dicomdir_unzip_dirname = '{0}/uuid_{1}'.format(
                                        self.locutus_settings.LOCUTUS_ONPREM_DICOM_ZIP_DIR, curr_uuid)
            print('{0}.Process(): PHASE03c: downloading dicom_dir to: {1} ...'.format(
                                        CLASS_PRINTNAME,
                                        dicomdir_zip_filename),
                                        flush=True)

            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: START of PHASE03c Accession# \'{1}\' DOWNLOAD from src-Orthanc'.format(
                                        nowtime,
                                        self.remove_trailing_zeros(curr_accession_num)),
                                        flush=True)

            # PROGRESS BAR UPDATES via MANIFEST_STATUS:
            interim_manifest_status = 'PROCESSING_CHANGE_at_PHASE03c_Downloading_from_Orthanc'
            self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                        'last_datetime_processed=now(), '\
                                        'manifest_status=\'{1}\' '\
                                        'WHERE accession_num=\'{2}\' '\
                                        '    AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                        interim_manifest_status,
                                        curr_accession_num))

            # TODO: consider re-authenticate within the RestToolbox whenever accessing EITHER Orthanc:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE03c: re-authenticating with src-Orthanc server...'.format(CLASS_PRINTNAME), flush=True)
            RestToolbox.SetCredentials(self.src_onprem_dicom_config['orthanc_user'],
                                        self.src_onprem_dicom_config['orthanc_password'])
            # and actually get the study URL from src-Orthanc:
            dicomdir_data = RestToolbox.DoGet(staged_url)

            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE03c Accession# \'{1}\' DOWNLOAD from src-Orthanc'.format(
                                        nowtime,
                                        self.remove_trailing_zeros(curr_accession_num)),
                                        flush=True)
            print('{0}.Process(): PHASE03c: downloaded dicom_dir with len={1}'.format(
                                        CLASS_PRINTNAME,
                                        len(dicomdir_data)),
                                        flush=True)

            with open(dicomdir_zip_filename, 'wb') as dicomdir_zip_file:
                dicomdir_zip_file.write(dicomdir_data)
                dicomdir_zip_file.close()


            ######################################
            # PRE-DELETE any anticipated OUTPUT from any already there.
            # NOTE: Now that the ZIP file has been downloaded,
            # clear out its destination dir (as determined by cropping off the ZIP_SUFFIX)
            # to ensure that no previously unzipped data still resides in there
            ######################################
            if dicomdir_unzip_dirname and os.path.exists(dicomdir_unzip_dirname):
                print('{0}.Process(): PHASE03d: removing *previously* unzipped dicom_dir {1} prior to unzipping ...'.format(
                                        CLASS_PRINTNAME,
                                        dicomdir_unzip_dirname),
                                        flush=True)
                shutil.rmtree(dicomdir_unzip_dirname)
            else:
                print('{0}.Process(): PHASE03d: no *previously* unzipped dicom_dir {1} to remove prior to unzipping ...'.format(
                                        CLASS_PRINTNAME,
                                        dicomdir_unzip_dirname),
                                        flush=True)
            ######################################
            # NOTE: See also such pre-delete prior to Phase04's dicom_anon
            # TODO: Consider such pre-delete after Phase04's dicom_anon and ZIP of De-ID'd into the LOCUTUS_TARGET_ISILON_PATH
            #   == Unlikely, as the ZIP will already be well isolated, due to the other 2x pre-deletes in Phase03 & Phase04,
            #   += with the possibility that the user has already added/modified contents of the UNZIPPED De-ID data in LOCUTUS_TARGET_ISILON_PATH,
            #       it may very well be best to leave that to the user to manage, rather than to suprisingly wipe out something unexpected. :-(
            ######################################

            # Processing Phase03d:
            # continue the process on into unzipping the identified DICOM:
            print('{0}.Process(): PHASE03d: unzipping dicom_dir {1} ...'.format(
                                        CLASS_PRINTNAME,
                                        dicomdir_zip_filename),
                                        flush=True)
            zip_ref = ZipFile(dicomdir_zip_filename, 'r')
            zip_ref.extractall(dicomdir_unzip_dirname)
            zip_ref.close()

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE03d: unzipped dicom_dir to: {1} ...'.format(
                                        CLASS_PRINTNAME,
                                        dicomdir_unzip_dirname),
                                        flush=True)

            if self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE:
                # share the following LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE-related message regardless of LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE03d+: LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE '\
                        'redacting Patient and Study levels in the freshly unzipped Archive hierarchy...'.format(
                                        CLASS_PRINTNAME),
                                        flush=True)
                # the above freshly unzipped dicom_dir likely contains PHI in the Patient and Study subdirs:
                # no need to retain the returned patient_top_dir in this case, but here it is anyhow:
                patient_top_dir = self.walk_and_redact_zip_archive_patient_study_levels(dicomdir_unzip_dirname, enable_redact_subdirs=True)

            # TODO: ENSURE that no errors occurred in any of the above, but
            # exceptions should just be raised if not explicitly checked.

            # Processing Phase03e:
            # Update the LOCUTUS_ONPREM_DICOM_STATUS_TABLE
            # with identified_local_path =  this hostname +  unzip_dirname,
            # and set the processing time on the one actually processed
            # (not needing the uuid AND change_seq_id, but including for safety):
            curr_uuid_id_hostpath = '{0}{1}{2}'.format(self.this_hostname, self.locutus_settings.HOSTPATH_DELIMITER, dicomdir_unzip_dirname)
            print('{0}.Process(): PHASE03: setting phase-{1} COMPLETE for uuid {2} '\
                                        'on its max change_seq_id == {3}!'.format(
                                        CLASS_PRINTNAME,
                                        (prev_phase_processed+1),
                                        curr_uuid,
                                        curr_uuid_max_change_seq_id),
                                        flush=True)

            ########################################## ########################################## ##########################################
            # NOTE: *** UPDATING at completion of Phase03 as well as Phase04 & Phase05 ***
            #       (***corresponding to CHECKING for Phase03 as well as Phase04 Sweep & Phase05 Sweep***)
            #   ... since this SECOND PASS of the Internal Configuration Versioning Management
            #       feature has expanded to support PARTIALLY-PROCESSED sessions.
            # Also check and compare against the internal configuration(s) used during processing:
            ###########
            # NOTE: saving these internal config values with EACH update of the session's status
            # upon EACH phase completion, such that they CAN be checked at Phase Sweeps for Phase04 & 5.
            # This is to help further prevent any inadvertant phase sweeps of sessions which had only been
            # partially processed previously (e.g., up to or through Phase04 before encountering an error),
            # but with different internal configuration(s).
            ###########
            int_cfg_status_update_fields = ''
            int_cfg_status_update_fields_str = ''
            if len(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES):
                ######
                # TODO: for any internal configuration versions with user-configurable overrides,
                # will need to include their configuration settings as well as the active defaults: LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES
                ######
                # NOTE: ONLY reset_others here at the initial update upon completion of Phase03:
                int_cfg_status_update_fields = self.generate_update_status_fields_for_internal_configs(
                                                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES,
                                                                curr_at_phase=(prev_phase_processed+1),
                                                                reset_others=True)
                # NOTE: since this is an optional field, preface with a comma only if fields to add:
                if int_cfg_status_update_fields:
                    int_cfg_status_update_fields_str = ',{0}'.format(','.join(int_cfg_status_update_fields))
            ###########

            # NOTE: no comma before {9} in the below, as it may NOT appear if int_cfg_status_update_fields_str is empty:
            self.LocutusDBconnSession.execute('UPDATE {0} '\
                                        'SET datetime_processed=now(), '\
                                        'phase_processed={1}, identified_local_path=\'{2}\', '\
                                        'subject_id=\'{3}\', '\
                                        'object_info_01=\'{4}\', '\
                                        'object_info_02=\'{5}\', '\
                                        'object_info_03=\'{6}\', '\
                                        'accession_num=\'{7}\' '\
                                        '{8} '\
                                        'WHERE change_seq_id={9} AND uuid=\'{10}\' '\
                                        '    AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        (prev_phase_processed+1),
                                        curr_uuid_id_hostpath,
                                        curr_subject_id,
                                        curr_object_info_01,
                                        curr_object_info_02,
                                        curr_object_info_03,
                                        curr_accession_num,
                                        int_cfg_status_update_fields_str,
                                        curr_uuid_max_change_seq_id,
                                        curr_uuid))

            # and remove the zip file, as it is no longer needed:
            if not self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES:
                os.remove(dicomdir_zip_filename)
            else:
                print('DEBUG: {0}.Process(): PHASE03: LOCUTUS_KEEP_INTERIM_FILES '\
                                        'leaving the identified zip == {1} for later DeID '\
                                        'comparison!....'.format(
                                        CLASS_PRINTNAME,
                                        dicomdir_zip_filename),
                                        flush=True)

            # Processing Phase03f:
            # NOTE: the above update on the actual max change_seq_id for
            # this uuid has already updated its own phase_processed to 3.
            # Now that PHASE03 is done w/ this uuid, update any other of the
            # uuid's other PHASE02 change_seq_ids (NO LONGER w/ as_change_seq_id)
            # to this uuid's max(change_seq_id),
            # AND reset their phase_processed to NULL to help distinguish as a secondary change_seq_id.
            #
            # NO LONGER: towards the consolidated/collapsed/compressed/aggregated via as_change_seq_id.
            # [See also: CHANGE-SEQUENCE CONSOLIDATION:]
            #
            # TODO: ENSURE that no errors occurred in any of the above Phase03d, etc.!!!!
            # ===> Need to check on return status, or caught exceptions, etc.,
            # but for now, simply marking these as complete....
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('DEBUG: {0}.Process(): NO LONGER setting all unprocessed change_seq_ids for '\
                                        'uuid {1} in LOCUTUS_ONPREM_DICOM_STATUS_TABLE to its '\
                                        'max change_seq_id == {2} BECAUSE WE NOW WANT as_change_seq_ids GONE!'.format(
                                        CLASS_PRINTNAME,
                                        curr_uuid,
                                        curr_uuid_max_change_seq_id),
                                        flush=True)

            # processRemainingPhases if no errors in this phase so far
            if errors_encountered == 0 and processRemainingPhases:
                print('{0}.Process(): PHASE03: processRemainingPhases, calling Process function for PHASE04...'.format(CLASS_PRINTNAME), flush=True)
                prev_phase_processed += 1

                curr_uuid_errs = 0
                caught_exception = None
                caught_exception_msg = None
                err_desc = ''
                try:
                    (this_errors_encountered, this_errors_message2return, curr_deid_qc_api_study_url) = self.Process_Phase04_DeidentifyAndZip(
                                                    prev_phase_processed,
                                                    curr_uuid_max_change_seq_id,
                                                    curr_uuid,
                                                    curr_accession_num,
                                                    curr_uuid_id_hostpath,
                                                    manifest_deid_qc_status,
                                                    processRemainingPhases)
                    curr_uuid_errs += this_errors_encountered
                    #errors_encountered += this_errors_encountered
                    if (errors_encountered > 1) and (len(errors_message2return) > 0):
                        errors_message2return += ';'
                    errors_message2return += this_errors_message2return
                except Exception as locally_caught_exception:
                    safe_locally_caught_exception_str = str(locally_caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                    caught_exception = '{0}'.format(safe_locally_caught_exception_str)
                    print('{0}.Process(): PHASE03->04: ERROR: Caught Exception \'{1}\' '\
                                        'calling Process_Phase04_DeidentifyAndZip() for uuid \'{2}\''.format(
                                        CLASS_PRINTNAME,
                                        caught_exception,
                                        curr_uuid),
                                        flush=True)
                    #######################
                    # r3m0: WARNING: SQL_UPDATE_QUOTE_TO_REPLACE replacement happens TWICE here
                    # TODO = Q: is that as expected/needed? If not, then tidy this up!
                    # NOTE: this is in both GCP and OnPrem modules (since OnPrem ^C/^V'd from GCP during its Level-Up)
                    #######################
                    # at least save some bits from the last exception into status:
                    safe_exception_str = str(caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                    err_desc = '{0}'.format(safe_exception_str)
                    # save and re-raise it after updating manifest_status
                    #errors_encountered += 1
                    curr_uuid_errs += 1
                    if (errors_encountered > 1) and (len(errors_message2return) > 0):
                        errors_message2return += ';'
                    errors_message2return += err_desc

                if curr_uuid_errs > 0:
                    errors_encountered += curr_uuid_errs
                    # TODO: further logging of actual error/exception, etc:
                    # NOTE: only prepend current ERROR_PHASE if not already prefixed w/ ERROR_PHASE:
                    manifest_status = err_desc
                    if (len(err_desc) < len(self.locutus_settings.ERR_PHASE_PREFIX)) or (err_desc[:len(self.locutus_settings.ERR_PHASE_PREFIX)] != self.locutus_settings.ERR_PHASE_PREFIX):
                        # NOTE: manifest_status MUST start with ERR_PHASE_PREFIX = 'ERROR_PHASE' for this to work.
                        # only prepend ERROR_PHASE04via03 if a phase isn't already noted:
                        manifest_status = 'ERROR_PHASE04via03=[{0}]'.format(err_desc)
                        print('DEBUG: =====>  r3m0: =====>  ERROR_PHASE04via03 manifest_status={0}'.format(manifest_status), flush=True)

                    self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                'last_datetime_processed=now(), '\
                                                'manifest_status=\'{1}\' '\
                                                'WHERE accession_num=\'{2}\' '\
                                                '    AND active ;'.format(
                                                LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                manifest_status.replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT),
                                                curr_accession_num))

        nowtime = datetime.datetime.utcnow()
        print('@ {0} UTC: END of PHASE03 Accession# \'{1}\''.format(
                                                    nowtime,
                                                    self.remove_trailing_zeros(curr_accession_num)),
                                                    flush=True)
        print('{0}.Process(): PHASE03 ending for this uuid.'.format(CLASS_PRINTNAME), flush=True)
        return (errors_encountered, errors_message2return, curr_deid_qc_api_study_url)
        # end of Process_Phase03_DownloadFromOrthanc()


    def Process_Phase04_DeidentifyAndZip(self,
                                        prev_phase_processed,
                                        curr_uuid_change_seq_id,
                                        curr_uuid,
                                        curr_accession_num,
                                        curr_uuid_id_hostpath,
                                        manifest_deid_qc_status,
                                        processRemainingPhases):
        errors_encountered = 0
        errors_message2return = ''
        curr_deid_qc_study_url = ''
        curr_uuid_deid_hostpath = ''

        print('{0}.Process(): PHASE04: Process_Phase04_DeidentifyAndZip() called with '\
                                        'phase-{1} processed uuid=={2} '\
                                        'for change_seq_id=={3} '\
                                        'w/ ID-dir==\'{4}\' and manifest_deid_qc_status=\'{5}\'!'.format(
                                        CLASS_PRINTNAME,
                                        prev_phase_processed,
                                        curr_uuid,
                                        curr_uuid_change_seq_id,
                                        curr_uuid_id_hostpath,
                                        manifest_deid_qc_status),
                                        flush=True)

        nowtime = datetime.datetime.utcnow()
        print('@ {0} UTC: START of PHASE04 Accession# \'{1}\''.format(
            nowtime,
            self.remove_trailing_zeros(curr_accession_num)),
            flush=True)

        curr_uuid_id_images_path = '[N/A:{0}]'.format(UNDER_REVIEW_STATUS)


        # regardless of self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE,
        # allow a first-pass through this Phase04 to use the curr_uuid_id_hostpath, if available:
        # NOTE: prior to 3/24/2025 LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE was absent ~= False == DICOMDIR (flat IMAGES/IM** structure) = LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE
        if curr_uuid_id_hostpath and not self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE:
            # strip off the hostname & path, and ensure that it exists/matches:
            (curr_uuid_id_host, curr_uuid_id_path) = curr_uuid_id_hostpath.split(self.locutus_settings.HOSTPATH_DELIMITER)
            curr_uuid_id_images_path = os.path.join(curr_uuid_id_path, 'IMAGES')

            if not (os.path.exists(curr_uuid_id_images_path) and os.path.isdir(curr_uuid_id_images_path)):
                # TODO: Q: throw an exception here? or just log for now?
                errors_message2display = ''
                errors_encountered += 1
                if errors_encountered > 1:
                    errors_message2return += ';'
                errors_message2return += 'ERROR_PHASE04=[this uuid identified directory IMAGE subdir was NOT found on this host'
                if ((not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE) \
                    and (not self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS)):
                    errors_message2display = ' (!LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE but !LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS)'
                    errors_message2return += '(pause4manual but NOT use_manifest_QC_status)'
                errors_message2return += ']'

                print('{0}.Process(): PHASE04: ERROR: uuid\'s identified '\
                                'directory\'s IMAGE subdir {1} does NOT exist here{2}.'.format(
                                CLASS_PRINTNAME,
                                curr_uuid_id_images_path,
                                errors_message2display),
                                flush=True)
                if curr_uuid_id_host != self.this_hostname:
                    print('{0}.Process(): PHASE04: WARNING: uuid\'s ID directory host ({1}) '\
                                    'does not match current host ({2})'.format(
                                    CLASS_PRINTNAME,
                                    curr_uuid_id_host,
                                    self.this_hostname),
                                    flush=True)
        elif curr_uuid_id_hostpath and self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE:
            # emit a little INFO blurb, whether or not LOCUTUS_VERBOSE:
            print('{0}.Process(): PHASE04: INFO: LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE '\
                'Overridding uuid\'s url_dicomdir to USE zip Archive already downladed from endpoint ({1})'.format(
                        CLASS_PRINTNAME,
                        self.locutus_settings.ORTHANC_DOWNLOAD_ZIP_ENDPOINT),
                        flush=True)
            # Rather than changing any of the Staging tables at this time,
            # merely programmatically override from the DICOMDIR media endpoint
            # to use the zip Archive archive endpoint,
            # strip off the hostname & path, and ensure that it exists/matches:
            (curr_uuid_id_host, curr_uuid_id_path) = curr_uuid_id_hostpath.split(self.locutus_settings.HOSTPATH_DELIMITER)
            curr_uuid_id_images_path = curr_uuid_id_path
            #######
            print('{0}.Process(): PHASE04: INFO: LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE '\
                'Looking at curr_uuid_id_images_path={1}'.format(
                        CLASS_PRINTNAME,
                        curr_uuid_id_images_path),
                        flush=True)

            #######
            # NOTE: utilize a new variation of walk_and_redact_zip_archive_patient_study_levels(),
            # to only walk and return patient_topdir via redact_subdirs=False:
            #######
            patient_topdir = self.walk_and_redact_zip_archive_patient_study_levels(curr_uuid_id_images_path, enable_redact_subdirs=False)
            curr_uuid_id_images_path = patient_topdir

        if not errors_encountered:
            # Phase04a = De-identification
            deidentified_dirname = '{0}/uuid_{1}'.format(
                                        self.locutus_settings.LOCUTUS_ONPREM_DICOM_DEID_DIR, curr_uuid)

            # NOTE: if LOCUTUS_TARGET_ISILON_PATH is defined (whether or not any other targets are also defined),
            # could run dicom_anon straight to it, bypassing the need for any subsequent copying.
            # With deidentified_dir_zip_filename being generated thereafter, though,
            # leaving dicom_anon to write to: LOCUTUS_ONPREM_DICOM_DEID_DIR
            # to subsequently zip it up to: LOCUTUS_TARGET_ISILON_PATH
            #
            # In that manner, Phase04 can still remove all interim files, regardless.
            #
            # NOTE: this does NOT yet take into account any of the intracies around the new QC stuff with either:
            #   self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE
            #   self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS

            # Get the patient+study info for this uuid to properly replace PatientsName/ID w/ subject_id:
            patient_study_info_result = self.LocutusDBconnSession.execute('SELECT '\
                                                        'subject_id, object_info_01, '\
                                                        'object_info_02, object_info_03, '\
                                                        'deid_qc_status, '\
                                                        'deid_qc_api_study_url, '\
                                                        'deid_qc_explorer_study_url '\
                                                        'FROM {0} WHERE change_seq_id={1} '\
                                                        'AND uuid=\'{2}\' '\
                                                        '    AND active ;'.format(
                                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                        curr_uuid_change_seq_id,
                                                        curr_uuid))
            row = patient_study_info_result.fetchone()
            if row is None:
                print('{0}.Process(): PHASE04: ERROR: found NO patient/study info '\
                                'for change_seq_id={1} AND uuid={2} in {3}!'.format(
                                CLASS_PRINTNAME,
                                curr_uuid_max_change_seq_id,
                                curr_uuid,
                                STAGER_STABLESTUDY_TABLE),
                                flush=True)
                # NOTE: return without any further updates or processing of this uuid:
                errors_encountered += 1
                if errors_encountered > 1:
                    errors_message2return += ';'
                errors_message2return += 'ERROR_PHASE04=[found NO patient/study info for this uuid change_seq_id={0}]'.format(curr_uuid_max_change_seq_id)

                print('{0}.Process(): PHASE04 erroring out and ending for this uuid.'.format(CLASS_PRINTNAME), flush=True)
                return (errors_encountered, errors_message2return)

            curr_subject_id = row['subject_id']
            curr_object_info_01 = row['object_info_01']
            curr_object_info_02 = row['object_info_02']
            curr_object_info_03 = row['object_info_03']
            curr_deid_qc_status = row['deid_qc_status']
            curr_deid_qc_api_study_url = row['deid_qc_api_study_url']
            curr_deid_qc_explorer_study_url = row['deid_qc_explorer_study_url']

            # NOTE: may want a different replacement_patient_info, but here is an initial possibility, and...
            # TODO: consider the usefulness of having included the "..._via_{CLASS_PRINTNAME}" suffix.
            # An early example:
            #   curr_replacement_patient_info = '{0}_{1}_via_{2}'.format(curr_subject_id, curr_object_info_01, CLASS_PRINTNAME)
            # BUT: return to a most simple form of the subject ID
            # (as prefaced by the optional LOCUTUS_ONPREM_DICOM_SUBJECT_ID_PREFACE, which should end with something like `_`):

            # NOTE: bypass the SUBJECT_ID_PREFACE if !DEID_DISABLE and USE_MANIFEST_QC and qc_status=='PASS:*':
            # NOTE: the curr_deid_qc_status might NOT yet have been saved to the STATUS table,
            # so use its active manifest input value:
            curr_replacement_patient_info = ''
            if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE \
                and self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS \
                and manifest_deid_qc_status \
                and manifest_deid_qc_status.find('PASS:') == 0:
                # NOTE: emit WARNING of omitting the subject_ID_preface:
                print('{0}.Process(): PHASE04: WARNING: ignoring subject_ID_preface (\'{1}\') '\
                                'since using MANIFEST_QC_STATUS and PASSing QC_status provided (\'{2}\')'.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_SUBJECT_ID_PREFACE,
                                manifest_deid_qc_status),
                                flush=True)
                curr_replacement_patient_info = '{0}'.format(curr_subject_id)
            else:
                # NOTE: emit NOTE of using the subject_ID_preface:
                print('{0}.Process(): PHASE04: NOTE: using subject_ID_preface (\'{1}\') '\
                                'since NOT using a MANIFEST_QC_STATUS with PASSing QC_status (\'{2}\')'.format(
                                CLASS_PRINTNAME,
                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_SUBJECT_ID_PREFACE,
                                manifest_deid_qc_status),
                                flush=True)
                curr_replacement_patient_info = '{0}{1}'.format(self.locutus_settings.LOCUTUS_ONPREM_DICOM_SUBJECT_ID_PREFACE, \
                                                            curr_subject_id)
                # NOTE: emit NOTE of the curr_replacement_patient_info:
                print('{0}.Process(): PHASE04: NOTE: preparing to inject DICOM Patient '\
                                'curr_replacement_patient_info: \'{1}\''.format(
                                CLASS_PRINTNAME,
                                curr_replacement_patient_info),
                                flush=True)

            if ((not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE) and \
                (not self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS) and \
                curr_deid_qc_status and (curr_deid_qc_status.find(UNDER_REVIEW_STATUS) == 0) and \
                curr_deid_qc_api_study_url and curr_deid_qc_explorer_study_url):
                # already UNDER_REVIEW, but not yet set to process with USE_MANIFEST_QC_STATUS:
                manifest_status = UNDER_REVIEW_STATUS
                err_desc = 'Accession# \'{0}\' already {1}; awaiting either LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS '\
                        'or LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS'.format(curr_accession_num, UNDER_REVIEW_STATUS)
                print('{0}.Process(): WARNING: PHASE04: {1}'.format(
                        CLASS_PRINTNAME,
                        err_desc),
                        flush=True)
                # NOTE: do not increment errors_encountered += 1
                # BUT do still halt processing of any subsequent phases for this manual DeID QC step:
                processRemainingPhases = False

                # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                curr_deid_qc_options = UNDER_REVIEW_QC_OPTIONS
                print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        curr_subject_id,
                        curr_object_info_01,
                        curr_object_info_02,
                        curr_object_info_03,
                        self.remove_trailing_zeros(curr_accession_num),
                        curr_deid_qc_options,
                        '',
                        manifest_status,
                        curr_deid_qc_explorer_study_url,
                        '',
                        ''), flush=True)

            else:
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.Process(): PHASE04: confirmed this uuid\'s identified dir, deidentifying....'.format(CLASS_PRINTNAME), flush=True)
                    print('{0}.Process(): PHASE04: using input ID dir=\'{1}\''.format(
                                        CLASS_PRINTNAME,
                                        curr_uuid_id_images_path),
                                        flush=True)
                    print('{0}.Process(): PHASE04: using output de-ID dir=\'{1}\''.format(
                                        CLASS_PRINTNAME,
                                        deidentified_dirname),
                                        flush=True)
                    print('{0}.Process(): PHASE04: and using subject_ID_preface=\'{1}\' with subject_ID=\'{2}\' '\
                                        'for a total replacement subject_ID of \'{3}\''.format(
                                        CLASS_PRINTNAME,
                                        self.locutus_settings.LOCUTUS_ONPREM_DICOM_SUBJECT_ID_PREFACE,
                                        curr_subject_id,
                                        curr_replacement_patient_info),
                                        flush=True)

                print('{0}.Process(): PHASE04a: deidentifying IMAGES for uuid == {1} '\
                                            'FROM {2} TO {3} ....'.format(
                                            CLASS_PRINTNAME,
                                            curr_uuid,
                                            curr_uuid_id_images_path,
                                            deidentified_dirname),
                                            flush=True)
                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: START of PHASE04a Accession# \'{1}\' DEIDENTIFY-DICOM with dicom_anon'.format(
                    nowtime,
                    self.remove_trailing_zeros(curr_accession_num)),
                    flush=True)

                # PROGRESS BAR UPDATES via MANIFEST_STATUS:
                interim_manifest_status = 'PROCESSING_CHANGE_at_PHASE04a_DeIDing_with_dicom-anon'
                self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                            'last_datetime_processed=now(), '\
                                            'manifest_status=\'{1}\' '\
                                            'WHERE accession_num=\'{2}\' '\
                                            '    AND active ;'.format(
                                            LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                            interim_manifest_status,
                                            curr_accession_num))

                ######################################
                # PRE-DELETE any anticipated OUTPUT from any already there.
                # NOTE: Now that the ZIP file destination has been downloaded & unzipped,
                # clear out its De-ID destination dir for dicom_anon
                # (as determined by cropping off the ZIP_SUFFIX)
                # to ensure that no previously De-ID'd data still resides in there
                ######################################
                if deidentified_dirname and os.path.exists(deidentified_dirname):
                    print('{0}.Process(): PHASE04: removing *previously* De-IDed deidentified_dirname {1} prior to dicom_anon ...'.format(
                                            CLASS_PRINTNAME,
                                            deidentified_dirname),
                                            flush=True)
                    shutil.rmtree(deidentified_dirname)
                else:
                    print('{0}.Process(): PHASE04: no *previously* De-IDed deidentified_dirname {1} to remove prior to dicom_anon ...'.format(
                                            CLASS_PRINTNAME,
                                            deidentified_dirname),
                                            flush=True)
                ######################################
                # NOTE: See also such pre-delete prior to Phase03's download and UNZIP of ID'd
                # TODO: Consider such pre-delete after Phase04's dicom_anon and ZIP of De-ID'd into the LOCUTUS_TARGET_ISILON_PATH
                #   == Unlikely, as the ZIP will already be well isolated, due to the other 2x pre-deletes in Phase03 & Phase04,
                #   += with the possibility that the user has already added/modified contents of the UNZIPPED De-ID data in LOCUTUS_TARGET_ISILON_PATH,
                #       it may very well be best to leave that to the user to manage, rather than to suprisingly wipe out something unexpected. :-(
                ######################################

                dicom_anon_Popen_args = [
                    'python3',
                    './src_3rdParty/dicom_anon.py',
                    '--spec_file',
                    DEFAULT_DICOM_ANON_SPEC_FILE,
                    '--modalities',
                    DEFAULT_DICOM_ANON_MODALITIES_STR,
                    '--force_replace',
                    curr_replacement_patient_info,  # for any 'R' specs (e.g., PatientsName & PatientID) in the dicom_anon_spec_file
                    '--exclude_series_descs',
                    DICOM_SERIES_DESCS_TO_EXCLUDE,
                    '{0}'.format(curr_uuid_id_images_path),
                    '{0}'.format(deidentified_dirname)
                ]

                if self.locutus_settings.LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP:
                    dicom_anon_alignment_mode = '--alignment_mode_GCP_forBGD'
                    # NOTE: "DicomAnon::" DEBUG crumbs, as per those from dicom_anon itself:
                    print('{0}.Process(): PHASE04 DEBUG: pre-DicomAnon:: Added "{1}" to the initial dicom_anon_Popen_args set: "{2}"'.format(
                            CLASS_PRINTNAME, dicom_anon_alignment_mode, dicom_anon_Popen_args), flush=True)
                    dicom_anon_Popen_args.append(dicom_anon_alignment_mode)
                # NOTE: "DicomAnon::" DEBUG crumbs, as per those from dicom_anon itself:
                print('{0}.Process(): PHASE04 DEBUG: pre-DicomAnon:: calling dicom_anon with new dicom_anon_Popen_args for a total set: "{1}"'.format(
                        CLASS_PRINTNAME, dicom_anon_Popen_args), flush=True)

                proc = Popen(dicom_anon_Popen_args, stdout=PIPE, stderr=PIPE)
                (stdoutdata, stderrdata) = proc.communicate()

                # TODO: may want to consider wrapping these DEBUG prints in a LOCUTUS_VERBOSE check
                # but for now, print regardless of whether or not any errors occurred:
                print('{0}.Process(): PHASE04: DEBUG: stdoutdata from the above dicom_anon returned: [{1}]'.format(
                                CLASS_PRINTNAME,
                                stdoutdata),
                                flush=True)
                print('{0}.Process(): PHASE04: DEBUG: stderrdata from the above dicom_anon returned: [{1}]'.format(
                                CLASS_PRINTNAME,
                                stderrdata),
                                flush=True)

                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: END of PHASE04a Accession# \'{1}\' DEIDENTIFY-DICOM with dicom_anon'.format(
                    nowtime,
                    self.remove_trailing_zeros(curr_accession_num)),
                    flush=True)

                internal_DEBUG_simulate_failed_phase04_dicom_anon = False
                # Enable the following internal DEBUG_ to force fail on Phase04 dicom_anon
                #internal_DEBUG_simulate_failed_phase04_dicom_anon = True
                if internal_DEBUG_simulate_failed_phase04_dicom_anon:
                    proc.returncode = "DEBUG_simulate_failed_phase04_dicom_anon"
                    print('{0}.Process(): PHASE04: DEBUG: setting internal_DEBUG_simulate_failed_phase04_dicom_anon={1} '\
                                                        'AND proc.returncode={2} to test phase ERROR!'.format(
                                                        CLASS_PRINTNAME,
                                                        internal_DEBUG_simulate_failed_phase04_dicom_anon,
                                                        proc.returncode),
                                                        flush=True)

                if proc.returncode == 0 and not os.path.isdir(deidentified_dirname):
                    # dicom_anon might not have generated any errors since it is doing what it is told,
                    # but if all modalities in the source directory are unsupported, for example,
                    # then no output directory will be created, and the show can't go on for this session.
                    # Rather than throw the following ValueError here though:
                    #
                    #raise ValueError('Directory not found for dicom_anon output dir of "{0}".'.format(
                    #            deidentified_dirname))
                    #
                    # instead, merely set the proc.returncode to force an error
                    # that the subsequent code can process, according to the eventually possible self.locutus_settings.LOCUTUS_FORCE_SUCCESS:
                    #
                    # NOTE: although proc.returncode might typically be numeric,
                    # it might be A-Okay to give a bit more information here:
                    proc.returncode = 'dicom_anon did not create the expected output directory (e.g., if all input modalities are unsupported)'

                if proc.returncode != 0:
                    print('{0}.Process(): PHASE04: ERROR: the above proc.communicate to dicom_anon '\
                                    'just returned non-zero error code of: {1}'.format(
                                    CLASS_PRINTNAME,
                                    proc.returncode),
                                    flush=True)
                    # TODO: Q: throw/raise an exception here? or just log for now?
                    errors_encountered += 1
                    if (errors_encountered > 1) and (len(errors_message2return) > 0):
                        errors_message2return += ';'
                    errors_message2return += 'ERROR_PHASE04=[dicom_anon returned non-zero error code of {0}]'.format(proc.returncode)

                    ##### ##### ##### ##### #####
                    # r3m0: NOTE:
                    # code block #1 of such FORCE_SUCCESS, straight from GCP:
                    # TODO: read through and test this, then add other such FORCE_SUCCESS blocks
                    ##### ##### ##### ##### #####
                    # TODO: test on an accession with a legitimate dicom_anon Unexpected Value error.
                    #   NOTE: consider also including the stderr into the following exception,
                    #   except that it might likely be much too much verbose!
                    #   so, might still reference the more verbose log via: (see run log for specific details)
                    # TODO: integrate locutus_force_success around this.
                    #
                    # NOTE: throwing an exception here may very well halt processsing,
                    # at least, unless force_success is enabled:
                    if self.locutus_settings.LOCUTUS_FORCE_SUCCESS:
                        print('{0}.Process(): LOCUTUS_FORCE_SUCCESS enabled; suppressing a ValueError() '\
                                    'for the above dicom_anon non-zero error code.'.format(
                                    CLASS_PRINTNAME),
                                    flush=True)
                        # NOTE: at this point the processing should NOT be hard-halted immediately
                        # with the below ValueError() exception,
                        # but should still fall back to a soft error with this method returning
                        # errors_encountered > 0.
                        #
                        # NOTE: with this more robust approach currently comes a downside,
                        # in that the details of the suppressed exception are not yet saved
                        # back into the DICOM_MANIFEST table, which simply shows as something like:
                        #       "... non-exception errors returned from ..."
                    else:
                        new_exception_msg = 'PHASE04: ERROR: '\
                                            'dicom_anon '\
                                            'error: {1} (see run log for further details)'.format(
                                            CLASS_PRINTNAME,
                                            proc.returncode)
                        print('{0}.Process() throwing ValueError exception: {1}.'.format(
                            CLASS_PRINTNAME,
                            new_exception_msg),
                            flush=True)
                        # 9/24/2024: NOTE: added these close() before the raise() (highlighting in case needing to alter):
                        #self.manifest_infile.close()
                        #self.LocutusDBconnSession.close()
                        #self.StagerDBconnSession.close()
                        raise ValueError(new_exception_msg)
                    ##### ##### ##### ##### #####

                else:
                    # i.e., IF proc.returncode == 0, following a seemingly successful DeID via dicom_anon:
                    nowtime = datetime.datetime.utcnow()
                    print('@ {0} UTC: START of PHASE04b Accession# \'{1}\' Re-ZIP post dicom_anon'.format(
                        nowtime,
                        self.remove_trailing_zeros(curr_accession_num)),
                        flush=True)

                    # Phase04b: RE-ZIP the de-id'd dir,
                    # for uploading to s3 as one file:
                    deidentified_dir_zip_filename = '{0}.zip'.format(deidentified_dirname)

                    if self.locutus_settings.LOCUTUS_TARGET_USE_ISILON:
                        # NOTE: could have mixed this in as an if/else, but nice to still have a LOCUTUS_ONPREM_DICOM_DEID_DIR default set above
                        #
                        # NOTE: if LOCUTUS_TARGET_ISILON_PATH is defined (whether or not any other targets are also defined),
                        # could have run dicom_anon straight to it, bypassing the need for any subsequent copying.
                        # With this deidentified_dir_zip_filename being generated thereafter, though,
                        # leaving dicom_anon to write to: LOCUTUS_ONPREM_DICOM_DEID_DIR
                        # to subsequently zip it up to: LOCUTUS_TARGET_ISILON_PATH
                        #
                        # NOTE: this does NOT yet take into account any of the intricacies around the new QC stuff with either:
                        #   self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE
                        #   self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS
                        #
                        # FROM Phase04a = De-identification
                        #deidentified_dirname = '{0}/uuid_{1}'.format(
                        #                            self.locutus_settings.LOCUTUS_ONPREM_DICOM_DEID_DIR, curr_uuid)

                        # NOTE the NOTE:
                        # NOTE: first, need to ensure that deidentified_dirname exists via a mkdir:
                        if not os.path.exists(self.locutus_settings.LOCUTUS_TARGET_ISILON_PATH):
                            print('{0}.Process(): PHASE04b: making target isilon deid zip dir {1} ...'.format(
                                        CLASS_PRINTNAME,
                                        self.locutus_settings.LOCUTUS_TARGET_ISILON_PATH),
                                        flush=True)
                            # Living bravely, not checking the mkdir output,
                            # but..... it will be caught by the call to zip_deid_ref = ZipFile(, 'w')
                            # still, TODO: check the os.mkdir() status ;-)
                            os.makedirs(self.locutus_settings.LOCUTUS_TARGET_ISILON_PATH)


                        # AS INSPIRED BY the subsequent if self.locutus_settings.LOCUTUS_TARGET_USE_S3:
                        #######
                        # s3 keyname: <TOP_LEVEL>/<subject_id>/<object_info_01>/<object_info_02>/<object_info_03>/uuid_<uuid#>.zip
                        # TODO: eventually expand to handle a dynamic number of object_infos, but for now:
                        #s3_bucket_keyname = '{0}/{1}/{2}/{3}/{4}/uuid_{5}.zip'.format(
                        #                self.locutus_settings.LOCUTUS_ONPREM_DICOM_BUCKET_PATH_TOP_LEVEL,
                        #                curr_subject_id, curr_object_info_01,
                        #                curr_object_info_02, curr_object_info_03, curr_uuid)
                        #######
                        # allow the OnPrem LOCUTUS_TARGET_ISILON_PATH a similar such subject subdir:
                        #subject_subdir = '{0}_{1}_{2}_{3}'.format(
                        #                curr_subject_id, curr_object_info_01,
                        #                curr_object_info_02, curr_object_info_03)
                        # OR, perhaps better for general sparse object_infos, with a primitive brute force concatentation of each:
                        subject_subdir = '{0}'.format(curr_subject_id)
                        if curr_object_info_01:
                            subject_subdir += '_{0}'.format(curr_object_info_01)
                        if curr_object_info_02:
                            subject_subdir += '_{0}'.format(curr_object_info_02)
                        if curr_object_info_03:
                            subject_subdir += '_{0}'.format(curr_object_info_03)
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): Phase04b DeID zip using subject_subdir: {1}'.format(
                                                    CLASS_PRINTNAME,
                                                    subject_subdir),
                                                    flush=True)

                        deidentified_dir_zip_filename = '{0}/{1}/uuid_{2}.zip'.format(
                            self.locutus_settings.LOCUTUS_TARGET_ISILON_PATH,
                            subject_subdir,
                            curr_uuid)

                        # NOTE: new and improved makedirs for the LOCUTUS_TARGET_ISILON_PATH:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): Phase04b DeID zip Ensuring LOCUTUS_TARGET_ISILON_PATH exists as: {1}/{2}'.format(
                                                    CLASS_PRINTNAME,
                                                    self.locutus_settings.LOCUTUS_TARGET_ISILON_PATH,
                                                    subject_subdir),
                                                    flush=True)
                        try:
                            os.makedirs(self.locutus_settings.LOCUTUS_TARGET_ISILON_PATH+'/'+subject_subdir)
                        except OSError as e:
                            if e.errno != errno.EEXIST:
                                raise

                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.Process(): Phase04b DeID zip: creating de-ID zip={1}'.format(
                                    CLASS_PRINTNAME,
                                    deidentified_dir_zip_filename),
                                    flush=True)
                    zip_deid_ref = ZipFile(deidentified_dir_zip_filename, 'w')

                    # NOTE: following len calculation is on the base dir
                    # such that only the uuid_<curr_uuid> path will remain in the zip
                    # (could instead calculate the len all the way down to deidentified_dirname,
                    # IF only wanting the IMAGES to be in the zip,
                    # without being prefaced by the parent uuid_<curr_uuid> path):
                    len_deid_basedir = len(self.locutus_settings.LOCUTUS_ONPREM_DICOM_DEID_DIR)

                    # NOTE: with only the IMAGES subfolder underneath,
                    # can easily just add these right here without a full zip function:
                    image_counter = 0
                    for (rootArchiveDirPath, dirNames, fileNames) in os.walk(deidentified_dirname):
                        for fileName in fileNames:
                            image_counter += 1
                            deidentified_dir_filepath = os.path.join(rootArchiveDirPath, fileName)
                            deidentified_arc_filename = deidentified_dir_filepath[len_deid_basedir :]
                            # NOTE: the following is too verbose for even the VERBOSE mode! ;-)
                            #if self.locutus_settings.LOCUTUS_VERBOSE:
                            #    print('{0}.Process(): Phase04b DeID zip: Adding to zip arcname={1} ....'.format(
                            #           CLASS_PRINTNAME,
                            #           deidentified_arc_filename),
                            #           flush=True)
                            zip_deid_ref.write(deidentified_dir_filepath, deidentified_arc_filename)
                    nowtime = datetime.datetime.utcnow()
                    print('@ {0} UTC: Re-ZIPPED {1} De-Identified DICOM image file(s)'.format(
                        nowtime,
                        image_counter), flush=True)

                    # NOTE: for a full and more robust zip implementation, see either:
                    # https://peterlyons.com/problog/2009/04/zip-dir-python
                    # https://stackoverflow.com/questions/16091904/python-zip-how-to-eliminate-absolute-path-in-zip-archive-if-absolute-paths-for
                    # or instead use shutil.make_archive() as per: https://docs.python.org/2/library/shutil.html#shutil.make_archive
                    zip_deid_ref.close()

                    nowtime = datetime.datetime.utcnow()
                    print('@ {0} UTC: END of PHASE04b Accession# \'{1}\' Re-ZIP post dicom_anon'.format(
                        nowtime,
                        self.remove_trailing_zeros(curr_accession_num)),
                        flush=True)

                    # === PASS:*
                    # NOTE: that curr_deid_qc_status might be NULL from disk,
                    # so be sure to reference the input manifest_deid_qc_status:
                    if (self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE \
                        or (self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS \
                            and (manifest_deid_qc_status \
                                and (manifest_deid_qc_status.find('PASS:') == 0) \
                                ) \
                            ) \
                        ):
                        # IF so, the Manual DeID QC step is DISABLED,
                        #   OR we are performing a full PASS:* reprocess, bypassing another such Manual DeID QC step.
                        # so now allow for the below wrapup of checking/leaving interim files,
                        # updating the DB, and calling Phase05:
                        next_prev_phase_processed = (prev_phase_processed+1)

                        # Phase04c, updates once done w/ Phase04a & 4b:
                        #  * update the LOCUTUS_ONPREM_DICOM_STATUS_TABLE with the deidentified_dir_zip_filename
                        #       with an updated processing time and incremented phase_completed,
                        #       just as the Phase03e had done for the identified location,
                        #       and can now also set identified_local_path=NULL
                        print('{0}.Process(): PHASE04: setting phase-{1} COMPLETE for uuid {2}!'.format(
                                    CLASS_PRINTNAME,
                                    (prev_phase_processed+1),
                                    curr_uuid),
                                    flush=True)
                    else:
                        # i.e., if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE,
                        # the Manual DeiD QC step is active. For the rest of Phase04, then.....
                        print('{0}.Process(): PHASE04: MANUAL_DEID_PAUSE4MANUAL_QC: referencing existing '\
                                                    'deid_qc_status=\'{1}\'...'.format(
                                                    CLASS_PRINTNAME,
                                                    curr_deid_qc_status), flush=True)

                        #############################################
                        # Manual DeID QC: UPLOAD to Orthanc DeID QC

                        ########
                        # NOTE: no need for the following:
                        #print('{0}.Process(): PHASE04: DEBUG: NOW authenticating with the new ONPREM De-ID QC Orthanc server; '\
                        #                            'NEXT UPLOADING...'.format(CLASS_PRINTNAME), flush=True)
                        # NOTE: re-authenticate via the RestToolbox whenever accessing EITHER Orthanc:
                        #if self.locutus_settings.LOCUTUS_VERBOSE:
                        #    print('{0}.Process(): PHASE04: re-authenticating with qc-Orthanc server...'.format(CLASS_PRINTNAME), flush=True)
                        #RestToolbox.SetCredentials(self.qc_onprem_dicom_config['orthanc_user'],
                        #                           self.qc_onprem_dicom_config['orthanc_password'])
                        ########
                        # no need to re-authenticate at this point, since passing the creds onto a curl call via:
                        # copy_local_directory_to_Orthanc()
                        ########

                        nowtime = datetime.datetime.utcnow()
                        print('@ {0} UTC: START of PHASE04 Accession# \'{1}\' UPLOAD to qc-Orthanc'.format(
                                            nowtime,
                                            self.remove_trailing_zeros(curr_accession_num)),
                                            flush=True)

                        deid_qc_uploaded_api_study_url = None
                        deid_qc_uploaded_explorer_study_url = None

                        try:
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('{0}.Process(): PHASE04-deid-qc: About to copy_local_directory_to_Orthanc('\
                                    'local_path=\'{1}\', deid_qc_onprem_dicom_orthanc_url=\'{2}\')...'.format(
                                    CLASS_PRINTNAME,
                                    deidentified_dirname,
                                    self.deid_qc_onprem_dicom_orthanc_url),
                                    flush=True)
                            (orthanc_err, deid_qc_uploaded_api_study_url, deid_qc_uploaded_explorer_study_url) = \
                                    self.copy_local_directory_to_Orthanc(top_local_path=deidentified_dirname,
                                                                local_path=deidentified_dirname,
                                                                orthanc_url=self.deid_qc_onprem_dicom_orthanc_url,
                                                                orthanc_user=self.qc_onprem_dicom_config['orthanc_user'],
                                                                orthanc_password=self.qc_onprem_dicom_config['orthanc_password'],
                                                                image_counter=0)
                        except Exception as locally_caught_exception:
                            caught_exception = locally_caught_exception
                            # Q: log it anyhow, just in case a problem?
                            # And then continue on without need to delete an already existing dataset:
                            print('{0}.Process(): PHASE04-deid-qc: ERROR: copy_local_directory_to_Orthanc(\'{1}\') seems to have '\
                                    'thrown the following exception: \'{2}\'.'.format(
                                    CLASS_PRINTNAME,
                                    deidentified_dirname,
                                    caught_exception),
                                    flush=True)
                        # NOTE: do not increment errors_encountered += 1
                        # since this will be incremented in a below "if orthanc_err or caught_exception:"

                        nowtime = datetime.datetime.utcnow()
                        print('@ {0} UTC: END of PHASE04 Accession# \'{1}\' UPLOAD to qc-Orthanc'.format(
                                            nowtime,
                                            self.remove_trailing_zeros(curr_accession_num)),
                                            flush=True)

                        # NOTE: overall LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE update of manifest_status='PROCESSED'
                        # as COPIED from end of Phase05
                        # is here such that any changes which are not fully processed
                        # from Phase03 and are resumed mid-way will also be updated:
                        manifest_status = UNDER_REVIEW_STATUS
                        self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                    'last_datetime_processed=now(), '\
                                                    'manifest_status=\'{1}\' '\
                                                    'WHERE accession_num=\'{2}\' '\
                                                    '    AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    manifest_status,
                                                    curr_accession_num))

                        # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                        # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                        # and NOTE: using a ;-delimited list of targets within the following CSV,
                        # since multiple possible target platforms are supported in this module:
                        # NOTE: be sure to leave any empty MANIFEST_HEADER_MANIFEST_VER column at the end of the normally expected headers,
                        # and BEFORE additional output fields (manifest_status, etc):
                        curr_deid_qc_api_study_url = deid_qc_uploaded_api_study_url
                        curr_deid_qc_explorer_study_url = deid_qc_uploaded_explorer_study_url
                        #########
                        # NOTE: rather than any previous curr_deid_qc_status,
                        # instead emit a hint for curr_deid_qc_options:
                        curr_deid_qc_options = UNDER_REVIEW_QC_OPTIONS
                        print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                                MANIFEST_OUTPUT_PREFIX,
                                curr_subject_id,
                                curr_object_info_01,
                                curr_object_info_02,
                                curr_object_info_03,
                                self.remove_trailing_zeros(curr_accession_num),
                                curr_deid_qc_options,
                                '',
                                manifest_status,
                                curr_deid_qc_explorer_study_url,
                                '',
                                ''), flush=True)

                        print('{0}.Process(): PHASE04: preparing to leave phase-{1} as INCOMPLETE for Manual QC step for uuid {2}!'.format(
                                    CLASS_PRINTNAME,
                                    (prev_phase_processed+1),
                                    curr_uuid),
                                    flush=True)

                        # setting the next phase processed to the same as current,
                        # and do NOT update to Phase04 complete
                        # since we don't want to call this complete until the subsequent review...
                        next_prev_phase_processed = prev_phase_processed
                        processRemainingPhases = False

                        # NOTE: then fall on through to remove the interim files (unless otherwise flagged),
                        # since no longer needed once up to ORTHANCDEIDQC
                        # SET manifest_status to the QC_REVIEW kind of output
                        # NOTE: and to update the various Int Cfg Ver cfg_* items as well,

                    # whether or not LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE,
                    # continue on with the interim files, etc:
                    update_interim_files_str = ''
                    if not self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES:
                        # IF not keeping, then also clear their column in LOCUTUS_ONPREM_DICOM_STATUS_TABLE:
                        update_interim_files_str = 'identified_local_path=NULL,'

                    curr_uuid_deid_hostpath = '{0}{1}{2}'.format(self.this_hostname,
                                                self.locutus_settings.HOSTPATH_DELIMITER,
                                                deidentified_dir_zip_filename)

                    ########################################## ########################################## ##########################################
                    # NOTE: *** UPDATING at completion of Phase04 as well as Phase03 & Phase05 ***
                    #       (***corresponding to CHECKING for Phase03 as well as Phase04 Sweep & Phase05 Sweep***)
                    #   ... since this SECOND PASS of the Internal Configuration Versioning Management
                    #       feature has expanded to support PARTIALLY-PROCESSED sessions.
                    # Also check and compare against the internal configuration(s) used during processing:
                    ###########
                    # NOTE: saving these internal config values with EACH update of the session's status
                    # upon EACH phase completion, such that they CAN be checked at Phase Sweeps for Phase04 & 5.
                    # This is to help further prevent any inadvertant phase sweeps of sessions which had only been
                    # partially processed previously (e.g., up to or through Phase04 before encountering an error),
                    # but with different internal configuration(s).
                    ###########
                    int_cfg_status_update_fields = ''
                    int_cfg_status_update_fields_str = ''
                    if len(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES):
                        ######
                        # TODO: for any internal configuration versions with user-configurable overrides,
                        # will need to include their configuration settings as well as the active defaults: LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES
                        ######
                        # NOTE: do NOT reset_others here at Phase04, only at the initial update upon completion of Phase03:
                        int_cfg_status_update_fields = self.generate_update_status_fields_for_internal_configs(
                                                                    LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES,
                                                                    curr_at_phase=(prev_phase_processed+1),
                                                                    reset_others=False)
                        # NOTE: since this is an optional field, preface with a comma only if fields to add:
                        if int_cfg_status_update_fields:
                            int_cfg_status_update_fields_str = ',{0}'.format(','.join(int_cfg_status_update_fields))
                    ###########

                    if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE:
                        if self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS \
                            and (manifest_deid_qc_status.find('PASS:') == 0):
                            # === PASS:*
                            # use the provided info:
                            curr_deid_qc_status = manifest_deid_qc_status
                        else:
                            # all others, really just REPROCESS:*
                            # (since PASS_FROM_DEIDQC:* will pick up in PHASE05, and FAIL:* will bail)
                            # === REPROCESS:*
                            curr_deid_qc_status = UNDER_REVIEW_STATUS
                        # for the rest of Phase04.....
                        print('{0}.Process(): PHASE04: setting curr_deid_qc_status to \'{1}\' for the Manual QC step for uuid {2}!'.format(
                                    CLASS_PRINTNAME,
                                    curr_deid_qc_status,
                                    curr_uuid),
                                    flush=True)

                    # NOTE: if no curr_deid_qc_*_study_url, don't even include them (or deid_qc_status) in the subsequent UPDATE
                    deid_qc_cols = ''
                    if (not curr_deid_qc_api_study_url and not curr_deid_qc_explorer_study_url):
                        # even though deid_qc_status might = UNDER_REVIEW_STATUS, as per the above,
                        # this means that there is not any study on ORTHANCDEID, so warn of this:
                        if curr_deid_qc_status:
                            print('{0}.Process(): PHASE04: DEBUG WARNING: found both deid_qc_[api|explorer]_study_url to be empty; '\
                                'leaving as NULL in the STATUS table, and leaving deid_qc_status=\'{1}\' as well'.format(
                                CLASS_PRINTNAME,
                                curr_deid_qc_status),
                                flush=True)
                        # and set all deid_qc URL columns to NULL for the subsequent UPDATE,
                        # (leaving any existing deid_qc_status values exactly as is, e.g., PASS:*)
                        deid_qc_cols = 'deid_qc_api_study_url=NULL, '\
                                        'deid_qc_explorer_study_url=NULL '
                    else:
                        # all curr_deid_qc_* study fields are defined, go ahead and build up the deid_qc_cols:
                        deid_qc_cols = 'deid_qc_status=\'{0}\', '\
                                        'deid_qc_api_study_url=\'{1}\', '\
                                        'deid_qc_explorer_study_url=\'{2}\' '.format(
                                                curr_deid_qc_status,
                                                curr_deid_qc_api_study_url,
                                                curr_deid_qc_explorer_study_url)

                    # NOTE: no comma before {5} in the below, as it may NOT appear if int_cfg_status_update_fields_str is empty:
                    self.LocutusDBconnSession.execute('UPDATE {0} SET {1} datetime_processed=now(), '\
                                                'phase_processed={2}, '\
                                                'deidentified_local_path=\'{3}\',{4} '\
                                                '{5} '\
                                                'WHERE phase_processed={6} '\
                                                'AND change_seq_id={7} '\
                                                'AND uuid=\'{8}\' '\
                                                '    AND active ;'.format(
                                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                update_interim_files_str,
                                                next_prev_phase_processed,
                                                curr_uuid_deid_hostpath,
                                                deid_qc_cols,
                                                int_cfg_status_update_fields_str,
                                                prev_phase_processed,
                                                curr_uuid_change_seq_id,
                                                curr_uuid))

                    # And remove the identified zip file AND pre-zip DeID file
                    # as neither is needed any more:
                    if not self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES \
                        or (not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE):
                        if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE:
                            # IF NOT manual_QC_disabled AND already UPLOADED to ORTHANCDEID,
                            # no need for the local interim files, either:
                            print('{0}.Process(): PHASE04: NOTE: Even if !LOCUTUS_KEEP_INTERIM_FILES, removing the local interim files '\
                                                'already uploaded to ORTHANCDEIDQC for Manual QC via the ONPREM De-ID QC Orthanc server '\
                                                'since LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE={1}.'.format(
                                                    CLASS_PRINTNAME,
                                                    self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE),
                                                    flush=True)
                        # either way, remove the local interim files:
                        # BUT NOT REALLY for the LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE,
                        # unless really not LOCUTUS_KEEP_INTERIM_FILES,
                        # at least until the ORTHANCDEIDQC instance is actually available to hold act as the interim PACS:
                        if not self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES:
                            shutil.rmtree(curr_uuid_id_path)
                            shutil.rmtree(deidentified_dirname)
                        else:
                            # TODO: remove this portion, and the above if not self...LOCUTUS_KEEP_INTERIM_FILES,
                            # once ORTHANCDEIDQC is in place:
                            print('{0}.Process(): PHASE04: NOTE: BUT until the ONPREM De-ID QC Orthanc server is available for Manual QC, '\
                                                'must retain the interim files to test this workflow, via LOCUTUS_KEEP_INTERIM_FILES={1}.'.format(
                                                    CLASS_PRINTNAME,
                                                    self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES),
                                                    flush=True)
                            print('{0}.Process(): PHASE04: DEBUG: could be authenticated with the new ONPREM De-ID QC Orthanc server; '\
                                                                    'BUT NOT REMOVING Interim files... (awaiting Phase05 cleanup)'.format(CLASS_PRINTNAME), flush=True)
                    else:
                        print('DEBUG: {0}.Process(): PHASE04: '\
                                'LOCUTUS_KEEP_INTERIM_FILES leaving '\
                                'the identified uuid path == {1} AND DeID dir == {2} '\
                                'for comparison!....'.format(
                                CLASS_PRINTNAME,
                                curr_uuid_id_path,
                                deidentified_dirname),
                                flush=True)

        if errors_encountered == 0 and processRemainingPhases:
            # processRemainingPhases if no errors in this phase so far
            if self.locutus_settings.num_targets_configured <= 0:
                print('{0}.Process(): PHASE04: processRemainingPhases, but no targets configured: halting before PHASE05.'.format(CLASS_PRINTNAME), flush=True)
            else:
                print('{0}.Process(): PHASE04: processRemainingPhases, calling Process function for PHASE05 targets...'.format(CLASS_PRINTNAME), flush=True)
                prev_phase_processed += 1

                curr_uuid_errs = 0
                caught_exception = None
                err_desc = ''
                try:
                    (this_errors_encountered, this_errors_message2return, curr_deid_qc_study_url) = self.Process_Phase05_UploadToTargets(
                                                    prev_phase_processed,
                                                    curr_uuid_change_seq_id,
                                                    curr_uuid,
                                                    curr_accession_num,
                                                    curr_uuid_deid_hostpath,
                                                    processRemainingPhases)

                    curr_uuid_errs += this_errors_encountered
                    #errors_encountered += this_errors_encountered
                    if (errors_encountered > 1) and (len(errors_message2return) > 0):
                        errors_message2return += ';'
                    errors_message2return += this_errors_message2return
                except Exception as locally_caught_exception:
                    safe_locally_caught_exception_str = str(locally_caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                    caught_exception = '{0}'.format(safe_locally_caught_exception_str)
                    print('{0}.Process(): PHASE04->05: ERROR: Caught Exception \'{1}\' '\
                                        'calling Process_Phase05_UploadToTargets() for uuid \'{2}\''.format(
                                        CLASS_PRINTNAME,
                                        caught_exception,
                                        curr_uuid),
                                        flush=True)
                    #######################
                    # r3m0: WARNING: SQL_UPDATE_QUOTE_TO_REPLACE replacement happens TWICE here
                    # TODO = Q: is that as expected/needed? If not, then tidy this up!
                    # NOTE: this is in both GCP and OnPrem modules (since OnPrem ^C/^V'd from GCP during its Level-Up)
                    #######################
                    # at least save some bits from the last exception into status:
                    safe_exception_str = str(caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                    err_desc = '{0}'.format(safe_exception_str)
                    # save and re-raise it after updating manifest_status
                    #errors_encountered += 1
                    curr_uuid_errs += 1
                    if (errors_encountered > 1) and (len(errors_message2return) > 0):
                        errors_message2return += ';'
                    errors_message2return += err_desc

                if curr_uuid_errs > 0:
                    errors_encountered += curr_uuid_errs
                    # TODO: further logging of actual error/exception, etc:
                    # NOTE: only prepend current ERROR_PHASE if not already prefixed w/ ERROR_PHASE:
                    manifest_status = err_desc
                    if (len(err_desc) < len(self.locutus_settings.ERR_PHASE_PREFIX)) or (err_desc[:len(self.locutus_settings.ERR_PHASE_PREFIX)] != self.locutus_settings.ERR_PHASE_PREFIX):
                        # NOTE: manifest_status MUST start with ERR_PHASE_PREFIX = 'ERROR_PHASE' for this to work.
                        # only prepend ERROR_PHASE05via04 if a phase isn't already noted:
                        manifest_status = 'ERROR_PHASE05via04=[{0}]'.format(err_desc)
                        print('DEBUG: =====>  r3m0: =====>  ERROR_PHASE04via03 manifest_status={0}'.format(manifest_status), flush=True)

                    self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                'last_datetime_processed=now(), '\
                                                'manifest_status=\'{1}\' '\
                                                'WHERE accession_num=\'{2}\' '\
                                                '    AND active ;'.format(
                                                LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                manifest_status.replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT),
                                                curr_accession_num))

        nowtime = datetime.datetime.utcnow()
        print('@ {0} UTC: END of PHASE04 Accession# \'{1}\''.format(
                                                    nowtime,
                                                    self.remove_trailing_zeros(curr_accession_num)),
                                                    flush=True)
        print('{0}.Process(): PHASE04 ending for this uuid.'.format(CLASS_PRINTNAME), flush=True)
        return (errors_encountered, errors_message2return, curr_deid_qc_study_url)
        # end of Process_Phase04_DeidentifyAndZip()


    def Process_Phase05_UploadToTargets(self,
                                    prev_phase_processed,
                                    curr_uuid_change_seq_id,
                                    curr_uuid,
                                    curr_accession_num,
                                    curr_uuid_deid_hostpath,
                                    processRemainingPhases):
        # NOTE: don't need curr_deid_qc_study_url as a parameter,
        #   since curr_deid_qc_explorer_study_url will be read from *_DICOM_STATUS.
        # NOTE: don't even really need processRemainingPhases as a parameter,
        # since no subsequent phases yet(!).  But just in case this ever does
        # expand to another phase, and for consistency, leave it as a parm.
        errors_encountered = 0
        errors_message2return = ''

        print('{0}.Process(): PHASE05: Process_Phase05_UploadToTargets() called with '\
                                    'phase-{1} processed uuid == {2} '\
                                    'de-ID dir == \'{3}\'!'.format(
                                    CLASS_PRINTNAME,
                                    prev_phase_processed,
                                    curr_uuid,
                                    curr_uuid_deid_hostpath),
                                    flush=True)

        nowtime = datetime.datetime.utcnow()
        print('@ {0} UTC: START of PHASE05 Accession# \'{1}\''.format(
            nowtime,
            self.remove_trailing_zeros(curr_accession_num)),
            flush=True)

        # PROGRESS BAR UPDATES via MANIFEST_STATUS:
        interim_manifest_status = 'PROCESSING_CHANGE_at_PHASE05_UploadingToTargets'
        self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                    'last_datetime_processed=now(), '\
                                    'manifest_status=\'{1}\' '\
                                    'WHERE accession_num=\'{2}\' '\
                                    '    AND active ;'.format(
                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                    interim_manifest_status,
                                    curr_accession_num))

        # Get the patient+study info for this uuid to properly name key for targets:
        patient_study_info_result = self.LocutusDBconnSession.execute('SELECT '\
                                                    'subject_id, object_info_01, '\
                                                    'object_info_02, object_info_03, '\
                                                    'accession_num, '\
                                                    'deid_qc_status, '\
                                                    'deidentified_targets, '\
                                                    'deid_qc_api_study_url, '\
                                                    'deid_qc_explorer_study_url '\
                                                    'FROM {0} WHERE change_seq_id={1} '\
                                                    'AND uuid=\'{2}\' '\
                                                    '    AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                    curr_uuid_change_seq_id,
                                                    curr_uuid))
        row = patient_study_info_result.fetchone()

        internal_DEBUG_simulate_failed_phase05 = False
        # Enable the following internal DEBUG_ to force fail on Phase05
        #internal_DEBUG_simulate_failed_phase05 = True
        if internal_DEBUG_simulate_failed_phase05:
            row = None
            print('{0}.Process(): PHASE05: DEBUG: setting internal_DEBUG_simulate_failed_phase05={1} '\
                                                    'AND row={2} to test phase ERROR!'.format(
                                                    CLASS_PRINTNAME,
                                                    internal_DEBUG_simulate_failed_phase05,
                                                    row),
                                                    flush=True)

        if row is None:
            print('{0}.Process(): PHASE05: ERROR: found NO patient/study info '\
                            'for change_seq_id={1} AND uuid={2} in {3}!'.format(
                            CLASS_PRINTNAME,
                            curr_uuid_max_change_seq_id,
                            curr_uuid,
                            STAGER_STABLESTUDY_TABLE),
                            flush=True)
            # NOTE: return without any further updates or processing of this uuid:
            errors_encountered += 1
            if errors_encountered > 1:
                errors_message2return += ';'
            errors_message2return += 'ERROR_PHASE05=[found NO patient/study info for this uuid change_seq_id={0}]'.format(curr_uuid_max_change_seq_id)

            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE05 Accession# \'{1}\''.format(
                nowtime,
                self.remove_trailing_zeros(curr_accession_num)),
                flush=True)
            print('{0}.Process(): PHASE05 erroring out and ending for this uuid.'.format(CLASS_PRINTNAME), flush=True)
            return (errors_encountered, errors_message2return)

        curr_subject_id = row['subject_id']
        curr_object_info_01 = row['object_info_01']
        curr_object_info_02 = row['object_info_02']
        curr_object_info_03 = row['object_info_03']
        curr_accession_num = row['accession_num']
        curr_deid_qc_status = row['deid_qc_status']
        curr_deid_qc_api_study_url = row['deid_qc_api_study_url']
        curr_deid_qc_api_study_url4media = None
        if curr_deid_qc_api_study_url:
            curr_deid_qc_api_study_url4media = curr_deid_qc_api_study_url+"/media"
            #####
            # NOTE: for FUTURE reference, as ^C/^V'd from Phase03:
            # the API endpoint to this parent study, to download its DICOMDIR (flat structure of: IMAGES/IMG####)
            # WAS: orthanc_get_study_media_url = '{0}/media'.format(orthanc_get_study_url)
            # r3m0: 3/24/2025 = testing download of study to be retrieved as a zip file (w/ subdirs for each Series/MR####.csv),
            # as per: https://orthanc.uclouvain.be/book/users/rest.html#downloading-studies
            #orthanc_get_study_media_url = '{0}/archive'.format(orthanc_get_study_url)
            #####
            # and more, including:
            # NOTE: NOT really sure that this orthanc_get_study_media_url is even being used anywhere, whether here in orthanc_get_study_media_url() or otherwise.
            #####
        curr_deid_qc_explorer_study_url = row['deid_qc_explorer_study_url']
        prev_deidentified_targets = row['deidentified_targets']

        # IF not manual_QC_disabled, then be sure to pull down from ORTHANCDEID, delete from there, and move along....
        if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE \
            and not curr_deid_qc_api_study_url4media:
            print('{0}.Process(): PHASE05: NOTE: WARNING: no curr_deid_qc_api_study_url4media '\
                                'to DOWNLOAD from ORTHANCDEIDQC for Manual QC via the ONPREM De-ID QC Orthanc server which is enabled since '\
                                'LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE={1}.'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE),
                                    flush=True)
        elif not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE \
            and curr_deid_qc_api_study_url4media:
            print('{0}.Process(): PHASE05: NOTE: DOWNLOAD from ORTHANCDEIDQC for Manual QC via the ONPREM De-ID QC Orthanc server is enabled since '\
                                'LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE={1}.'.format(
                                    CLASS_PRINTNAME,
                                    self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE),
                                    flush=True)

            #############################################
            # Manual DeID QC: DOWNLOAD from Orthanc DeID QC

            print('{0}.Process(): PHASE05: DEBUG: NOW authenticating with De-ID QC Orthanc server; '\
                                                    'NEXT DOWNLOADING...'.format(CLASS_PRINTNAME), flush=True)

            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: START of PHASE05 Accession# \'{1}\' DOWNLOAD from qc-Orthanc'.format(
                                        nowtime,
                                        self.remove_trailing_zeros(curr_accession_num)),
                                        flush=True)

            # NOTE: re-authenticate via the RestToolbox whenever accessing EITHER Orthanc:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE05: re-authenticating with qc-Orthanc server...'.format(CLASS_PRINTNAME), flush=True)
            RestToolbox.SetCredentials(self.qc_onprem_dicom_config['orthanc_user'],
                                        self.qc_onprem_dicom_config['orthanc_password'])

            # and actually get the study URL from qc-Orthanc:
            ##################################################
            # TODO: wrap this in a try/catch to at least better present exceptions
            # such as "name 'staged_url' is not defined" in the following:
            ##################################################
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE05: DOWNLOADING from qc-Orthanc server... url = [{1}]'.format(
                                                CLASS_PRINTNAME, curr_deid_qc_api_study_url4media), flush=True)
            dicomdir_data = RestToolbox.DoGet(curr_deid_qc_api_study_url4media)

            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE05 Accession# \'{1}\' DOWNLOAD from qc-Orthanc'.format(
                                        nowtime,
                                        self.remove_trailing_zeros(curr_accession_num)),
                                        flush=True)
            print('{0}.Process(): PHASE05: downloaded dicom_dir with len={1}'.format(
                                        CLASS_PRINTNAME,
                                        len(dicomdir_data)),
                                        flush=True)

            # NOTE:  dicomdir_data does not seem to be referenced any further beyond the above,
            # and as such, seems like the below COULD be using what might just be a left over interm file from Phase4.
            # Therefore, ensure that we ARE properly using the downloaded dicomdir_data from ORTHANCDEIDQC.
            if not curr_uuid_deid_hostpath:
                print('{0}.Process(): PHASE05: DEBUG ERROR: curr_uuid_deid_hostpath={1}, BUT, '\
                            'UNTIL the new ONPREM De-ID QC Orthanc server, '\
                            'testing must currently be done with locutus_debug_keep_interim_files=True '\
                            '(locutus_debug_keep_interim_files={2}, and no interim files remain)'.format(
                            CLASS_PRINTNAME,
                            curr_uuid_deid_hostpath,
                            self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES), flush=True)
                # IF not throwing the following exception, then and an error will therefor occur in 3... 2.... 1...
                raise ValueError('Locutus {0}.Process(): PHASE05: DEBUG ERROR: curr_uuid_deid_hostpath={1}, BUT, '\
                            'UNTIL the new ONPREM De-ID QC Orthanc server, '\
                            'testing must currently be done with locutus_debug_keep_interim_files=True '\
                            '(locutus_debug_keep_interim_files={2}, and no interim files remain)'.format(
                            CLASS_PRINTNAME,
                            curr_uuid_deid_hostpath,
                            self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES))

        # strip off the hostname & path, and ensure that it exists/matches:
        (curr_uuid_deid_host, curr_uuid_deid_zippath) = curr_uuid_deid_hostpath.split(self.locutus_settings.HOSTPATH_DELIMITER)

        if not (os.path.exists(curr_uuid_deid_zippath) and os.path.isfile(curr_uuid_deid_zippath)):
            print('{0}.Process(): PHASE05: ERROR: uuid\'s de-identified zip {1} '\
                            'does NOT exist here.'.format(
                            CLASS_PRINTNAME,
                            curr_uuid_deid_zippath),
                            flush=True)
            # TODO: Q: throw an exception here? or just log for now?
            errors_encountered += 1
            if errors_encountered > 1:
                errors_message2return += ';'
            errors_message2return += 'ERROR_PHASE05=[uuid de-identified zip NOT found here]'

            if curr_uuid_deid_host != self.this_hostname:
                print('{0}.Process(): WARNING: uuid\'s ID directory host ({1}) '\
                            'does not match current host ({2})'.format(
                            CLASS_PRINTNAME,
                            curr_uuid_deid_host,
                            self.this_hostname),
                            flush=True)
        else:
            # Phase05a = Transfer on up to configured targets:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE05: confirmed de-identified dir for this uuid with De-ID zip=\'{1}\' ; '\
                                    'transferring to all configured targets....'.format(
                                    CLASS_PRINTNAME,
                                    curr_uuid_deid_zippath),
                                    flush=True)

            # NOTE: delay updates of the Phase5 targets to LOCUTUS_ONPREM_DICOM_STATUS_TABLE until all possible targets completed:
            deidentified_targets = []
            deidentified_targets_new = []

            # Split the previously saved comma-delimited deidentified_targets for our next update:
            if prev_deidentified_targets:
                deidentified_targets = prev_deidentified_targets.split(',')
                print('{0}.Process(): PHASE05: this uuid has already saved de-identified targets at: \'{1}\''.format(
                                    CLASS_PRINTNAME,
                                    prev_deidentified_targets),
                                    flush=True)
                # and even if the same targets had already been used previously,
                # still follow through with any new such uploads below (in case of updated data, processing, etc.),
                # BUT - only append them onto the list of deidentified_targets IF not already previously listed.

            if self.locutus_settings.LOCUTUS_TARGET_USE_ISILON:
                # NOTE: optimized for the dicom_anon output to go straight there already :-)
                print('{0}.Process(): PHASE05-isilon: NO NEED for additional transfer \'{1}\' to target local isilon of \'{2}\' since dicom_anon output zipped up directly to it....'.format(
                                    CLASS_PRINTNAME,
                                    curr_uuid_deid_zippath,
                                    self.locutus_settings.LOCUTUS_TARGET_ISILON_PATH),
                                    flush=True)

                # NOTE: to support listing multiple targets, build up an array for a single UPDATE after all of them:
                this_targetname = curr_uuid_deid_zippath

                # add this_targetname if not already listed in any previous deidentified_targets:
                queuing_str = "NOT queuing an additional (already listed)"
                if this_targetname not in deidentified_targets:
                    deidentified_targets.append(this_targetname)
                    queuing_str = "queuing up a"
                deidentified_targets_new.append(this_targetname)

                print('{0}.Process(): PHASE05-isilon: {1} phase-{2} COMPLETE for '\
                                'uuid {3} to target=\'{4}\'!'.format(
                                CLASS_PRINTNAME,
                                queuing_str,
                                (prev_phase_processed+1),
                                curr_uuid,
                                this_targetname),
                                flush=True)

            if self.locutus_settings.LOCUTUS_TARGET_USE_S3:
                print('{0}.Process(): PHASE05-s3: Preparing to transfer to target AWS s3 bucket....'.format(
                                    CLASS_PRINTNAME),
                                    flush=True)

                # s3 keyname: <TOP_LEVEL>/<subject_id>/<object_info_01>/<object_info_02>/<object_info_03>/uuid_<uuid#>.zip
                # TODO: eventually expand to handle a dynamic number of object_infos, but for now:
                s3_bucket_keyname = '{0}/{1}/{2}/{3}/{4}/uuid_{5}.zip'.format(
                                        self.locutus_settings.LOCUTUS_ONPREM_DICOM_BUCKET_PATH_TOP_LEVEL,
                                        curr_subject_id, curr_object_info_01,
                                        curr_object_info_02, curr_object_info_03, curr_uuid)

                # Now, actually do the s3 transfer, w/ Boto3's .put()
                s3_err = 0
                print('{0}.Process(): PHASE05-s3: transferring deidentified uuid {1} '\
                                        'to AWS s3 with bucket=\'{2}\' & key=\'{3}\''.format(
                                        CLASS_PRINTNAME,
                                        curr_uuid,
                                        self.locutus_settings.LOCUTUS_TARGET_S3_BUCKET,
                                        s3_bucket_keyname),
                                        flush=True)

                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: START of PHASE05-s3 Accession# \'{1}\' UPLOAD to AWS'.format(
                    nowtime,
                    self.remove_trailing_zeros(curr_accession_num)),
                    flush=True)

                caught_exception = None
                try:
                    s3_retval = self.locutus_settings.locutus_target_s3resource.Object( \
                                        self.locutus_settings.LOCUTUS_TARGET_S3_BUCKET,
                                        s3_bucket_keyname).put(
                                            Body=open(curr_uuid_deid_zippath, 'rb'),
                                        ServerSideEncryption='AES256'
                                        )
                except Exception as locally_caught_exception:
                    safe_locally_caught_exception_str = str(locally_caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                    caught_exception = '{0}'.format(safe_locally_caught_exception_str)
                    print('{0}.Process(): PHASE05-s3: ERROR: Caught Exception \'{1}\' '\
                                    'while performing Accession# \'{1}\' UPLOAD to AWS.'.format(
                                    CLASS_PRINTNAME,
                                    caught_exception,
                                    self.remove_trailing_zeros(curr_accession_num)),
                                    flush=True)
                    # at least save some bits from the last exception into status:
                    safe_exception_str = str(caught_exception).replace("'","")
                    err_desc = '{0}'.format(safe_exception_str)
                    #######################
                    # r3m0: WARNING: SQL_UPDATE_QUOTE_TO_REPLACE replacement happens TWICE here
                    # TODO = Q: is that as expected/needed? If not, then tidy this up!
                    # NOTE: this is in both GCP and OnPrem modules (since OnPrem ^C/^V'd from GCP during its Level-Up)
                    #######################
                    # at least save some bits from the last exception into status:
                    safe_exception_str = str(caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                    err_desc = '{0}'.format(safe_exception_str)
                    # save and re-raise it after updating manifest_status
                    #######################
                    # NOTE: errors_encountered will be updated in the later check for s3_err, so
                    # save both s3_err & s3_retval as the error itself, since nothing returned there:
                    s3_err = err_desc
                    s3_retval = err_desc

                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: END of PHASE05-s3 Accession# \'{1}\' UPLOAD to AWS'.format(
                    nowtime,
                    self.remove_trailing_zeros(curr_accession_num)),
                    flush=True)

                # TODO: parse and strip out any errors from the above s3_retval, to determine s3_err!!!
                # in the meantime (even w/o VERBOSE mode), go ahead and print it out:
                print('{0}.Process(): DEBUG INFO: the above call to AWS s3 just '\
                                            'returned: {1}'.format(
                                            CLASS_PRINTNAME,
                                            s3_retval),
                                            flush=True)

                # NOTE: a successful upload returns an s3_retval much like the following:
                # {'ETag': '"76cdb2bad9582d23c1f6f4d868218d6c"',
                #   'ServerSideEncryption': 'AES256',
                #   'ResponseMetadata': {'HTTPStatusCode': 200,
                #                       'HostId': 'HPqygZmCJexpy81rdB9/SQ0HZpiuHuR+xmI5OJiQSLX5cDVevlFdXOtsEZsZQomoY0s5EJKBZdk=',
                #                       'RetryAttempts': 0,
                #                       'HTTPHeaders': {
                #                           'etag': '"76cdb2bad9582d23c1f6f4d868218d6c"',
                #                           'content-length': '0',
                #                           'date': 'Fri, 30 Mar 2018 20:16:23 GMT',
                #                           'x-amz-request-id': '70A5BBE1DC9C6C1A',
                #                           'x-amz-server-side-encryption': 'AES256',
                #                           'x-amz-id-2': 'HPqygZmCJexpy81rdB9/SQ0HZpiuHuR+xmI5OJiQSLX5cDVevlFdXOtsEZsZQomoY0s5EJKBZdk=',
                #                           'server': 'AmazonS3'},
                #                       'RequestId': '70A5BBE1DC9C6C1A'}}
                ###########################
                # ==> NEXT TODO: try to capture an error, to generate s3_err:
                # TODO: determine how/where to get an s3_error set from the below s3.Object().put:
                # TODO: TEST w/ invalid bucket name to investigate the s3_retval ;-)
                # TODO: AND TEST w/ the following pre-zip code, since that generated errors,
                # to test that the Jenkins deployment likewise propagates the error:
                # Pre-Zip .put() ERROR HAD USED:
                #       Body=open(curr_uuid_deid_zippath, 'rb'),
                # on the entire deidentification directory.

                if s3_err:
                    print('{0}.Process(): PHASE05-s3: ERROR: the above call to AWS s3 '\
                                    'for \'{1}\' just returned non-zero error code of: {2}'.format(
                                    CLASS_PRINTNAME,
                                    curr_uuid_deid_zippath,
                                    s3_err),
                                    flush=True)
                    # TODO: Q: throw an exception here? or just log for now?
                    errors_encountered += 1
                    if errors_encountered > 1:
                        errors_message2return += ';'
                    errors_message2return += 'ERROR_PHASE05=[call to AWS s3 upload for this uuid returned non-zero error code of {0}]'.format(s3_err)
                else:
                    # NOTE: to support listing multiple targets, build up an array for a single UPDATE after all of them:
                    this_targetname = os.path.join(
                                            AWS_S3_TARGET_PREFIX,
                                            self.locutus_settings.LOCUTUS_TARGET_S3_BUCKET,
                                            s3_bucket_keyname)

                    # add this_targetname if not already listed in any previous deidentified_targets:
                    queuing_str = "NOT queuing an additional (already listed)"
                    if this_targetname not in deidentified_targets:
                        deidentified_targets.append(this_targetname)
                        queuing_str = "queuing up a"
                    deidentified_targets_new.append(this_targetname)

                    print('{0}.Process(): PHASE05-s3: {1} phase-{2} COMPLETE for '\
                                    'uuid {3} to target=\'{4}\'!'.format(
                                    CLASS_PRINTNAME,
                                    queuing_str,
                                    (prev_phase_processed+1),
                                    curr_uuid,
                                    this_targetname),
                                    flush=True)


            if self.locutus_settings.LOCUTUS_TARGET_USE_GS:
                print('{0}.Process(): PHASE05-gs: Preparing to transfer to target GCP Google Storage bucket....'.format(
                                    CLASS_PRINTNAME),
                                    flush=True)

                # GS keyname: <TOP_LEVEL>/<subject_id>/<object_info_01>/<object_info_02>/<object_info_03>/uuid_<uuid#>.zip
                # TODO: eventually expand to handle a dynamic number of object_infos, but for now:
                gs_bucket_keyname = '{0}/{1}/{2}/{3}/{4}/uuid_{5}.zip'.format(
                                        self.locutus_settings.LOCUTUS_ONPREM_DICOM_BUCKET_PATH_TOP_LEVEL,
                                        curr_subject_id, curr_object_info_01,
                                        curr_object_info_02, curr_object_info_03, curr_uuid)

                # Now, actually do the GS transfer, w/ GS's upload_from_filename()
                gs_err = 0
                gs_retval = 0

                print('{0}.Process(): PHASE05-gs: transferring deidentified uuid {1} '\
                                        'to GCP GS with bucket=\'{2}\' & key=\'{3}\''.format(
                                        CLASS_PRINTNAME,
                                        curr_uuid,
                                        self.locutus_settings.LOCUTUS_TARGET_GS_BUCKET,
                                        gs_bucket_keyname),
                                        flush=True)

                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: START of PHASE05-gs Accession# \'{1}\' UPLOAD to GS'.format(
                    nowtime,
                    self.remove_trailing_zeros(curr_accession_num)),
                    flush=True)

                # initialize actual Google Storage bucket:
                gs_bucket = self.locutus_settings.locutus_target_GSclient.get_bucket(
                                        self.locutus_settings.LOCUTUS_TARGET_GS_BUCKET)
                gs_blob2 = gs_bucket.blob(gs_bucket_keyname)
                gs_retval = gs_blob2.upload_from_filename(filename=curr_uuid_deid_zippath)

                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: END of PHASE05-gs Accession# \'{1}\' UPLOAD to GS'.format(
                    nowtime,
                    self.remove_trailing_zeros(curr_accession_num)),
                    flush=True)

                print('{0}.Process(): DEBUG INFO: the above call to GCP GS just '\
                                            'returned: {1}'.format(
                                            CLASS_PRINTNAME,
                                            gs_retval),
                                            flush=True)

                # TODO: parse and strip out any errors from the above gs_retval, to determine gs_err!!!
                if gs_retval:
                    # NOTE: normally gs_retval==None
                    # (likely no return value at all, and needing an internal try/except block),
                    # noting that this entire Phase05 method is usually wrapped in a try/except block.
                    gs_err = True

                if gs_err:
                    print('{0}.Process(): PHASE05-gs: ERROR: the above call to GCP GS '\
                                    'for \'{1}\' just returned non-zero error code of: {2}'.format(
                                    CLASS_PRINTNAME,
                                    curr_uuid_deid_zippath,
                                    gs_err),
                                    flush=True)
                    # TODO: Q: throw an exception here? or just log for now?
                    errors_encountered += 1
                    if errors_encountered > 1:
                        errors_message2return += ';'
                    errors_message2return += 'ERROR_PHASE05=[call to GCP GS to upload this uuid returned non-zero error code of {0}]'.format(gs_err)
                else:
                    # NOTE: to support listing multiple targets, build up an array for a single UPDATE after all of them:
                    this_targetname = os.path.join(
                                            GOOGLE_STORAGE_TARGET_PREFIX,
                                            self.locutus_settings.LOCUTUS_TARGET_GS_BUCKET,
                                            gs_bucket_keyname)

                    # add this_targetname if not already listed in any previous deidentified_targets:
                    queuing_str = "NOT queuing an additional (already listed)"
                    if this_targetname not in deidentified_targets:
                        deidentified_targets.append(this_targetname)
                        queuing_str = "queuing up a"
                    deidentified_targets_new.append(this_targetname)

                    print('{0}.Process(): PHASE05-gs: {1} phase-{2} COMPLETE for '\
                                    'uuid {3} to target=\'{4}\'!'.format(
                                    CLASS_PRINTNAME,
                                    queuing_str,
                                    (prev_phase_processed+1),
                                    curr_uuid,
                                    this_targetname),
                                    flush=True)

            # END of the above target destinations.
            # Perform any DB status table updates, and optionally remove interim files,
            # IF any destination targets processed:

            if len(deidentified_targets_new) > 0:

                #  * update the LOCUTUS_ONPREM_DICOM_STATUS_TABLE with the deidentified_targets,
                #       with an updated processing time and incremented phase_completed,
                #       just as the Phases 3e & 4b  had done for the (de-)identified locations,
                #       and can now also set deidentified_local_path=NULL

                print('{0}.Process(): PHASE05: setting phase-{1} COMPLETE for '\
                                'uuid {2} to targets=\'{3}\'!'.format(
                                CLASS_PRINTNAME,
                                (prev_phase_processed+1),
                                curr_uuid,
                                ','.join(deidentified_targets),
                                flush=True))

                update_interim_files_str = ''
                if not self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES:
                    # IF not keeping, then also clear their column in LOCUTUS_ONPREM_DICOM_STATUS_TABLE:
                    update_interim_files_str = 'deidentified_local_path=NULL,'

                ########################################## ########################################## ##########################################
                # NOTE: *** UPDATING at completion of Phase05 as well as Phase03 & Phase04 ***
                #       (***corresponding to CHECKING for Phase03 as well as Phase04 Sweep & Phase05 Sweep***)
                #   ... since this SECOND PASS of the Internal Configuration Versioning Management
                #       feature has expanded to support PARTIALLY-PROCESSED sessions.
                # Also check and compare against the internal configuration(s) used during processing:
                ###########
                # NOTE: saving these internal config values with EACH update of the session's status
                # upon EACH phase completion, such that they CAN be checked at Phase Sweeps for Phase04 & 5.
                # This is to help further prevent any inadvertant phase sweeps of sessions which had only been
                # partially processed previously (e.g., up to or through Phase04 before encountering an error),
                # but with different internal configuration(s).
                ###########
                int_cfg_status_update_fields = ''
                int_cfg_status_update_fields_str = ''

                if len(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES):
                    ######
                    # TODO: for any internal configuration versions with user-configurable overrides,
                    # will need to include their configuration settings as well as the active defaults: LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES
                    ######
                    # NOTE: do NOT reset_others here at Phase05, only at the initial update upon completion of Phase03:
                    int_cfg_status_update_fields = self.generate_update_status_fields_for_internal_configs(
                                                                LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES,
                                                                curr_at_phase=(prev_phase_processed+1),
                                                                reset_others=False)
                    # NOTE: since this is an optional field, preface with a comma only if fields to add:
                    if int_cfg_status_update_fields:
                        int_cfg_status_update_fields_str = ',{0}'.format(','.join(int_cfg_status_update_fields))
                ###########

                # NOTE: no comma before {4} in the below, as it may NOT appear if int_cfg_status_update_fields_str is empty:
                # TODO: ensure that we can just go ahead and set deid_qc_*_study_url=NULL below, that all has been processed
                # either with a PASS:*, PASS_FROM_DEIDQC:*, or a FAIL:*
                self.LocutusDBconnSession.execute('UPDATE {0} SET {1} datetime_processed=now(), '\
                                            'phase_processed={2}, '\
                                            'deid_qc_api_study_url=NULL, '\
                                            'deid_qc_explorer_study_url=NULL, '\
                                            'deidentified_targets=\'{3}\' '\
                                            '{4} '\
                                            'WHERE phase_processed={5} '\
                                            'AND change_seq_id={6} '\
                                            'AND uuid=\'{7}\' '\
                                            '    AND active ;'.format(
                                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                            update_interim_files_str,
                                            (prev_phase_processed+1),
                                            ','.join(deidentified_targets),
                                            int_cfg_status_update_fields_str,
                                            prev_phase_processed,
                                            curr_uuid_change_seq_id,
                                            curr_uuid))

                # NOTE: overall LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE update of manifest_status='PROCESSED'
                # is here such that any changes which are not fully processed
                # from Phase03 and are resumed mid-way will also be updated:
                manifest_status = 'PROCESSED'
                self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                            'last_datetime_processed=now(), '\
                                            'manifest_status=\'{1}\' '\
                                            'WHERE accession_num=\'{2}\' '\
                                            '    AND active ;'.format(
                                            LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                            manifest_status,
                                            curr_accession_num))

                # TODO: create a function for the following fw_session_import_arg, since now used in multiple places:
                # NOTE: generate the helper column for downstream import into Flywheel via --session <age>d_<location>
                # BEWARE: this is now also used in cmd_dicom_summarize_status.py
                curr_fw_session_import_arg = '{0}d_{1}'.format(curr_object_info_02, curr_object_info_03)

                # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                # and NOTE: using a ;-delimited list of targets within the following CSV,
                # since multiple possible target platforms are supported in this module:
                # NOTE: be sure to leave any empty MANIFEST_HEADER_MANIFEST_VER column at the end of the normally expected headers,
                # and BEFORE additional output fields (manifest_status, etc):

                curr_deid_qc_api_study_url_msg = curr_deid_qc_explorer_study_url
                if not self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES:
                    # rather than leaving the URL as is, now (removed)'ing, as:
                    #WAS: curr_deid_qc_api_study_url_msg = '(removed from interim De-ID QC)'
                    # or as: 'n/a' or '' :-)
                    # TODO: at least untkl QC is re-explored,
                    # add check for LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE:
                    curr_deid_qc_api_study_url_msg = ''

                # WAS: w/ self.remove_trailing_zeros(curr_accession_num),
                # BUT now with LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS, may want to revert to the original "curr_accession_num" string:
                print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                            MANIFEST_OUTPUT_PREFIX,
                            curr_subject_id,
                            curr_object_info_01,
                            curr_object_info_02,
                            curr_object_info_03,
                            curr_accession_num,
                            curr_deid_qc_status,
                            '',
                            manifest_status,
                            curr_deid_qc_api_study_url_msg,
                            ';'.join(deidentified_targets_new),
                            curr_fw_session_import_arg), flush=True)


                # And remove the de-identified zip file, as it is no longer needed,
                # so long as it wasn't part of the ISILON target:
                if not self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES \
                    and not self.locutus_settings.LOCUTUS_TARGET_USE_ISILON:
                    os.remove(curr_uuid_deid_zippath)

                    # IF not manual_QC_disabled, Delete from ORTHANCDEID, as its own set of interim files:
                    # but first, ensure that curr_deid_qc_api_study_url is defined:
                    if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE \
                        and not curr_deid_qc_api_study_url:
                        # no curr_deid_qc_api_study_url, give a WARNING but carry on:
                        print('{0}.Process(): PHASE05: WARNING: empty DEID QC study url from '\
                            'the STATUS table showing as (\'{1}\'); '\
                            'no need to remove this interim DEID QC study for Accession# \'{2}\' '\
                            'with curr_deid_qc_status=\'{3}\'.'.format(
                            CLASS_PRINTNAME,
                            curr_deid_qc_api_study_url,
                            self.remove_trailing_zeros(curr_accession_num),
                            curr_deid_qc_status),
                            flush=True)
                    elif not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE \
                        and curr_deid_qc_api_study_url:
                        print('{0}.Process(): PHASE05: NOTE: !LOCUTUS_KEEP_INTERIM_FILES so REMOVE FROM ORTHANCDEIDQC '\
                                            'the Manual QC via the ONPREM De-ID QC Orthanc server is enabled since '\
                                            'LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE={1}.'.format(
                                                CLASS_PRINTNAME,
                                                self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE),
                                                flush=True)
                        #############################################
                        # Manual DeID QC: DELETE from Orthanc DeID QC

                        print('{0}.Process(): PHASE05: DEBUG: NOW authenticating with De-ID QC Orthanc server; '\
                                                                'NEXT DELETING...'.format(CLASS_PRINTNAME), flush=True)

                        # TODO: package the following up into a DeleteFromDeIDQC() method:
                        nowtime = datetime.datetime.utcnow()
                        print('@ {0} UTC: START of PHASE05 Accession# \'{1}\' DELETE from qc-Orthanc'.format(
                                        nowtime,
                                        self.remove_trailing_zeros(curr_accession_num)),
                                        flush=True)


                        # NOTE: re-authenticate via the RestToolbox whenever accessing EITHER Orthanc:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): PHASE05: re-authenticating with qc-Orthanc server...'.format(CLASS_PRINTNAME), flush=True)
                        RestToolbox.SetCredentials(self.qc_onprem_dicom_config['orthanc_user'],
                                                    self.qc_onprem_dicom_config['orthanc_password'])

                        # NOTE: after having also cleared out the 2x api/explorer study uid fields
                        # in the previous UPDATE to ONPREM_DICOM_STATUS, can now actually remove it from ORTHANCDEIDQC,
                        # now actually delete the study URL from qc-Orthanc:
                        ##################################################
                        # TODO: wrap this in a try/catch to at least better present exceptions
                        # such as "name 'staged_url' is not defined" in the following:
                        ##################################################
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): PHASE05: DELETING from qc-Orthanc server... url = [{0}]'.format(
                                                        CLASS_PRINTNAME, curr_deid_qc_api_study_url), flush=True)
                        dicomdir_data = RestToolbox.DoDelete(curr_deid_qc_api_study_url)

                        nowtime = datetime.datetime.utcnow()
                        print('@ {0} UTC: END of PHASE05 Accession# \'{1}\' DELETE from qc-Orthanc'.format(
                                        nowtime,
                                        self.remove_trailing_zeros(curr_accession_num)),
                                        flush=True)

                else:
                    print('DEBUG: {0}.Process(): PHASE05: '\
                            'LOCUTUS_TARGET_USE_ISILON leaving the de-identified uuid zip == {1} '\
                            'for comparison!....'.format(
                            CLASS_PRINTNAME,
                            curr_uuid_deid_zippath),
                            flush=True)

        # NOTE: nothing more for processRemainingPhases to do ;-)
        nowtime = datetime.datetime.utcnow()
        print('@ {0} UTC: END of PHASE05 Accession# \'{1}\''.format(
            nowtime,
            self.remove_trailing_zeros(curr_accession_num)),
            flush=True)
        print('{0}.Process(): PHASE05 ending for this uuid.'.format(CLASS_PRINTNAME), flush=True)
        return (errors_encountered, errors_message2return, curr_deid_qc_api_study_url)
        # end of Process_Phase05_UploadToTargets()


    def Process(self):
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process() says Howdy'.format(CLASS_PRINTNAME), flush=True)

        ###########
        # System Status:
        # first, for CFG_OUT, check the Locutus overall status:
        ###########
        check_Docker_node=False
        alt_node_name=None
        check_module=False
        module_name=SYS_STAT_MODULENAME
        (curr_sys_stat_overall, sys_msg_overall, sys_overall_name) = self.locutus_settings.get_Locutus_system_status(self.locutus_settings, check_Docker_node, alt_node_name, check_module, module_name, self.LocutusDBconnSession)
        print('{0},{1},overall,{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "onprem_GET_LOCUTUS_SYS_STATUS", sys_overall_name, curr_sys_stat_overall, sys_msg_overall), flush=True)

        # next, also check the node-level status that we'll ultimately use
        # (still checking even if the above overall active System Status is False, for completeness):
        check_Docker_node=True
        (curr_sys_stat_node, sys_msg_node, sys_node_name) = self.locutus_settings.get_Locutus_system_status(self.locutus_settings, check_Docker_node, alt_node_name, check_module, module_name, self.LocutusDBconnSession)
        print('{0},{1},node={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "onprem_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_node, sys_msg_node), flush=True)

        # and then, also check the module-specific status that we'll also ultimately use
        # (still checking even if either of the above overall & node-level active System Status are False, for completeness):
        check_Docker_node=False
        check_module=True
        #print('r3m0 DEBUG: about to call get_Locutus_system_status with module_name={0}'.format(module_name), flush=True)
        (curr_sys_stat_module, sys_msg_module, sys_module_name) = self.locutus_settings.get_Locutus_system_status(self.locutus_settings, check_Docker_node, alt_node_name, check_module, module_name, self.LocutusDBconnSession)
        print('{0},{1},module={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "onprem_GET_LOCUTUS_SYS_STATUS", sys_module_name, curr_sys_stat_module, sys_msg_module), flush=True)

        # NOTE: bail here, as early as possible in Process(), if not active System Status
        if (not curr_sys_stat_overall) or (not curr_sys_stat_node) or (not curr_sys_stat_module):
            print('ERROR: OnPrem Process() found Locutus NOT active System Status for this module (status={0}), node (status={1}), or overall (status={2}); returning EARLY!'.format(
                    curr_sys_stat_module, curr_sys_stat_node, curr_sys_stat_overall), flush=True)
            raise ValueError('OnPrem module found Locutus NOT active System Status: {0}'.format(sys_msg_node))
            #print('r3m0 DEBUG: NOT YET raising that error, testing later in GCP....')
        #
        ###########

        this_fatal_errors = False
        total_errors_encountered = 0
        total_accessions_with_text_removed = 0
        total_accessions_processed_successfully = 0

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('DEBUG: {0}.Process(): Using src_onprem_dicom_orthanc_url={1}'.format(
                            CLASS_PRINTNAME,
                            self.src_onprem_dicom_orthanc_url),
                            flush=True)
            if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE:
                print('DEBUG: {0}.Process(): Using optionally enabled deid_qc_onprem_dicom_orthanc_url={1}'.format(
                            CLASS_PRINTNAME,
                            self.deid_qc_onprem_dicom_orthanc_url),
                            flush=True)
            else:
                # i.e., IF self.locutus_settings.LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE:
                print('DEBUG: {0}.Process(): NOT using optional deid_qc_onprem_dicom_orthanc_url.'.format(
                            CLASS_PRINTNAME),
                            flush=True)
            print('DEBUG: {0}.Process(): Using Locutus onprem_dicom_target_db_name = {1}!'.format(
                            CLASS_PRINTNAME,
                            self.onprem_dicom_target_db_name),
                            flush=True)

        # Processing Phase00:
        ###########################################################
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process(): Setting up Locutus-local tables for ONPREM DICOM '\
                        'Status in {1} ...'.format(
                        CLASS_PRINTNAME,
                        self.locutus_settings.locutus_target_db_name),
                        flush=True)

        # WAS: in debugging the new Locutus Workspaces, AGAIN emit the generated workspace table names to the CFG_OUT:
        #print('{0},onprem-dicom-workspace-DEBUG_REPEAT,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_STATUS_TABLE", LOCUTUS_ONPREM_DICOM_STATUS_TABLE), flush=True)
        #print('{0},onprem-dicom-workspace-DEBUG_REPEAT,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE", LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE), flush=True)
        #print('{0},onprem-dicom-workspace-DEBUG_REPEAT,{1},\"{2}\"'.format(src_modules.settings.CFG_OUT_PREFIX, "LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE", LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE), flush=True)
        # NOTE: oops, initial approach in Setup is not updating the actual LOCUTUS_ONPREM_DICOM_*_TABLE #defines
        # FIXED w/ globals.

        if self.locutus_settings.LOCUTUS_DB_DROP_TABLES \
            and not self.locutus_settings.LOCUTUS_TEST:
            # NOTE: only drop the LOCUTUS tables which are directly applicable to ONPREM DICOM:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): DROPPING existing Locutus-local ONPREM DICOM '\
                        'tables since config LOCUTUS_DB_DROP_TABLES == {1}'.format(
                        CLASS_PRINTNAME,
                        self.locutus_settings.LOCUTUS_DB_DROP_TABLES),
                        flush=True)
            self.LocutusDBconnSession.execute('DROP TABLE if exists {0};'.format(LOCUTUS_ONPREM_DICOM_STATUS_TABLE))
            self.LocutusDBconnSession.execute('DROP TABLE if exists {0};'.format(LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE))
        else:
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): NOT dropping existing tables since config '\
                        'LOCUTUS_DB_DROP_TABLES == {1} and '\
                        'LOCUTUS_TEST == {2}'.format(
                        CLASS_PRINTNAME,
                        self.locutus_settings.LOCUTUS_DB_DROP_TABLES,
                        self.locutus_settings.LOCUTUS_TEST),
                        flush=True)

        # Create LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE:
        print('{0}.Process(): About to CREATE TABLE, if not exists, LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE: {1}'.format(
            CLASS_PRINTNAME,
            LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE),
            flush=True)
        if not self.locutus_settings.LOCUTUS_TEST:
            self.LocutusDBconnSession.execute('CREATE TABLE if not exists {0} ('\
                                    'config_type text, '\
                                    'config_version text, '\
                                    'config_desc text, '\
                                    'date_activated date, '\
                                    'active boolean, '\
                                    'at_phase int, '\
                                    'status_field text '\
                                    ');'.format(LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE))
        # and immediately populate/update LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE
        # with the currently active internal configuration versions:
        self.set_active_internal_configs(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES)

        # CFG_OUT_PREFIX for LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES:
        # iterate through the list of dictionaries:
        # NOTE: as moved from Setup to here into Process(),
        #   immediately after Phase00 CREATE TABLE LOCUTUS_ONPREM_DICOM_INT_CFGS_TABLE
        #   and its subsequent call to: self.set_active_internal_configs(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES
        # to ensure that any latest ACTIVE-DB configs are taken into account.
        # TODO: consider moving all modules' CREATE TABLE code snippets up into the respective Setup().
        for idx_cfg, cfg_dict in enumerate(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES):
            # and iterate through each key/value pair of each dictionary:
            for idx_key, cfg_key in enumerate(cfg_dict.keys()):
                print('{0},onprem-dicom-hardcoded,{1}-{2}-{3},\"{4}\"'.format(
                            src_modules.settings.CFG_OUT_PREFIX,
                            "INT_CFGS_ACTIVES",
                            (idx_cfg+1),
                            cfg_key,
                            LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES[idx_cfg][cfg_key]), flush=True)


        ############
        # NOTE: dynamic addition into the BELOW LOCUTUS_ONPREM_DICOM_STATUS_TABLE
        #   of each `status_field` from the LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES list of dictionaries.
        int_cfg_status_fields_str = ''
        if len(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES):
            int_cfg_status_fields = self.generate_create_status_fields_for_internal_configs(LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES)
            # NOTE: since this is an optional field, preface with a comma only since fields to add:
            int_cfg_status_fields_str = ',{0}'.format(','.join(int_cfg_status_fields))

        # TODO: consider where to also CREATE TABLE for initial self.locutus_settings.LOCUTUS_SYS_STATUS_TABLE
        # though better would be in main_locutus(), though it doesn't yet have a DBconn

        ####################
        # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the STATUS table:
        # NOTE: be sure to use LOWER to align with the postgres lower-case representation of tablename:
        status_table_exists_results = self.LocutusDBconnSession.execute('SELECT EXISTS '\
                                            '(SELECT FROM pg_tables WHERE schemaname = \'public\' '\
                                            'AND tablename = LOWER(\'{0}\') );'.format(
                                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE))
        status_table_exists_results_row = status_table_exists_results.fetchone()
        status_table_exists = status_table_exists_results_row['exists']

        alter_status_table = False
        if status_table_exists:
            print('{0}.Process(): LOCUTUS_ONPREM_DICOM_STATUS_TABLE found at {1}, '\
                    'checking for Juneteenth 2025 Upgrade from numeric to alpha-numeric accession_num columns.....'.format(
                    CLASS_PRINTNAME,
                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                    flush=True)
            # NOTE: be sure to use LOWER to align with the postgres lower-case representation of table_name as well:
            # FOLLOWING the alpha-numeric upgrade of Juneteenth 2025, deprecating accession_num_src as used in antiquated approach of SPLITS for multi-UUIDs....
            status_table_acc_column_results = self.LocutusDBconnSession.execute('SELECT column_name, data_type '\
                                                'FROM information_schema.columns '\
                                                'WHERE table_name = LOWER(\'{0}\') '\
                                                'AND column_name IN (\'accession_num\') '\
                                                'ORDER BY column_name;'.format(
                                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE))

            status_table_acc_column_row = status_table_acc_column_results.fetchone()
            # SHOULD be column #1 = accession_num, but no worries either way:
            status_table_acc_column_row_colname = status_table_acc_column_row['column_name']
            status_table_acc_column_row_datatype = status_table_acc_column_row['data_type']
            if status_table_acc_column_row_datatype == 'numeric':
                alter_status_table = True

            #WAS: with prior accession_num_src, now deprecated from old SPLITS:
            """
            status_table_acc_column_row = status_table_acc_column_results.fetchone()
            # SHOULD be column #2 = accession_num_src, but no worries either way:
            status_table_acc_column_row_colname = status_table_acc_column_row['column_name']
            status_table_acc_column_row_datatype = status_table_acc_column_row['data_type']
            if status_table_acc_column_row_datatype == 'numeric':
                # NOTE: flag to ALTER the STATUS table for later alteration:
                # FIRST, a quick check for column consistency:
                # WAS: WITH:      'but NOT this 2nd (accession_num_src) column! '\
                if not alter_status_table:
                    print('{0}.Process(): WARNING: found mid-UPGRADE conflict with {1}, '\
                            'STATUS table already upgraded from numeric to alpha-numeric for 1st (accession_num) columns! '\
                            'Upgrading it momentarily...'.format(
                            CLASS_PRINTNAME,
                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                            flush=True)
                alter_status_table = True
            """

            if alter_status_table:
                print('{0}.Process(): Altering LOCUTUS_ONPREM_DICOM_STATUS_TABLE {1} '\
                    'with Juneteenth 2025 Upgrade from numeric to alpha-numeric accession_num columns now.'.format(
                    CLASS_PRINTNAME,
                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                    flush=True)
                status_table_alter_results = self.LocutusDBconnSession.execute('ALTER TABLE {0}  '\
                                'ALTER COLUMN accession_num TYPE TEXT ;'.format(
                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE))
                # AND be sure to truncate any trailing .000 left over from the conversion from NUMERIC(18,3)
                status_table_alter_trunc_results = self.LocutusDBconnSession.execute('UPDATE {0}  '\
                                'SET  accession_num=REPLACE(accession_num, \'.000\', \'\') '\
                                'WHERE accession_num LIKE \'%.000\' ;'.format(
                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE))
            else:
                print('{0}.Process(): INFO: STATUS table {1} already upgraded from numeric to alpha-numeric '\
                        'through the Juneteenth 2025 Upgrade Path, YAY! '.format(
                            CLASS_PRINTNAME,
                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                            flush=True)

        ####################
        # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
        # NOTE: be sure to use LOWER to align with the postgres lower-case representation of tablename:
        manifest_table_exists_results = self.LocutusDBconnSession.execute('SELECT EXISTS '\
                                            '(SELECT FROM pg_tables WHERE schemaname = \'public\' '\
                                            'AND tablename = LOWER(\'{0}\') );'.format(
                                            LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE))
        manifest_table_exists_results_row = manifest_table_exists_results.fetchone()
        manifest_table_exists = manifest_table_exists_results_row['exists']

        alter_manifest_table = False
        if manifest_table_exists:
            print('{0}.Process(): LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE found at {1}, '\
                    'checking for Juneteenth 2025 Upgrade from numeric to alpha-numeric accession_num column.....'.format(
                    CLASS_PRINTNAME,
                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE),
                    flush=True)
            # NOTE: be sure to use LOWER to align with the postgres lower-case representation of table_name as well:
            manifest_table_acc_column_results = self.LocutusDBconnSession.execute('SELECT column_name, data_type '\
                                                'FROM information_schema.columns '\
                                                'WHERE table_name = LOWER(\'{0}\') '\
                                                'AND column_name IN (\'accession_num\') '\
                                                'ORDER BY column_name;'.format(
                                                LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE))

            manifest_table_acc_column_row = manifest_table_acc_column_results.fetchone()
            # SHOULD be column #1 = accession_num, but no worries either way:
            manifest_table_acc_column_row_colname = manifest_table_acc_column_row['column_name']
            manifest_table_acc_column_row_datatype = manifest_table_acc_column_row['data_type']
            if manifest_table_acc_column_row_datatype == 'numeric' or manifest_table_acc_column_row_datatype== 'bigint':
                # NOTE: flag to ALTER the MANIFEST table HERE for later alteration,
                # BUT FIRST, a quick check on the previous alter_status, for consistency:
                if not alter_status_table:
                    print('{0}.Process(): WARNING: found mid-UPGRADE conflict with {1}, '\
                            'STATUS table already upgraded from numeric to alpha-numeric accession_num columns, but NOT this MANIFEST table {2}! '\
                            'Upgrading it momentarily...'.format(
                            CLASS_PRINTNAME,
                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                            LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE),
                            flush=True)
                alter_manifest_table = True
            else:
                # another quick check on the previous alter_status, for consistency:
                if alter_status_table:
                    print('{0}.Process(): WARNING: found mid-UPGRADE conflict with {1}, '\
                            'STATUS table NEEDING upgrade from numeric to alpha-numeric accession_num columns, but NOT this MANIFEST table {2}! '.format(
                            CLASS_PRINTNAME,
                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                            LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE),
                            flush=True)

            if alter_manifest_table:
                print('{0}.Process(): Altering LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE {1} '\
                    'with Juneteenth 2025 Upgrade from numeric to alpha-numeric accession_num column now.'.format(
                    CLASS_PRINTNAME,
                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE),
                    flush=True)
                manifest_table_alter_results = self.LocutusDBconnSession.execute('ALTER TABLE {0}  '\
                                'ALTER COLUMN accession_num TYPE TEXT;'.format(
                                LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE))
                # AND be sure to truncate any trailing .000 left over from the conversion from NUMERIC(18,3)
                status_table_alter_trunc_results = self.LocutusDBconnSession.execute('UPDATE {0}  '\
                                'SET  accession_num=REPLACE(accession_num, \'.000\', \'\') '\
                                'WHERE accession_num LIKE \'%.000\' ;'.format(
                                LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE))
            else:
                print('{0}.Process(): INFO: MANIFEST table {1} already upgraded from numeric to alpha-numeric '\
                        'through the Juneteenth 2025 Upgrade Path, YAY! '.format(
                            CLASS_PRINTNAME,
                            LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE),
                            flush=True)

        if alter_status_table or alter_manifest_table or self.locutus_settings.LOCUTUS_DICOM_FORCE_ALPHANUM_REUPGRADE_DURING_MIGRATION:
            if self.locutus_settings.LOCUTUS_DICOM_FORCE_ALPHANUM_REUPGRADE_DURING_MIGRATION:
                print('{0}.Process(): because LOCUTUS_DICOM_FORCE_ALPHANUM_REUPGRADE_DURING_MIGRATION={1},'\
                    'will force a RE-UPGRADE of the Juneteenth 2025 Upgrade from numeric to alpha-numeric accession_num columns now.'.format(
                    CLASS_PRINTNAME,
                    self.locutus_settings.LOCUTUS_DICOM_FORCE_ALPHANUM_REUPGRADE_DURING_MIGRATION),
                    flush=True)
            ####################
            # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions:
            # Now, do the automatic conversion for any of the existing Stager accessions with text accessions, as per...
            # trig_dicom_staging=# select count(*) from dicom_stablestudies where text(accession_num) != accession_str;
            print('{0}.Process(): Updating all STATUS_TABLE ({1}) & MANIFEST TABLE ({2}) accessions with Staged text '\
                'with Juneteenth 2025 Upgrade from numeric to alpha-numeric accession_num columns now.'.format(
                CLASS_PRINTNAME,
                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE),
                flush=True)
            self.upgrade_alphanum_accessions_for_Juneteenth(LOCUTUS_ONPREM_DICOM_STATUS_TABLE, LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE)

        if not status_table_exists:
            # Create LOCUTUS_ONPREM_DICOM_STATUS_TABLE:
            print('{0}.Process(): About to CREATE TABLE LOCUTUS_ONPREM_DICOM_STATUS_TABLE: {1}'.format(
                CLASS_PRINTNAME,
                LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                flush=True)
            # NOTE: & no the "if not exists" clause isn't as needed with the above conditional
            # NO LONGER: including an 'as_change_seq_id' for squashing multiple change_seq_ids on the same uuid:
            # NOTE: no comma before {1} in the below, as it may NOT appear if int_cfg_status_fields_str is empty:
            # NOTE: prior to Juneteenth 2025 Upgrade of MANIFEST TABLE accession_num + _num_src to TEXT,
            #                       WAS: 'accession_num decimal(18,3), '\
            #                       WAS: 'accession_num_src bigint, '\
            # THEN, with the Juneteenth 2025 alpha-numeric upgrade,
            #                       WAS: 'accession_num_src text, '\
            # 6/19/2025: Added active, retiring the need for wacky accession math (accnum<0 or accstr[0]='-') to determine if retired
            if not self.locutus_settings.LOCUTUS_TEST:
                self.LocutusDBconnSession.execute('CREATE TABLE if not exists {0} ('\
                                    'change_seq_id int, '\
                                    'change_type text, '\
                                    'uuid text, '\
                                    'subject_id text, '\
                                    'object_info_01 text, '\
                                    'object_info_02 text, '\
                                    'object_info_03 text, '\
                                    'object_info_04 text, '\
                                    'accession_num text, '\
                                    'active boolean, '\
                                    'src_orthanc_notes text, '\
                                    'datetime_processed timestamp without time zone, '\
                                    'phase_processed int, '\
                                    'identified_local_path text, '\
                                    'deidentified_local_path text, '\
                                    'deidentified_targets text, '\
                                    'deid_qc_status text, '\
                                    'deid_qc_api_study_url text, '\
                                    'deid_qc_explorer_study_url text '\
                                    '{1}'
                                    ');'.format(LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                int_cfg_status_fields_str))

        if not manifest_table_exists:
            # Create LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE:
            print('{0}.Process(): About to CREATE TABLE LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE: {1}'.format(
                CLASS_PRINTNAME,
                LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE),
                flush=True)
            # NOTE: & no the "if not exists" clause isn't as needed with the above conditional
            # NOTE: prior to Juneteenth 2025 Upgrade of MANIFEST TABLE accession_num to TEXT,
            #                       WAS: 'accession_num decimal(18,3), '\
            # 6/19/2025: Added active, retiring the need for wacky accession math (accnum<0 or accstr[0]='-') to determine if retired
            if not self.locutus_settings.LOCUTUS_TEST:
                self.LocutusDBconnSession.execute('CREATE TABLE if not exists {0} ('\
                                    'subject_id text, '\
                                    'object_info_01 text, '\
                                    'object_info_02 text, '\
                                    'object_info_03 text, '\
                                    'object_info_04 text, '\
                                    'accession_num text, '\
                                    'active boolean, '\
                                    'last_datetime_processed timestamp without time zone, '\
                                    'manifest_status text '\
                                    ');'.format(LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE))

        if self.locutus_settings.LOCUTUS_DICOM_BYPASS_MIGRATION:
            print('{0}.Process(): PHASE01a: INFO: LOCUTUS_DICOM_BYPASS_MIGRATION bypassing pre-Migration zombie removals.'.format(
                    CLASS_PRINTNAME),
                    flush=True)
            print('{0}.Process(): PHASE01b: INFO: LOCUTUS_DICOM_BYPASS_MIGRATION bypassing pre-Migration old change_seq_id removals.'.format(
                    CLASS_PRINTNAME),
                    flush=True)
            print('{0}.Process(): PHASE02: INFO: LOCUTUS_DICOM_BYPASS_MIGRATION bypassing Migration proper.'.format(
                    CLASS_PRINTNAME),
                    flush=True)
        else:
            # Processing Phase01a: pre-Migration, part of it in regards to LOCUTUS_DICOM_BYPASS_MIGRATION
            ###########################################################
            # load up lists of all change_seq_ids in the GCPDICOM_OUTPUT_STABLESTUDY_TABLE
            # and those change_seq_ids already in: LOCUTUS_ONPREM_DICOM_STATUS_TABLE

            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            print('{0}.Process(): START of PHASE01: pre-Migrating for this batch...'.format(CLASS_PRINTNAME), flush=True)
            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: START of PHASE01 batch'.format(nowtime), flush=True)

            total_xtra_changes_removed = 0

            # limit to active (non-retired) accession_nums > 0 with change_seq_id > 0:
            #################
            # NOTE: ENHANCED 6/03/2025 to only list staged_changes as those which are the MAX(change_seq_id) for any UUID,
            # so as to align with the removal of as_change_seq_id, and as inspired by the new extra_changes_result, further below:
            #   extra_changes_result =  self.LocutusDBconnSession.execute('WITH temp_num_changes AS '\ [...] LOCUTUS_ONPREM_DICOM_STATUS_TABLE))
            # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
            staged_changes_result =  self.StagerDBconnSession.execute('WITH temp_max_stage_changes AS '\
                                                                        '(SELECT accession_num, uuid, '\
                                                                        '  MAX(change_seq_id) AS max_change_seq_id '
                                                                        '  FROM {0} '\
                                                                        '  WHERE active '\
                                                                        '  GROUP BY accession_num, uuid '\
                                                                        ') '\
                                                                        'SELECT accession_num, uuid, max_change_seq_id as change_seq_id '\
                                                                        'FROM temp_max_stage_changes '\
                                                                        'ORDER by change_seq_id asc;'.format(
                                                                        STAGER_STABLESTUDY_TABLE))

            staged_changes_list = []
            for row in staged_changes_result:
                staged_changes_list.append(row['change_seq_id'])
            # TODO: may want to create an EXTREME_VERBOSE for the following lists of staged changes:
            # Until then, it's getting large enough that it's time to mute this one:
            #
            #if self.locutus_settings.LOCUTUS_VERBOSE:
            #    print('{0}.Process(): PHASE01: ONPREM-DICOM-STAGED list of change ids == {1}'.format(
            #                    CLASS_PRINTNAME,
            #                    staged_changes_list),
            #                    flush=True)
            #

            # limit to active (non-retired) accession_nums > 0 with change_seq_id > 0:
            # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
            status_changes_result =  self.LocutusDBconnSession.execute('SELECT change_seq_id FROM {0} '\
                                'WHERE active '\
                                'order by change_seq_id ;'.format(
                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE))
            status_changes_list = []
            for row in status_changes_result:
                status_changes_list.append(row['change_seq_id'])
            # TODO: may want to create an EXTREME_VERBOSE for the following lists of staged changes:
            # Until then, it's getting large enough that it's time to mute this one:
            #
            #if self.locutus_settings.LOCUTUS_VERBOSE:
            #    print('{0}.Process(): PHASE01: Locutus known status list of change '\
            #                    'ids == {1}'.format(
            #                    CLASS_PRINTNAME,
            #                    status_changes_list),
            #                    flush=True)
            #

            #print('r3m0 DEBUG: ABOUT to test ALL OnPrem zombies from the entire set of possibilities', flush=True)
            possible_zombie_changes_list = [item for item in status_changes_list if item not in staged_changes_list]
            #print('{0}.Process(): PHASE01: WARNING: The current Locutus OnPrem workspace '\
            #            'status table ({1}) contains the following possible (including already retired!) '\
            #            'zombie change_seq_ids no longer in the GCP DICOM STAGE: {2}'.format(
            #            CLASS_PRINTNAME,
            #            LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
            #            possible_zombie_changes_list),
            #            flush=True)
            # NOTE: filtering down to only those which have not already been retired, the positive ones:
            #print('r3m0 DEBUG: ABOUT to test ONLY the POSITIVE subset of zombies from the entire set of possibilities', flush=True)
            # NOTE: refined staged_changes_result to now only query those positive changes:
            positive_zombie_changes_list = [item for item in possible_zombie_changes_list if item > 0]
            total_positive_zombie_changes_TO_BE_removed = len(positive_zombie_changes_list)
            total_positive_zombie_changes_removed = 0

            if not total_positive_zombie_changes_TO_BE_removed:
                print('{0}.Process(): PHASE01a: YAY: no positive zombie change_seq_ids in this Locutus OnPrem workspace '\
                            'status table, {1}'.format(
                            CLASS_PRINTNAME,
                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                            flush=True)
            else:
                print('{0}.Process(): PHASE01a: WARNING: The current Locutus OnPrem workspace '\
                            'status table ({1}) contains the following positive (not yet retired) zombie change_seq_ids '\
                            'no longer in the STAGER_STABLESTUDY_TABLE: {2}'.format(
                            CLASS_PRINTNAME,
                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                            positive_zombie_changes_list),
                            flush=True)
                ###################################################
                # temp debug to immediately bail until confirmed:
                #print('r3m0 DEBUG: testing positive subset of zombies from the entire set of possibilities', flush=True)
                #raise ValueError('r3m0 DEBUG: testing positive subset of zombies from the entire set of possibilities')
                ###################################################
                # NOTE: though usually aiming to not print something every time if in
                # continuous mode, just do it anyhow, as it IS indeed a warning to note.
                # Furthermore, now supporting the following new "remove zombies" setting
                # in order to best maintain consistency across all workspaces:
                if not self.locutus_settings.LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION:
                    print('{0}.Process(): PHASE01a: WARNING: to retire/remove the above positive zombie change_seq_ids, '\
                            'please set LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION'.format(
                            CLASS_PRINTNAME),
                            flush=True)
                else:
                    # self.locutus_settings.LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION:
                    print('{0}.Process(): PHASE01a:  LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION set; '\
                            'about to retire/remove the above zombie change_seq_ids from '\
                            'this Locutus OnPrem workspace status table ({1}).'.format(
                            CLASS_PRINTNAME,
                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                            flush=True)
                    # NOTE: even with the subsequent extra_changes_result
                    for curr_zombie_change_seq_id in positive_zombie_changes_list:
                        print('{0}.Process(): PHASE01a: Retiring/removing '\
                            'zombie change_seq_id: {1}'.format(
                            CLASS_PRINTNAME,
                            curr_zombie_change_seq_id),
                            flush=True)
                        self.preretire_change_status_only(curr_zombie_change_seq_id)
                        total_positive_zombie_changes_removed += 1

            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE01a batch'.format(nowtime), flush=True)
            # emit the Phase01 MIGRATION STATS:
            print('{0},# Phase01a,NUM_ZOMBIE_CHANGES_TO_REMOVE:,{1},from,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        total_positive_zombie_changes_TO_BE_removed,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)
            print('{0},# Phase01a,NUM_ZOMBIE_CHANGES_REMOVED:,{1},from,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        total_positive_zombie_changes_removed,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)
            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)

            # Migration Processing Phase01b-01:
            # == blanket removal of ALL older change_seq_ids (all but the latest change_seq_id for *any* UUID),
            #   prior to Phase02 migration of new changes, one UUID at a time.
            # NOTE: see also subsequent Phase01b-02 per-UUID backup safety net in Phase02,
            # for any remaining extraneous per-UUID change_seq_ids that somehow got through.
            ###########################################################
            if not self.locutus_settings.LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION:
                print('{0}.Process(): PHASE01b-01: WARNING: to retire/remove extraneous pre-MAX change_seq_ids, '\
                        'please set LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION'.format(
                        CLASS_PRINTNAME),
                        flush=True)
            else:
                print('{0}.Process(): PHASE01b-01: pre-Migration old change_seq_id removals...'.format(
                        CLASS_PRINTNAME),
                        flush=True)

                print('{0}.Process(): PHASE01b-01: pre-Migration querying extra_changes_results...'.format(
                        CLASS_PRINTNAME),
                        flush=True)
                # NOTE: query a count of num_changes per grouped accession_num, uuid:
                # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
                extra_changes_result =  self.LocutusDBconnSession.execute('WITH temp_num_changes AS '\
                                                                            '(SELECT accession_num, uuid, '\
                                                                            '  MAX(change_seq_id) AS max_change_seq_id, '
                                                                            '  COUNT(DISTINCT(change_seq_id)) AS num_changes '\
                                                                            '  FROM {0} '\
                                                                            '  WHERE active '\
                                                                            '  GROUP BY accession_num, uuid '\
                                                                            ') '\
                                                                            'SELECT accession_num, uuid, max_change_seq_id, num_changes '\
                                                                            'FROM temp_num_changes '\
                                                                            'WHERE num_changes > 1 '\
                                                                            'ORDER by num_changes desc;'.format(
                                                                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE))
                # w/ total_xtra_changes_removed = 0
                for xtra_change_row in extra_changes_result:
                    curr_xtra_change_accession_num = xtra_change_row['accession_num']
                    curr_xtra_change_uuid = xtra_change_row['uuid']
                    curr_xtra_max_changeseqid = xtra_change_row['max_change_seq_id']
                    curr_xtra_change_num_changes = xtra_change_row['num_changes']

                    print('{0}.Process(): PHASE01b-01: pre-Migration about to clear first {1} of '\
                            'num_changes={2} for acc={3}, uuid=\'{4}\' '\
                            'w/ change_seq_id < {5}, '\
                            'from {6}.'.format(
                            CLASS_PRINTNAME,
                            curr_xtra_change_num_changes-1,
                            curr_xtra_change_num_changes,
                            str(self.remove_trailing_zeros(curr_xtra_change_accession_num)),
                            curr_xtra_change_uuid,
                            curr_xtra_max_changeseqid,
                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                            flush=True)

                    #####
                    # NOTE: ensuring that this is only pulling the info for ACTIVE uuids
                    # (i.e., non-retired, w/ POSITION(\'-\' IN accession_num) = 0 & change_seq_id > 0)
                    # lest previously (as above) retired versions of this trigger false multiples:
                    # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
                    self.LocutusDBconnSession.execute('DELETE FROM {0} '\
                                                'WHERE uuid=\'{1}\' '\
                                                '    AND active '\
                                                'AND change_seq_id < {2} ;'.format(
                                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                curr_xtra_change_uuid,
                                                curr_xtra_max_changeseqid))
                    total_xtra_changes_removed += curr_xtra_change_num_changes-1

                print('{0}.Process(): PHASE01b-01: pre-Migration cleared '\
                        'total of {1} extraneous former change_seq_ids '\
                        'from {2}.'.format(
                        CLASS_PRINTNAME,
                        total_xtra_changes_removed,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)

            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE01b-01 batch'.format(nowtime), flush=True)
            # emit the Phase01b MIGRATION STATS:
            print('{0},# Phase01b-01,NUM_BULK_XTRA_CHANGES_REMOVED:,{1},from,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        total_xtra_changes_removed,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)
            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            print('{0}.Process(): END of PHASE01: pre-Migrating for this batch...'.format(CLASS_PRINTNAME), flush=True)
            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE01 batch'.format(nowtime), flush=True)


            # Migration Processing Phase02:
            ###########################################################
            # Load any new changes from STAGER_STABLESTUDY_TABLE
            changes_needing_migrating_list = [item for item in staged_changes_list if item not in status_changes_list]

            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE02: change ids needing migrating == {1}'.format(
                            CLASS_PRINTNAME,
                            changes_needing_migrating_list),
                            flush=True)

            #prev_phase_processed = MIN_PROCESSING_PHASE-1:
            prev_phase_processed = 1
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE02: migrate outstanding '\
                        'STAGER_STABLESTUDY_TABLE changes -> '\
                        'LOCUTUS_ONPREM_DICOM_STATUS_TABLE:'.format(
                        CLASS_PRINTNAME),
                        flush=True)

            # Only migrate those which have not yet made it into LOCUTUS;
            # WAS: processed_uuids_in_this_phase = False
            # WAS: if not processed_uuids_in_this_phase:
            # NOTE: now printing PHASE02 & its STATS whether or not any to actually MIGRATE:
            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            print('{0}.Process(): START of PHASE02: phase processing this batch...'.format(CLASS_PRINTNAME), flush=True)
            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: START of PHASE02 batch'.format(nowtime), flush=True)
            # WAS: processed_uuids_in_this_phase = True

            # NOTE: could just use len(changes_needing_migrating_list), but  let's recount anyhow, because we can,
            # AND because num_changes_migrated_in_Phase02 shall be incremented only if !LOCUTUS_TEST, to compare:
            num_changes_migrated_in_Phase02 = 0
            total_xtra_changes_removed = 0
            total_xtra_changes_NOTYET_removed = 0
            for needed_change_id in changes_needing_migrating_list:

                staged_change_result = self.StagerDBconnSession.execute('SELECT change_seq_id, '\
                                                                    'uuid, change_type, '\
                                                                    'accession_num, '\
                                                                    'accession_str '\
                                                                    'FROM {0} '\
                                                                    'WHERE change_seq_id={1} '\
                                                                    '    AND active ;'.format(
                                                                    STAGER_STABLESTUDY_TABLE,
                                                                    needed_change_id))
                row = staged_change_result.fetchone()
                staged_change = row['change_seq_id']
                staged_change_type = row['change_type']
                staged_uuid = row['uuid']
                ####################
                # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions:
                # WAS: staged_accession_num = row['accession_num']
                staged_accession_num = row['accession_str']
                ####################

                test_msg = ""
                if self.locutus_settings.LOCUTUS_TEST:
                    test_msg = " TEST MODE: not actually"
                print('{0}.Process(): PHASE02:{1} migrating new change {2} with uuid {3} '\
                        'into the current Locutus OnPrem workspace status table ({4})'.format(
                                            CLASS_PRINTNAME,
                                            test_msg,
                                            staged_change,
                                            staged_uuid,
                                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                                            flush=True)
                if not self.locutus_settings.LOCUTUS_TEST:
                    # NOTE: no need for accession_num_src FOLLOWING the alpha-numeric upgrade of Juneteenth 2025, in which accession_num_src might have been used to detect any previous SPLITS....
                    # NOTE: redundantly saving staged_accession_num as both accession_num_src AND accession_num_src
                    # to support the later manual splitting of multiple DICOM studies/sessions which do happen
                    # to share the same accession number.
                    #
                    # For example, if two separate studies of accession # "1234" appear, a first pass
                    # of processing might result in manifest_status = 'ERROR_MULTIPLE_CHANGE_UUIDS'.
                    # Their accession_num_src values could be left as is,
                    # and their accession_num values changed to something like: 1234.001 & 1234.002
                    #
                    # NO LONGER USING: initially migrated accessions have as_change_seq_id=NULL.  **VERY IMPORTANT!**

                    # CHECK for existing uuid STATUS record to UPDATE rather than INSERT:
                    #####
                    # NOTE: ensuring that this is only pulling the info for ACTIVE uuids
                    # (i.e., non-retired, w/ POSITION(\'-\' IN accession_num) = 0 & change_seq_id > 0)
                    # lest previously (as above) retired versions of this trigger false multiples:
                    # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
                    existing_changes_for_uuid_result =  self.LocutusDBconnSession.execute('SELECT '\
                                        'count(distinct(change_seq_id)) as num_changes, '\
                                        'max(distinct(change_seq_id)) as max_changeseqid '\
                                        'FROM {0} '\
                                        'WHERE uuid=\'{1}\' '\
                                        '    AND active ;'.format(
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        staged_uuid))
                    existing_changes_for_uuid_row = existing_changes_for_uuid_result.fetchone()
                    num_locutus_changes_for_uuid = existing_changes_for_uuid_row['num_changes']
                    max_locutus_changeseqid_for_uuid = existing_changes_for_uuid_row['max_changeseqid']

                    if num_locutus_changes_for_uuid > 0:
                        # FIRST remove any extraneous change_seq_id, prior to the MAX(change_seq_id)
                        if num_locutus_changes_for_uuid > 1:

                            # emit a WARNING that these should not be encountered anymore ONCE self.locutus_settings.LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION:
                            print('{0}.Process(): WARNING: PHASE01b-02-A: unexpectedly encountered; '\
                                        'all should be cleared out by now ONCE deployed '\
                                        'with LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION.'.format(
                                        CLASS_PRINTNAME),
                                        flush=True)

                            # delete all change_seq_id for this uuid EXCEPT max_locutus_changeseqid_for_uuid:
                            #####
                            # NOTE: ensuring that this is only pulling the info for ACTIVE uuids
                            # (i.e., non-retired, w/ POSITION(\'-\' IN accession_num) = 0 & change_seq_id > 0)
                            # lest previously (as above) retired versions of this trigger false multiples:
                            # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
                            if self.locutus_settings.LOCUTUS_VERBOSE:
                                print('{0}.Process(): PHASE01b02: about to remove any existing outstanding '\
                                    'Stager STABLESTUDY changes for an EXISTING uuid in '\
                                    'the current Locutus OnPrem workspace status table ({1}) via DELETE : ' \
                                    'WHERE uuid=\'{2}\' '\
                                    '    AND active '\
                                    'AND change_seq_id < {3} ;'.format(
                                    CLASS_PRINTNAME,
                                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                    staged_uuid,
                                    staged_change))

                            if not self.locutus_settings.LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION:
                                print('{0}.Process(): WARNING: PHASE01b-02-A: per-UUID Migration requires '\
                                        'LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION to clear first {1} of '\
                                        'num_changes={2} for acc={3}, uuid=\'{4}\', '\
                                        'w/ change_seq_id < {5}, '\
                                        'from {6}.'.format(
                                        CLASS_PRINTNAME,
                                        total_xtra_changes_removed-1,
                                        total_xtra_changes_removed,
                                        str(self.remove_trailing_zeros(curr_xtra_change_accession_num)),
                                        curr_xtra_change_uuid,
                                        max_locutus_changeseqid_for_uuid,
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                                        flush=True)
                                total_xtra_changes_NOTYET_removed += total_xtra_changes_removed-1
                            else:
                                print('{0}.Process(): PHASE01b-02-A: per-UUID Migration about to clear first {1} of '\
                                        'num_changes={2} for acc={3}, uuid=\'{4}\', '\
                                        'w/ change_seq_id < {5}, '\
                                        'from {6}.'.format(
                                        CLASS_PRINTNAME,
                                        total_xtra_changes_removed-1,
                                        total_xtra_changes_removed,
                                        str(self.remove_trailing_zeros(curr_xtra_change_accession_num)),
                                        curr_xtra_change_uuid,
                                        max_locutus_changeseqid_for_uuid,
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                                        flush=True)
                                #####
                                # NOTE: ensuring that this is only pulling the info for ACTIVE uuids
                                # (i.e., non-retired, w/ POSITION(\'-\' IN accession_num) = 0 & change_seq_id > 0)
                                # lest previously (as above) retired versions of this trigger false multiples:
                                # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
                                self.LocutusDBconnSession.execute('DELETE FROM {0} '\
                                                'WHERE uuid=\'{1}\' '\
                                                '    AND active '\
                                                'AND change_seq_id < {2} ;'.format(
                                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                staged_uuid,
                                                max_locutus_changeseqid_for_uuid))
                                total_xtra_changes_removed += total_xtra_changes_removed-1

                        # THEN UPDATE the existing STATUS record, merely bumping its change_seq_id to the latest and greatest
                        #####
                        # NOTE: ensuring that this is only pulling the info for ACTIVE uuids
                        # (i.e., non-retired, w/ POSITION(\'-\' IN accession_num) = 0 & change_seq_id > 0)
                        # lest previously (as above) retired versions of this trigger false multiples:
                        # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): PHASE02: about to migrate outstanding '\
                                'Stager STABLESTUDY changes for an EXISTING uuid in '\
                                'the current Locutus OnPrem workspace status table ({1}) via UPDATE of: ' \
                                'SET change_seq_id={2} '\
                                'WHERE uuid=\'{3}\' '\
                                '   AND active ;'.format(
                                CLASS_PRINTNAME,
                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                staged_change,
                                staged_uuid))
                        #####
                        # NOTE: ensuring that this is only pulling the info for ACTIVE uuids
                        # (i.e., non-retired, w/ POSITION(\'-\' IN accession_num) = 0 & change_seq_id > 0)
                        # lest previously (as above) retired versions of this trigger false multiples:
                        # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
                        self.LocutusDBconnSession.execute('UPDATE {0} SET change_seq_id={1} '\
                                            'WHERE uuid=\'{2}\' '\
                                            '   AND active ;'.format(
                                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                            staged_change,
                                            staged_uuid))
                    else:
                        # NOTE: no need for accession_num_src FOLLOWING the alpha-numeric upgrade of Juneteenth 2025, in which accession_num_src might have been used to detect any previous SPLITS....
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): PHASE02: about to migrate outstanding '\
                                'Stager STABLESTUDY changes for a NEW uuid to '\
                                'the current Locutus OnPrem workspace STATUS table ({1}) via INSERT of: ' \
                                '(change_seq_id, '\
                                'change_type, uuid, '\
                                'accession_num, '\
                                'active, '\
                                'phase_processed) '\
                                'VALUES({2}, "{3}", "{4}", '\
                                '\'{5}\', '\
                                'True, '\
                                '{6})'.format(
                                CLASS_PRINTNAME,
                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                staged_change,
                                staged_change_type,
                                staged_uuid,
                                staged_accession_num,
                                MIN_PROCESSING_PHASE))
                        self.LocutusDBconnSession.execute('INSERT INTO {0} ('\
                                            'change_seq_id, '\
                                            'change_type, uuid, '\
                                            'accession_num, '\
                                            'active, '\
                                            'phase_processed) '\
                                            'VALUES({1}, \'{2}\', \'{3}\', '\
                                            '\'{4}\', '\
                                            'True, '\
                                            '{5}) ;'.format(
                                            LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                            staged_change,
                                            staged_change_type,
                                            staged_uuid,
                                            staged_accession_num,
                                            MIN_PROCESSING_PHASE))
                    num_changes_migrated_in_Phase02 += 1

            if not self.locutus_settings.LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION:
                if total_xtra_changes_NOTYET_removed > 0:
                    print('{0}.Process(): WARNING: PHASE01b-02: per-UUID Migrations DID NOT clear '\
                        'the {1} extraneous former change_seq_ids specific to the {2} '\
                        'changes migrated; use LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION '\
                        'to properly clear these '\
                        'from {3}.'.format(
                        CLASS_PRINTNAME,
                        total_xtra_changes_NOTYET_removed,
                        num_changes_migrated_in_Phase02,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)
            else:
                # LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION:
                print('{0}.Process(): PHASE01b-02: per-UUID Migrations cleared out '\
                        'the {1} extraneous former change_seq_ids specific to the {2} '\
                        'changes migrated into {3}.'.format(
                        CLASS_PRINTNAME,
                        total_xtra_changes_NOTYET_removed,
                        num_changes_migrated_in_Phase02,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)

            # WAS: if processed_uuids_in_this_phase:
            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE02 batch'.format(nowtime), flush=True)
            # emit the Phase02 MIGRATION STATS (including new Phase01b-02):
            print('{0},# Phase01b-02,NUM_PER_MIGRATED_UUID_XTRA_CHANGES_REMOVED:,{1},from,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        total_xtra_changes_removed,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)
            print('{0},# Phase01b-02,NUM_PER_MIGRATED_UUID_XTRA_CHANGES_NOT_YET_REMOVED:,{1},from,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        total_xtra_changes_NOTYET_removed,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)
            print('{0},# Phase02,MIGRATION-READY:,{1},into,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        len(changes_needing_migrating_list),
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)
            print('{0},# Phase02,NUM_MIGRATED:,{1},into,{2}'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        num_changes_migrated_in_Phase02,
                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE),
                        flush=True)
            if self.locutus_settings.LOCUTUS_TEST and (num_changes_migrated_in_Phase02 != len(changes_needing_migrating_list)):
                print('{0},# Phase02,NOTE:,LOCUTUS_TEST enabled (above MIGRATION discrepancy to be expected)'.format(
                        MANIFEST_OUTPUT_PREFIX,
                        num_changes_migrated_in_Phase02),
                        flush=True)
            print('{0}.Process(): END of PHASE02: phase processing complete for this batch.'.format(CLASS_PRINTNAME), flush=True)
            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)

        # Processing Phase03:
        ###########################################################
        # == Download DICOM Dir and unzip, setting the first of many processed phases,
        # as initiated by the DICOM_MANIFEST file.
        #
        # Whether or not any new changes found from STAGER_STABLESTUDY_TABLE
        # in the above Phase02, go ahead and process any such
        # DICOM_MANIFEST file-based changes still at Phase02.
        # (NO LONGER with as_change_seq_id is NULL as well, since not yet consolidated/compressed/aggregated via as_change_seq_id).
        # [See also: CHANGE-SEQUENCE CONSOLIDATION:]
        #
        # Read distinct unprocessed uuid's from LOCUTUS_ONPREM_DICOM_STATUS_TABLE:
        #
        # NOTE: also WHERE change_seq_id >= 0, to allow negative change_seq_ids
        # when debugging potentially problematic downloads, e.g., when too large
        # due to too many studies, and resulting in a MEMORY ERROR, in Orthanc's
        #                 dicomdir_data = RestToolbox.DoGet(staged_url)
        #prev_phase_processed = 2
        prev_phase_processed = MIN_PROCESSING_PHASE
        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process(): PHASE03: DOWNLOAD & UNZIP, processing '\
                    'LOCUTUS_ONPREM_DICOM_STATUS_TABLE changes from the DICOM Manifest lines:'.format(
                    CLASS_PRINTNAME),
                    flush=True)

        processed_uuids_in_this_phase = False
        processRemainingPhases = True
        manifest_done = False
        manifest_accession_num_strs = []
        # NOTE: instead of a new manifest_accession_qc_attrs = {}
        # go ahead and expand the use of the following, momentary_manifest_attrs:
        momentary_manifest_attrs = {}
        rownum = 1
        try:
            # partA == initial manifest reading prior to the while not manifest_done loop
            curr_manifest_row = next(self.manifest_reader)
            while len(curr_manifest_row) == 0 or \
                    (not any(field.strip() for field in curr_manifest_row)) or \
                    curr_manifest_row[0] == '' or \
                    curr_manifest_row[0][0] == '#':
                # read across any blank manifest lines (or lines with only blank fields, OR lines starting with a comment, '#'):
                print('{0}.Process(): partA ignoring input manifest body line of "{1}"'.format(CLASS_PRINTNAME, curr_manifest_row), flush=True)
                if MANIFEST_OUTPUT_INCLUDE_COMMENT_LINES:
                    # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                    # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                    #if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0},{1}'.format(
                                MANIFEST_OUTPUT_PREFIX,
                                ','.join(curr_manifest_row)),
                                flush=True)
                curr_manifest_row = next(self.manifest_reader)
            print('{0}.Process(): partA head of while not manifest_done parsing input manifest body line of "{1}"'.format(CLASS_PRINTNAME, curr_manifest_row), flush=True)
        except StopIteration as e:
            manifest_done = True

        ######################################################################
        # System Status check
        # BEFORE this first manifest accession is processed, check Locutus DB..... System Status section:
        ######################################################################
        check_Docker_node=True
        alt_node_name=None
        check_module=False
        module_name=SYS_STAT_MODULENAME
        ####
        (curr_sys_stat_node, sys_msg_node, sys_node_name) = self.locutus_settings.get_Locutus_system_status(self.locutus_settings, check_Docker_node, alt_node_name, check_module, module_name, self.LocutusDBconnSession)
        print('{0},{1},node={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "onprem_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_node, sys_msg_node), flush=True)
        ####
        check_Docker_node=False
        check_module=True
        (curr_sys_stat_module, sys_msg_module, sys_module_name) = self.locutus_settings.get_Locutus_system_status(self.locutus_settings, check_Docker_node, alt_node_name, check_module, module_name, self.LocutusDBconnSession)
        print('{0},{1},module={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "onprem_GET_LOCUTUS_SYS_STATUS", sys_module_name, curr_sys_stat_module, sys_msg_module), flush=True)
        ####
        # only need to evaluate curr_sys_stat_node & curr_sys_stat_module, since it takes into account curr_sys_stat_overall during its calculation
        if (curr_sys_stat_node and curr_sys_stat_module):
            print('r3m0 DEBUG: OnPrem sees Locutus System Status is active for this node & module (and overall).... carry on!')
        else:
            run_loop = False
            print('FATAL ERROR: OnPrem sees Locutus System Status is NOT currently active for this '\
                    'module (status={0}) or node (status={1}); '\
                    'setting manifest_done to end the current manifest processing loop '\
                    '(and activating LOCUTUS_DISABLE_PHASE_SWEEP).'.format(
                        curr_sys_stat_module, curr_sys_stat_node), flush=True)
            # bail w/ this_run_has_fatal_errors:
            #this_run_has_fatal_errors = True
            # howzabout the less ominous manifest_done?
            manifest_done = True
            # Further, ensure that phase sweeps don't get picked up:
            self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP = True
        ###########

        last_accession_str = ''
        num_attempts_same_accession = 0
        while (not this_fatal_errors) and (not manifest_done) \
            and (num_attempts_same_accession < MAX_SAME_ACCESSION_ATTEMPTS):
            if not processed_uuids_in_this_phase:
                print('{0}.Process(): START of PHASE03: phase processing this batch...'.format(CLASS_PRINTNAME), flush=True)
                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: START of PHASE03 batch'.format(nowtime), flush=True)
                processed_uuids_in_this_phase = True
            skip_to_next_accession = False

            # TODO: rather than hardcoding the following CSV field #s, consider creating a dictionary:
            curr_subject_id = curr_manifest_row[0].strip()
            curr_object_info_01 = curr_manifest_row[1].strip()
            curr_object_info_02 = curr_manifest_row[2].strip()
            curr_object_info_03 = curr_manifest_row[3].strip()
            curr_accession_num = curr_manifest_row[4].strip()
            manifest_accession_str = curr_accession_num
            # MANIFEST_HEADER_DEID_QC_STATUS:
            curr_deid_qc_status = curr_manifest_row[5].strip()
            curr_processed_status = ''
            curr_deid_qc_api_study_url = ''

            # safety net against infinite loop repeating the same manifest line and accession
            # due to any continue'd bypass of the next manifest line via the reader,
            # for whatever reason:
            if manifest_accession_str == last_accession_str:
                num_attempts_same_accession += 1
            else:
                num_attempts_same_accession = 1
            last_accession_str = manifest_accession_str

            # and for likely redundant variable naming w/ this ^C/^V mod'd remove_text Level-Up code, also alias it to:
            accession_str = manifest_accession_str
            print('{0}.Process(): PHASE03: consecutive attempt # {1} for accession \'{2}\''.format(
                    CLASS_PRINTNAME, num_attempts_same_accession, manifest_accession_str), flush=True)
            if num_attempts_same_accession >= MAX_SAME_ACCESSION_ATTEMPTS:
                print('{0}.Process(): PHASE03: WARNING consecutive attempt # {1} for accession \'{2}\' is on its last attempt '\
                    'since soon exceeding MAX_SAME_ACCESSION_ATTEMPTS={3}'.format(
                        CLASS_PRINTNAME, num_attempts_same_accession, manifest_accession_str, MAX_SAME_ACCESSION_ATTEMPTS), flush=True)

            if (self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS):
                # NOTE: optional MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL fields
                # to follow MANIFEST_HEADER_MANIFEST_VER iff LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS:
                # MANIFEST_HEADER_OUTPUT_PROCESSED_STATUS:
                curr_processed_status = curr_manifest_row[7].strip()
                # MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL:
                curr_deid_qc_api_study_url = curr_manifest_row[8].strip()

            # reset manifest_status & whynot_manifest_status with each new manifest input accession:
            manifest_status = None
            whynot_manifest_status = None

            # add the current accession_num to all list of all from manifest,
            # regardless of its processing status, for subsequent comparison
            # with LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST:
            # [w/ the following as ^C/^V'd from module_gcp_dicom.pya]
            # WAS: safe_acc_num_str = str(self.remove_trailing_zeros(manifest_accession_str))
            # and for ^C/^V code compatibility, redundantly copy:
            ####################
            # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
            # WAS: accession_num = safe_acc_num_str
            accession_num = manifest_accession_str
            safe_acc_num_str = self.locutus_settings.atoi(manifest_accession_str)
            accession_num_COMPARE = safe_acc_num_str
            accession_restr = self.locutus_settings.itoa(accession_num_COMPARE)
            accession_is_ALPHAnumeric = True

            # NOTE: 6/19/2025 Ensuring that we are no longer using atoi()'d "safe_acc_num_str" beyond this comparison
            # else, BEWARE of early dev attempt w/ 1234AVH.005
            # that triggered the following if w/ 1234AVH.005 ~!= 1234AVH.005
            # and THEN tried to process it as 1234005
            if (manifest_accession_str != accession_restr):
                ####################
                # NOTE: 6/19/2025 Juneteenth Upgrade Path towards alpha-numeric accessions with the MANIFEST table:
                print('{0}.Process() PHASE03 found an alpha-numeric accession_str ~!= accession_num:  \'{1}\' ~!= atoi(\'{1}\')=={2} ;  '\
                        'Keeping calm and carrying on w/ accession_str=\'{1}\', courtesy of the Juneteenth 2025 alpha-numeric accessions Upgrade.'.format(
                        CLASS_PRINTNAME,
                        accession_str,
                        accession_restr),
                        flush=True)
                # NOTE: resetting the temporary COMPARE accession_restr, as it is used further below, prior to REMOVE_TEXT deprecation
                ##########
                #curr_accession_num = accession_str
                #safe_acc_num_str = accession_restr
                ##########
                # FORCING the raw accession_str back into accession_restr for "safe_acc_num_str" to later use:
                accession_restr = accession_str
                ##########
                # NOTE: self.locutus_settings.LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS deprecated
                ##########

            if not skip_to_next_accession:
                # NOTE: once the above has all been converted, can now refresh the other vars already used:
                # TODO: consider an overall update of curr_accession_num to curr_accession_numstr or something:
                #curr_accession_num = manifest_accession_str
                curr_accession_num = accession_str
                # and these ultimately both become the same;
                # TODO: combine and rename ;-)
                safe_acc_num_str = accession_restr

                if safe_acc_num_str in manifest_accession_num_strs:
                    # TODO: consider a non-fatal approach to logging this and any other duplicates?
                    # TODO: throw an exception here for duplicates?
                    # OR just mark it as such?
                    # As a first pass safety measure, throw exception, but first....

                    # TODO: throw an exception here for duplicates? OR just mark it as such?
                    # As a first pass safety measure, throw exception, but....
                    if self.locutus_settings.LOCUTUS_ALLOW_PROCESSING_OF_DUPLICATES:
                        # TODO: sure would be nice to recognize if the rest of the manifest row parameters are identical,
                        # and, if so, bypass such a reprocessing as well, merely noting as having already been processed, etc.
                        # Until then, though.... at least do allow the less efficient, but non-fatal, reprocessing:
                        print('{0}.Process(): PHASE03 '\
                                'encountered a duplicate input manifest row '\
                                'for Accession \'{1}\' as # \'{2}\', BUT processing again anyhow due to LOCUTUS_ALLOW_PROCESSING_OF_DUPLICATES'.format(
                                CLASS_PRINTNAME,
                                self.remove_trailing_zeros(curr_accession_num),
                                safe_acc_num_str))
                        manifest_accession_num_strs.append(safe_acc_num_str)
                    else:
                        new_exception_msg = 'ERROR_PHASE03: '\
                                'encountered a duplicate input manifest row '\
                                'for Accession# \'{1}\' (update manifest, or set LOCUTUS_ALLOW_PROCESSING_OF_DUPLICATES, to continue)'.format(
                                CLASS_PRINTNAME,
                                self.remove_trailing_zeros(curr_accession_num))
                        print('{0}.Process() PHASE03 throwing ValueError exception: {1}.'.format(
                            CLASS_PRINTNAME,
                            new_exception_msg),
                            flush=True)
                        # TODO: consider adding a FORCE_SUCCESS bypass of this ValueError():
                        # NOTE: for now, though, just consider setting that ALLOW_DUPLICATES flag...
                        #####
                        # WAS: raise ValueError('Locutus {0}.Process(): PHASE03 '\
                        #            'encountered a duplicate input manifest row '\
                        #            'for Accession# \'{1}\''.format(
                        #            CLASS_PRINTNAME,
                        #            self.remove_trailing_zeros(curr_accession_num)))
                        #####
                        print('{0}.Process() r3m0 DEBUG INFO: such that Process can complete remainder of the manifest, '\
                                'continuing while loop and **NOT** throwing this PHASE03 ValueError exception: {1}.'.format(
                                CLASS_PRINTNAME,
                                new_exception_msg),
                                flush=True)
                        skip_to_next_accession = True
                        # NOTE: it **seems** that no more outstanding manifest_status updates are needed for this
                        ############################# ############################# #############################

                if not skip_to_next_accession:
                    #WAS: else:
                    # NOTE: FIX for unswept zombie w/ manifest_state='PROCESSING_CHANGE_MOMENTARILY',
                    # following a previous fail during Phase04, w/ phase_processed=3.
                    # 9/30/2024 r3m0: Q: Why WAS? Once again NOT WAS:
                    #i.e., safe_acc_num_str NOT YET in manifest_accession_num_strs:
                    manifest_accession_num_strs.append(safe_acc_num_str)

                    # NOTE: passing the above manifest-based patient info into Process_Phase03...(),
                    # which will store these fields, to later be used by Process_Phase05...(),
                    # in creating this file's s3 name.  In the meantime, still using
                    # the uuid_<studyid> file format internally until Phase05's xfer to targets such as: AWS s3 & GCP GS.

                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                    print('{0}.Process(): START of PHASE03 for DICOM Manifest-based '\
                                    'Accession# \'{1}\' ...'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str),
                                    flush=True)
                    nowtime = datetime.datetime.utcnow()
                    print('@ {0} UTC: START of PHASE03 Accession# \'{1}\''.format(
                                    nowtime,
                                    safe_acc_num_str),
                                    flush=True)

                    if self.locutus_settings.LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS:
                        # print about the LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS regardless of VERBOSE:
                        print('{0}.Process(): PHASE03: INFO: LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS '\
                                    'set; returning to phase_processed={1} for Accession# \'{2}\''.format(
                                    CLASS_PRINTNAME,
                                    MIN_PROCESSING_PHASE,
                                    safe_acc_num_str),
                                    flush=True)
                        #WAS: self.reset_accession_phase_processed(safe_acc_num_str)
                        #from module_gcp_dicom.py:
                        # self.reset_accession_status_for_reprocessing(curr_accession_num, curr_subject_id, curr_object_info)
                        # localized to:
                        self.reset_accession_status_for_reprocessing(safe_acc_num_str, curr_subject_id, curr_object_info_01, curr_object_info_02, curr_object_info_03)
                    elif self.locutus_settings.LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS:
                        # print about the LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS regardless of VERBOSE:
                        print('{0}.Process(): PHASE03: WARNING: LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS '\
                                    'set; clearing out status records for Accession# \'{1}\''.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str),
                                    flush=True)
                        self.predelete_accession(safe_acc_num_str)
                    elif (self.locutus_settings.LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS \
                        and not self.locutus_settings.LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED):
                        # print about the LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS regardless of VERBOSE:
                        print('{0}.Process(): PHASE03: NOTE: LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS '\
                                    'enabled (without LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED); '\
                                    'retiring previous manifest in {1} for Accession# \'{2}\' '\
                                    'to its negative of Accession# \'-{2}\''.format(
                                    CLASS_PRINTNAME,
                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                    safe_acc_num_str),
                                    flush=True)
                        self.preretire_accession(safe_acc_num_str)


                    # NOTE: LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE  currently designed to have but a single entry
                    # for each Accession#, regardless of how many times a particular
                    # Accession# may have been re-processed (due to, for example, updated
                    # study or series images being transferred).  In general, it is to be
                    # presumed that all DICOM changes have already been submitted to Orthanc
                    # prior to running this from a Manifest, but... should subsequent
                    # changes be transferred, simply reprocess with that Accession# in
                    # a new manifest.
                    #
                    # Likewise, NOTE: that with this Manifest-driven processing, no new
                    # updates of any Accession# which has been previously processed
                    # shall trigger any subsequent automatic processing.
                    #
                    # That said, the tables are structured to support the future development
                    # of any reports which may aid in this process, such as:
                    #   * list any outstanding unprocessed Manifest Accession#s, pending changed uuids;
                    #   * list any outstanding unprocessed changed uuids, pending a Manifest Accession#;
                    # which could be used to help generate such future usable manifests.

                    # Ensure that LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE has a placeholder for each new subject_id+accession#
                    # e.g., before even checking on its actual processing, since that can be updated later:
                    manifest_accnum_result = self.LocutusDBconnSession.execute('SELECT COUNT(accession_num) as num '\
                                                        'FROM {0} '\
                                                        'WHERE accession_num=\'{1}\' '\
                                                        '    AND active ;'.format(
                                                        LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                        safe_acc_num_str))

                    manifest_accnum_row = manifest_accnum_result.fetchone()
                    manifest_accnum_count = manifest_accnum_row['num']
                    manifest_accnum_prev_status = '[none]'

                    if manifest_accnum_count == 1:
                        # NOTE: no worries even if this has previously been processed,
                        # since the arrival of a new uuid change will require re-processing.
                        # Otherwise, if no new change, do not mark as 'PENDING_CHANGE'.

                        # Get the previous status for later comparison against 'PENDING_CHANGE', etc:
                        manifest_status_prev_result = self.LocutusDBconnSession.execute('SELECT manifest_status, '\
                                                        'subject_id, object_info_01, object_info_02, object_info_03 '\
                                                        'FROM {0} '\
                                                        'WHERE accession_num=\'{1}\' '\
                                                        '    AND active ;'.format(
                                                        LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                        safe_acc_num_str))
                        manifest_status_prev_row = manifest_status_prev_result.fetchone()
                        manifest_status_prev_status = manifest_status_prev_row['manifest_status']
                        manifest_status_prev_subject_id = manifest_status_prev_row['subject_id']
                        manifest_status_prev_object_info_01 = manifest_status_prev_row['object_info_01']
                        manifest_status_prev_object_info_02 = manifest_status_prev_row['object_info_02']
                        manifest_status_prev_object_info_03 = manifest_status_prev_row['object_info_03']

                        # NOTE: go ahead and print this out for all, even if !self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.Process(): PHASE03: referencing existing DICOM '\
                                    'Manifest-based status row for Accession# \'{1}\', with previous manifest_status=\'{2}\', '\
                                    'and attributes=[{3}/{4}/{5}/{6}]'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str,
                                    manifest_status_prev_status,
                                    manifest_status_prev_subject_id,
                                    manifest_status_prev_object_info_01,
                                    manifest_status_prev_object_info_02,
                                    manifest_status_prev_object_info_03),
                                    flush=True)

                        # NOTE: go ahead and print this out for all, even if !self.locutus_settings.LOCUTUS_VERBOSE:
                        if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS:
                            print('{0}.Process(): PHASE03: NOT using LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS; '\
                                    'ignoring input deid_qc_status=\'{1}\''.format(
                                    CLASS_PRINTNAME,
                                    curr_deid_qc_status),
                                    flush=True)
                        else:
                            print('{0}.Process(): PHASE03: using LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS; '\
                                    'updating to input deid_qc_status=\'{1}\''.format(
                                    CLASS_PRINTNAME,
                                    curr_deid_qc_status),
                                    flush=True)

                            update_valid_deid_qc_status = False

                            ########
                            # NOTE: SAFETY CHECK, to ensure that the deid_qc_api_study_url in the *_DICOM_STATUS table
                            # matches that as supplied in the input manifest; if not, emit a WARNING and override w/ the STATUS value:
                            ########
                            deid_qc_api_study_url_via_status_result = self.LocutusDBconnSession.execute('SELECT '\
                                                    'deid_qc_api_study_url '\
                                                    'FROM {0} '\
                                                    'WHERE accession_num=\'{1}\' '\
                                                    '    AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                    safe_acc_num_str))
                            row = deid_qc_api_study_url_via_status_result.fetchone()

                            deid_qc_api_study_url_via_status = ''
                            # NOTE: watch out for non-existent url (e.g., if the use_manifest_QC is enabled, but none specified)
                            # May also want to ensure that deid_qc_status in the row is IGNORED if this NOT to manifest_use_QC_status?
                            if row \
                                and ('deid_qc_api_study_url' in row) \
                                and (row['deid_qc_api_study_url']):
                                deid_qc_api_study_url_via_status = row['deid_qc_api_study_url']
                            if  (curr_deid_qc_api_study_url != deid_qc_api_study_url_via_status):
                                # even if deid_qc_api_study_url_via_status is '', go ahead and apply it:
                                # print regardless of VERBOSE:
                                print('{0}.Process(): PHASE03: WARNING: DEID QC study url '\
                                    'from the input manifest (\'{1}\') differs from '\
                                    'that in the STATUS table (\'{2}\'); '\
                                    'using that in the STATUS table for Accession# \'{3}\''.format(
                                    CLASS_PRINTNAME,
                                    curr_deid_qc_api_study_url,
                                    deid_qc_api_study_url_via_status,
                                    safe_acc_num_str),
                                    flush=True)
                                curr_deid_qc_api_study_url = deid_qc_api_study_url_via_status

                            if (not curr_deid_qc_api_study_url \
                                and (curr_deid_qc_status.find('PASS_FROM_DEIDQC:') == 0)):
                                # If PASS_FROM_DEIDQC:*, specifically requires that the curr_deid_qc_api_study_url exists;
                                # all other values of curr_deid_qc_status (PASS:*, FAIL:*,. and REPROCESS:*) can proceed without.
                                # print regardless of VERBOSE
                                # (and note that now curr_deid_qc_api_study_url now matches deid_qc_api_study_url_via_status)
                                print('{0}.Process(): PHASE03: ERROR: empty DEID QC study url from '\
                                    'the STATUS table showing as (\'{1}\'); '\
                                    'unable to LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS for Accession# \'{2}\' '\
                                    'with curr_deid_qc_status=\'{3}\'; '\
                                    'consider setting deid_qc_status to anything other than PASS_FROM_DEIDQC '\
                                    '(e.g., PASS:*, FAIL:*, or REPROCESS:*)'.format(
                                    CLASS_PRINTNAME,
                                    curr_deid_qc_api_study_url,
                                    safe_acc_num_str,
                                    curr_deid_qc_status),
                                    flush=True)
                                raise ValueError('Locutus {0}.Process(): PHASE03: ERROR: empty DEID QC study url from '\
                                    'the STATUS table showing as (\'{1}\'); '\
                                    'unable to LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS for Accession# \'{2}\' '\
                                    'with curr_deid_qc_status=\'{3}\'; '\
                                    'consider setting deid_qc_status to anything other than PASS_FROM_DEIDQC '\
                                    '(e.g., PASS:*, FAIL:*, or REPROCESS:*)'.format(
                                    CLASS_PRINTNAME,
                                    curr_deid_qc_api_study_url,
                                    safe_acc_num_str,
                                    curr_deid_qc_status))

                            delay_reset_accession = False
                            delay_reset_accession_clear_deid_qc = True

                            # NOTE: eventually implement the full reprocessing PASS:*
                            # as part of https://github.research.chop.edu/dbhi/Locutus/issues/405 re: a subject_ID_preface
                            # NOTE: Preparing for both (the eventual) PASS:* (to reprocess without a DEIDQC_subject_ID_preface),
                            # and a PASS_FROM_DEIDQC:* (to pull directly from the ORTHANCDEIDQC, even if with such a preface)

                            # === PASS_FROM_DEIDQC:*
                            if curr_deid_qc_status.find('PASS_FROM_DEIDQC:') == 0:
                                update_valid_deid_qc_status = True

                                # NOTE: if already processed beyond the DEIDENTIFIED_PHASE,
                                # and this is just a straggler status being submitted,
                                # do NOT bother reprocessing it; instead just leave it at the greatest
                                # of the phase_processed values, itself of Phase04:

                                # WARNING: beware that we can't entirely rely upon the manifest_status_prev_status,
                                # because a previously FAILED one might have then been attempted to REPROCESS,
                                # at which point an error was encountered.
                                # SO, may also want to examine the current value of curr_deid_qc_api_study_url,
                                # and ensure that it is not None.
                                # COULD also look into using the previous deid_qc_status,
                                # but even that might not fully accurately reflect a mid-REPROCESS error.

                                if ((manifest_status_prev_status.find('FAIL:') != 0) \
                                    and curr_deid_qc_api_study_url \
                                    ):
                                    # NOTE: NOT previously with manifest_status == FAIL:*
                                    # or at least, still with a study sitting on ORTHANCDEIDQC.
                                    # NOTE: subtle difference in the call here (set_accession_phase_processed_to_greatest()),
                                    # as compared to that further below (set_accession_phase_processed()),
                                    # since this former one might NOT want to re-do a PASS on one that is already PROCESSED;
                                    # allow any prior progress to remain, but just setting to the greater 
                                    # of its current phase_procedssed & DEIDENTIFIED_PHASE:
                                    # NOTE: print regardless of VERBOSE:
                                    print('{0}.Process(): PHASE03: DEID QC STATUS advises to PASS; '\
                                            'BUMPING to greatest of (current phase_processed, DeIDed Phase {1} for Accession# \'{2}\''.format(
                                            CLASS_PRINTNAME,
                                            DEIDENTIFIED_PHASE,
                                            safe_acc_num_str),
                                            flush=True)
                                    self.set_accession_phase_processed_to_greatest(safe_acc_num_str, DEIDENTIFIED_PHASE)
                                elif ((manifest_status_prev_status.find('FAIL:') == 0) \
                                    and ( \
                                        (
                                            (not self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC \
                                            and curr_deid_qc_api_study_url
                                            )
                                        or self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES)
                                        )
                                    ):
                                    # NOTE: print regardless of VERBOSE:
                                    print('{0}.Process(): PHASE03: DEID QC found non-fail manifest_status_prev_status={1}, '\
                                            'curr_deid_qc_api_study_url={2} for Accession# \'{3}\''.format(
                                            CLASS_PRINTNAME,
                                            manifest_status_prev_status,
                                            curr_deid_qc_api_study_url,
                                            safe_acc_num_str),
                                            flush=True)

                                    # NOTE: WITH a previous manifest_status == FAIL:*
                                    # WARNING: this cfg setting could have just been enabled,
                                    # whereas the study previously set to FAIL while the cfg was disabled,
                                    # and the study not actually there on ORTHANCDEIC.
                                    # TODO: test if this will be caught by P5's download from ORTHANCDEIDQC
                                    # IF at FAIL AND study URL kept on ORTHANCDEIDQC:
                                    print('{0}.Process(): PHASE03: DEID QC STATUS advises to PASS the previous FAIL; '\
                                            'SETTING phase_process to DeIDed Phase {1} '
                                            'and will use interim DEID QC status '\
                                            'of \'{2}\' for Accession# \'{3}\''.format(
                                            CLASS_PRINTNAME,
                                            DEIDENTIFIED_PHASE,
                                            curr_deid_qc_status,
                                            safe_acc_num_str),
                                            flush=True)
                                    self.set_accession_phase_processed(safe_acc_num_str, DEIDENTIFIED_PHASE)
                                elif ((manifest_status_prev_status.find('FAIL:') == 0) \
                                    and ( \
                                            (self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC \
                                            or not curr_deid_qc_api_study_url
                                            ) \
                                        and (not self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES)
                                        )
                                    ):
                                    # NOTE: print regardless of VERBOSE:
                                    print('{0}.Process(): PHASE03: DEID QC found failed manifest_status_prev_status={1}, '\
                                            'curr_deid_qc_api_study_url={2} for Accession# \'{3}\', w/o interim files kept'.format(
                                            CLASS_PRINTNAME,
                                            manifest_status_prev_status,
                                            curr_deid_qc_api_study_url,
                                            self.remove_trailing_zeros(safe_acc_num_str)),
                                            flush=True)
                                    # FORCE to REPROCESS because we won't be able to retrieve the study from ORTHANCDEIDQC,
                                    # nor from any interim file which might have been kept:
                                    curr_deid_qc_status = 'REPROCESS:from_FAILED_via:{0}'.format(curr_deid_qc_status)

                                    # TODO: eventually implement the full reprocessing PASS:*
                                    # as part of https://github.research.chop.edu/dbhi/Locutus/issues/405 re: a subject_ID_preface
                                    # NOTE: Preparing for both (the eventual) PASS:* (to reprocess without a DEIDQC_subject_ID_preface),
                                    # and a PASS_FROM_DEIDQC:* (to pull directly from the ORTHANCDEIDQC, even if with such a preface)

                                    print('{0}.Process(): PHASE03: WARNING: supplied \'PASS_FROM_DEIDQC:*\' status for DEID QC STATUS has been '\
                                            'reset to \'{1}\' for Accession# \'{2}\' since no DEID QC interim files remain post-FAIL'.format(
                                            CLASS_PRINTNAME,
                                            curr_deid_qc_status,
                                            safe_acc_num_str),
                                            flush=True)
                                else:
                                    # SAFETY CATCH in case none of the above for some reason:
                                    # CONSIDER a softer failure, but perhaps to begin with, judo chop a fail with exception:
                                    print('{0}.Process(): PHASE03: DEID QC found failed deid_qc_status={1}, '\
                                            'manifest_status_prev_status={2}, curr_deid_qc_api_study_url={3} '\
                                            'for Accession# \'{4}\', w/: '\
                                            'LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC={5}, '\
                                            'LOCUTUS_KEEP_INTERIM_FILES={6}'.format(
                                            CLASS_PRINTNAME,
                                            deid_qc_status,
                                            manifest_status_prev_status,
                                            curr_deid_qc_api_study_url,
                                            safe_acc_num_str,
                                            self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC,
                                            self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES),
                                            flush=True)
                                    raise ValueError('{0}.Process(): PHASE03: DEID QC found failed deid_qc_status={1}, '\
                                            'manifest_status_prev_status={2}, curr_deid_qc_api_study_url={3} '\
                                            'for Accession# \'{4}\', w/: '\
                                            'LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC={5}, '\
                                            'LOCUTUS_KEEP_INTERIM_FILES={6}'.format(
                                            CLASS_PRINTNAME,
                                            deid_qc_status,
                                            manifest_status_prev_status,
                                            curr_deid_qc_api_study_url,
                                            safe_acc_num_str,
                                            self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC,
                                            self.locutus_settings.LOCUTUS_KEEP_INTERIM_FILES))

                            # ELIF === PASS:* / REPROCESS:*
                            # BUT, treating as a new "if", since the above "if" block might set the QC status to REPROCESS:*
                            if ((curr_deid_qc_status.find('REPROCESS:') == 0) \
                                or (curr_deid_qc_status.find('PASS:') == 0)):
                                update_valid_deid_qc_status = True
                                # print regardless of VERBOSE:
                                print('{0}.Process(): PHASE03: DEID QC STATUS of \'{1}\' advises to REPROCESS; '\
                                            'returning to phase_processed={2} for Accession# \'{3}\''.format(
                                            CLASS_PRINTNAME,
                                            curr_deid_qc_status,
                                            MIN_PROCESSING_PHASE,
                                            safe_acc_num_str),
                                            flush=True)
                                # WARNING: can't yet reset fully here, because the subsequent update_valid_deid_qc_status
                                # NO LONGER relies upon having change_sed_id=as_change_seq_id
                                # so delay a call to: self.reset_accession_phase_processed(safe_acc_num_str)
                                # (which will also reset deid_qc_status, and the respective study_url values, to NULL)
                                delay_reset_accession = True
                                delay_reset_accession_clear_deid_qc = True
                                if (curr_deid_qc_status.find('PASS:') == 0):
                                    # leave the deid_qc info (staus and urls) to allow its later removal if !keep_interim_files:
                                    delay_reset_accession_clear_deid_qc = False
                                    # and be sure to update the DeID QC status as well (even though already set, above)
                                    update_valid_deid_qc_status = True

                            # ELIF === FAIL:*
                            elif curr_deid_qc_status.find('FAIL:') == 0:
                                update_valid_deid_qc_status = True
                                # print regardless of VERBOSE:
                                print('{0}.Process(): PHASE03: DEID QC STATUS advises to FAIL; '\
                                            'setting to phase_processed={1} for Accession# \'{2}\''.format(
                                            CLASS_PRINTNAME,
                                            FAILED_PROCESSING_PHASE,
                                            safe_acc_num_str),
                                            flush=True)

                                self.set_accession_phase_processed(safe_acc_num_str, FAILED_PROCESSING_PHASE)

                                if not self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC:
                                    # print regardless of VERBOSE:
                                    print('{0}.Process(): PHASE03: WARNING: DEID QC study url '\
                                            '(\'{1}\') will remain on the DeiD QC instance of Orthanc '\
                                            'for Accession# \'{2}\' '
                                            'since not LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC'.format(
                                            CLASS_PRINTNAME,
                                            curr_deid_qc_api_study_url,
                                            safe_acc_num_str),
                                            flush=True)
                                elif self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC \
                                    and not curr_deid_qc_api_study_url:
                                    # no curr_deid_qc_api_study_url, give a WARNING but carry on:
                                    print('{0}.Process(): PHASE05: WARNING: empty FAILed DEID QC study url from '\
                                        'the STATUS table showing as (\'{1}\'); '\
                                        'no need to remove this interim DEID QC study for Accession# \'{2}\' '\
                                        'with curr_deid_qc_status=\'{3}\'.'.format(
                                        CLASS_PRINTNAME,
                                        curr_deid_qc_api_study_url,
                                        safe_acc_num_str,
                                        curr_deid_qc_status),
                                        flush=True)
                                else:
                                    # i.e., IF self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC:
                                    # AND and curr_deid_qc_api_study_url.
                                    # TODO: consider encapsulating this copy of the Phase05 OrthancDEIDQC DoDelete() code, if applicable

                                    # print regardless of VERBOSE:
                                    print('{0}.Process(): PHASE03: Removing study '\
                                            '(\'{1}\') from the DeiD QC instance of Orthanc '\
                                            'for Accession# \'{2}\' '
                                            'since LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC'.format(
                                            CLASS_PRINTNAME,
                                            curr_deid_qc_api_study_url,
                                            safe_acc_num_str),
                                            flush=True)

                                    # TODO: package the following up into a DeleteFromDeIDQC() method:
                                    nowtime = datetime.datetime.utcnow()
                                    print('@ {0} UTC: START of PHASE03 Accession# \'{1}\' DELETE from qc-Orthanc'.format(
                                                    nowtime,
                                                    safe_acc_num_str),
                                                    flush=True)

                                    # NOTE: re-authenticate via the RestToolbox whenever accessing EITHER Orthanc:
                                    if self.locutus_settings.LOCUTUS_VERBOSE:
                                        print('{0}.Process(): PHASE03: re-authenticating with qc-Orthanc server...'.format(CLASS_PRINTNAME), flush=True)
                                    RestToolbox.SetCredentials(self.qc_onprem_dicom_config['orthanc_user'],
                                                                self.qc_onprem_dicom_config['orthanc_password'])

                                    # NOTE: after having also cleared out the 2x api/explorer study uid fields
                                    # in the previous UPDATE to ONPREM_DICOM_STATUS, can now actually remove it from ORTHANCDEIDQC,
                                    # now actually delete the study URL from qc-Orthanc:
                                    ##################################################
                                    # TODO: wrap this in a try/catch to at least better present exceptions
                                    # such as "name 'staged_url' is not defined" in the following:
                                    ##################################################
                                    if self.locutus_settings.LOCUTUS_VERBOSE:
                                        print('{0}.Process(): PHASE03: DELETING from qc-Orthanc server... url = [{1}]'.format(
                                                                    CLASS_PRINTNAME, curr_deid_qc_api_study_url), flush=True)

                                    dicomdir_data = RestToolbox.DoDelete(curr_deid_qc_api_study_url)

                                    nowtime = datetime.datetime.utcnow()
                                    print('@ {0} UTC: END of PHASE03 Accession# \'{1}\' DELETE from qc-Orthanc'.format(
                                                    nowtime,
                                                    safe_acc_num_str),
                                                    flush=True)

                                    # AND, if packaging the above up into a DeleteFromDeIDQC() method,
                                    # consider also including a flag to optionally immediately reset
                                    # the deid_qc_study_*_url fields in ONPREM_DICOM_STATUS to NULL:
                                    self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                    'deid_qc_api_study_url=NULL, '\
                                                    'deid_qc_explorer_study_url=NULL '\
                                                    'WHERE accession_num=\'{1}\' '\
                                                    '    AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                    safe_acc_num_str))
                                    # and reset curr_deid_qc_api_study_url for subsequent MANIFEST_OUTPUT:
                                    curr_deid_qc_api_study_url = ''

                                # 'FAILED:*', use the same full value as provided by the input deid_qc_status:
                                failed_manifest_status = curr_deid_qc_status
                                self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                    'manifest_status=\'{1}\' '\
                                                    'WHERE accession_num=\'{2}\' '\
                                                    '    AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    failed_manifest_status,
                                                    safe_acc_num_str))

                            if not update_valid_deid_qc_status:
                                print('{0}.Process(): PHASE03: WARNING: unrecognized deid_qc_status of \'{1}\'; '\
                                        'expecting to {2} when locutus_onprem_dicom_use_manifest_QC_status is enabled'.format(
                                            CLASS_PRINTNAME,
                                            curr_deid_qc_status,
                                            UNDER_REVIEW_QC_OPTIONS), flush=True)

                                # NOTE: and pull this accession out from the list of accessions to process,
                                # such that it is not later picked up by a subsequent Phase Sweep:
                                manifest_accession_num_strs.remove(safe_acc_num_str)

                                # effectively just bail here, but first....
                                # a proper MANIFEST_OUTPUT manifest_status showing the error.
                                # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                                # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                                # and NOTE: using a ;-delimited list of targets within the following CSV,
                                # since multiple possible target platforms are supported in this module:
                                # NOTE: be sure to leave any empty MANIFEST_HEADER_MANIFEST_VER column at the end of the normally expected headers,
                                # and BEFORE additional output fields (manifest_status, etc):
                                ##########
                                # NOTE: For this mid-QA manual pause, no need to emit the {10} and {11} of:
                                #   ';'.join(deidentified_targets_new),
                                #   curr_fw_session_import_arg
                                # so they have been formatted with empty strings, to retain the same number of format args:
                                ##########
                                # NOTE: let curr_deid_qc_api_study_url remain as from the input maniest.
                                failed_manifest_status = 'ERROR_PHASE03=[unsupported deid_qc_status of {0}]'.format(curr_deid_qc_status)
                                print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                                            MANIFEST_OUTPUT_PREFIX,
                                            curr_subject_id,
                                            curr_object_info_01,
                                            curr_object_info_02,
                                            curr_object_info_03,
                                            safe_acc_num_str,
                                            curr_deid_qc_status,
                                            '',
                                            failed_manifest_status,
                                            curr_deid_qc_api_study_url,
                                            '',
                                            ''), flush=True)

                                # * and an update of the MANIFEST record to save its new failed_manifest_status:
                                self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                    'manifest_status=\'{1}\' '\
                                                    'WHERE accession_num=\'{2}\' '\
                                                    '    AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    failed_manifest_status,
                                                    safe_acc_num_str))

                                # CONSIDER a softer failure, but perhaps to begin with, just fail with such unexpected deid_qc_status:
                                print('{0}.Process(): PHASE03: ERROR: unrecognized deid_qc_status of \'{1}\' for '\
                                            'Accession# \'{2}\'; expecting deid_qc_status to {3} '\
                                            'when locutus_onprem_dicom_use_manifest_QC_status is enabled.'.format(
                                            CLASS_PRINTNAME,
                                            curr_deid_qc_status,
                                            safe_acc_num_str,
                                            UNDER_REVIEW_QC_OPTIONS),
                                            flush=True)
                                raise ValueError('{0}.Process(): PHASE03: ERROR: unrecognized deid_qc_status of \'{1}\' for '\
                                            'Accession# \'{2}\'; expecting deid_qc_status to {3}  '\
                                            'when locutus_onprem_dicom_use_manifest_QC_status is enabled'.format(
                                            CLASS_PRINTNAME,
                                            curr_deid_qc_status,
                                            safe_acc_num_str,
                                            UNDER_REVIEW_QC_OPTIONS))
                            else:
                                # i.e., IF update_valid_deid_qc_status:
                                # WARNING, though, if delay_reset_accession, any deid_qc_status set here will subsequently be reset to NULL
                                # NOTE: that can be okay, so long as the input manifest value, curr_deid_qc_status, is passed on to Phase03 & Phase04.
                                self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                    'datetime_processed=now(), '\
                                                    'deid_qc_status=\'{1}\' '\
                                                    'WHERE accession_num=\'{2}\' '\
                                                    '    AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                    curr_deid_qc_status,
                                                    safe_acc_num_str))

                            if delay_reset_accession:
                                self.reset_accession_phase_processed(safe_acc_num_str, clear_deid_qc=delay_reset_accession_clear_deid_qc)

                                # NOTE: as COPIED from end of Phase05,
                                # overall LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE update of manifest_status='PROCESSED'
                                # is here such that any changes which are not fully processed
                                # from Phase03 and are resumed mid-way will also be updated:
                                manifest_status = curr_deid_qc_status
                                self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                            'last_datetime_processed=now(), '\
                                                            'manifest_status=\'{1}\' '\
                                                            'WHERE accession_num=\'{2}\' '\
                                                            '    AND active ;'.format(
                                                            LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                            manifest_status,
                                                            safe_acc_num_str))

                    elif manifest_accnum_count > 1:
                        # NOTE: the GCP module does initially consider throwing an exception for this scenario, via:
                        #####
                        #new_exception_msg = 'ERROR_PHASE03: more than one '\
                        #                    'DICOM Manifest-based status row '\
                        #                    'found for Accession# \'{1}\''.format(
                        #                    CLASS_PRINTNAME,
                        #                    self.remove_trailing_zeros(curr_accession_num))
                        #####
                        print('{0}.Process(): PHASE03: ERROR: more than one DICOM Manifest-based '\
                                    'status rows found for Accession# \'{1}\''.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str),
                                    flush=True)
                        # 9/24/2024: NOTE: added these close() before the raise() (highlighting in case needing to alter):
                        ############################# ############################# #############################
                        # NOTE: CANCELed THESE close() + raise() sets within def Process() itself, UNLESS an actual FATAL exception (e.g., GCP_INVALID_CREDS_ERR).
                        # In general, we want Process() to carry on through to the completion of this manifest, if at all possible;
                        # even moreso when LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE is enabled.
                        # TODO: instead, ensure that the MANIFEST has been updated with the latest manifest_status.
                        # BUT NOTE: no close() here before the raise anyhow.
                        #####
                        # WAS: raise ValueError('Locutus {0}.Process() PHASE03 encountered errors in '\
                        #                    'DICOM Manifest-based status rows, more than one '\
                        #                    'found for Accession# \'{1}\''.format(
                        #                    CLASS_PRINTNAME,
                        #                    self.remove_trailing_zeros(safe_acc_num_str)))
                        #####
                        print('{0}.Process() r3m0 DEBUG INFO: such that Process can complete remainder of the manifest, '\
                            'continuing while loop and **NOT** throwing this PHASE03 ValueError exception: {1}.'.format(
                            CLASS_PRINTNAME,
                            new_exception_msg),
                            flush=True)
                        skip_to_next_accession = True
                        # NOTE: it **seems** that no more outstanding manifest_status updates are needed for this
                    elif manifest_accnum_count == 0:
                        # Add a new place-holder row for processing this manifest-based Accession#:
                        test_msg = ""
                        if self.locutus_settings.LOCUTUS_TEST:
                            test_msg = " TEST MODE: not actually"
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): PHASE03:{1} adding new DICOM Manifest-based '\
                                    'status row for Accession# \'{2}\''.format(
                                    CLASS_PRINTNAME,
                                    test_msg,
                                    safe_acc_num_str),
                                    flush=True)
                        if not self.locutus_settings.LOCUTUS_TEST:
                            self.LocutusDBconnSession.execute('INSERT INTO {0} ('\
                                                    'subject_id, '\
                                                    'object_info_01, '\
                                                    'object_info_02, '\
                                                    'object_info_03, '\
                                                    'active, '\
                                                    'accession_num ) '\
                                                    'VALUES(\'{1}\', \'{2}\', \'{3}\', \'{4}\', '\
                                                    'True, '\
                                                    '\'{5}\' );'.format(
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    curr_subject_id,
                                                    curr_object_info_01,
                                                    curr_object_info_02,
                                                    curr_object_info_03,
                                                    safe_acc_num_str))

                missing_fw_session_import_arg='n/a'
                if not skip_to_next_accession:
                    # Query the status table to find the uuid matching this Accession#, either that:
                    ##############
                    # WAS: (a) has NOT yet been processed at all (as_change_seq_id IS NULL AND phase_processed = MIN_PROCESSING_PHASE (2))
                    # WAS: OR that:
                    # WAS: (b) has only been partially processed (as_change_seq_id = change_seq_id AND phase_processed <= MAX_PROCESSING_PHASE)
                    ##############
                    # NOW: just picking the distinct uuids for this accession_num with active change_seq_id (>0)
                    ##############
                    # BUT....
                    # be aware that only fully unprocessed uuids should be passed on into Phase03 at this point;
                    # all others should be delayed until the applicable subsequent Phase0x processing steps picks it up.

                    # NOTE: WAS allowing ANY previously phase_processed phases in the below by comparing against <= MAX_PROCESSING_PHASE,
                    # as with the case for <= MAX_PROCESSING_PHASE will now emit the previous deidentified_targets(s) in subsequent code.
                    # de-coupled both the phase_processed and the deidentified_targets from this SELECT DISTINCT(uuid) to subsequent followup queries:

                    # NOTE: opened up to allow for a FAILED_PROCESSING_PHASE (which is > MAX_PROCESSING_PHASE):
                    staged_change_results = self.LocutusDBconnSession.execute('SELECT DISTINCT(uuid) '\
                                                        'FROM {0} '\
                                                        'WHERE accession_num=\'{1}\' '\
                                                        '    AND active '\
                                                        'AND phase_processed <= {2} '\
                                                        ';'.format(
                                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                        safe_acc_num_str,
                                                        FAILED_PROCESSING_PHASE))

                    # NOTE: rather than doing a separate SELECT COUNT() on the above,
                    # enumerate through the needed result to perform the count check:
                    numrows = 0
                    staged_change_row = None
                    for rownum, staged_change_row in enumerate(staged_change_results):
                        numrows += 1

                    # Check that the above result found either 0 or 1 (& only 1!) uuid:
                    manifest_status = None
                    whynot_manifest_status = None
                    missing_targetname='n/a'
                    missing_fw_session_import_arg='n/a'
                    post_manifest_update_exception = None
                    if numrows > 1:
                        print('{0}.Process(): PHASE03: ERROR: more than one changed '\
                                    'distinct uuids match Accession# \'{1}\'; setting error '\
                                    'status until resolved'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str),
                                    flush=True)
                        manifest_status = 'ERROR_MULTIPLE_CHANGE_UUIDS'
                        whynot_manifest_status = manifest_status
                        # NOTE: NO LONGER raise an exception for this
                        #   (even after the manifest_status has been updated)
                        #   since we may want to keep processing all others in the meantime:
                        # NOT: post_manifest_update_exception = 'Locutus {0}.Process(): PHASE03 '\
                        #            'found more than one changed '\
                        #            'distinct uuids matching Accession# \'{1}\''.format(
                        #            CLASS_PRINTNAME,
                        #            self.remove_trailing_zeros(safe_acc_num_str))
                        # BUT at least note an exception for the end of this batch.
                        total_errors_encountered += 1
                    elif numrows < 1:
                        print('{0}.Process(): PHASE03: Accession# \'{1}\' found no matching '\
                                    'distinct changed uuid to process '\
                                    '(which has not already been processed to max phase {2}).'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str,
                                    MAX_PROCESSING_PHASE),
                                    flush=True)

                        if manifest_accnum_count < 1:
                            # NOTE: this case (manifest_accnum_count < 1) is for no such manifest_status record
                            # WAS:  manifest_status = 'PENDING_CHANGE'
                            manifest_status = self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING
                            whynot_manifest_status = manifest_status
                            print('{0}.Process(): PHASE03: setting manifest_status=PENDING_CHANGE '\
                                    'for Accession# \'{1}\' and ignoring until next manifest-based '\
                                    'run with a matching changed uuid'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str),
                                    flush=True)
                        # WAS: elif manifest_status_prev_status == 'PENDING_CHANGE':
                        elif manifest_status_prev_status == self.locutus_settings.MANIFEST_OUTPUT_STATUS_PENDING \
                        or manifest_status_prev_status[:len(self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX)] == self.locutus_settings.ONDECK_PENDING_CHANGE_PREFIX:
                            # NOTE: THIS case is for one which was previously attempted and is still 'PENDING_CHANGE'
                            whynot_manifest_status = manifest_status_prev_status
                            print('{0}.Process(): PHASE03: ignoring Accession# \'{1}\' '\
                                    'until next manifest-based run with a staged matching changed uuid'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str),
                                    flush=True)
                        # WAS: elif manifest_status_prev_status == 'ERROR_MULTIPLE_CHANGE_UUIDS':
                        elif manifest_status_prev_status == self.locutus_settings.MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX \
                        or manifest_status_prev_status[:len(self.locutus_settings.ONDECK_MULTIPLES_CHANGE_PREFIX)] == self.locutus_settings.ONDECK_MULTIPLES_CHANGE_PREFIX:
                            # NOTE: THIS case is for one which was previously attempted but is still 'ERROR_MULTIPLE_CHANGE_UUIDS'
                            whynot_manifest_status = manifest_status_prev_status
                            print('{0}.Process(): PHASE03: ignoring Accession# \'{1}\' '\
                                    'until next manifest-based run with split-accessions (following cmd_dicom_split_accession) '\
                                    'for this accession'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str),
                                    flush=True)
                        else:
                            # DEV NOTE: -r3m0: 5/23/2025: unfortunately, we ARE ending up here again, currently having my question the....
                            #   REPROCESSING_CHANGE_MOMENTARILY="RE-PROCESSING_CHANGE_MOMENTARILY"
                            # ... that is being set for the newly reset manifest_status in def reset_accession_status_for_reprocessing()
                            # ESPECIALLY when this seems to be called on accessions which are PENDING_CHANGE and not even available to reprocess.
                            #
                            # NOTE: should REALLY no longer end up in this "else" block, NOT AT ALL! ;-)
                            # (would only be the case where a no-status table record exists,
                            #   AND that a previous manifest_status exists, but not of `PENDING_CHANGE`)
                            print('{0}.Process(): PHASE03: ignoring Accession# \'{1}\' '\
                                    'until next manifest-based run with a matching changed uuid'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str),
                                    flush=True)
                            whynot_manifest_status = 'ERROR_UNKNOWN_NONPENDING_MANIFEST_STATUS_SANS_STATUS_[{0}]'.format(manifest_status_prev_status)
                    else:
                        # Good: found expected number (1) of matching changed uuids:
                        if staged_change_row is not None:
                            curr_uuid = staged_change_row['uuid']
                            print('{0}.Process(): PHASE03: found changed distinct uuid {1} '\
                                                'matching Accession# \'{2}\''.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid,
                                                safe_acc_num_str),
                                                flush=True)

                            # Query the max phase_processed, de-coupled from the outer SELECT DISTINCT(uuid), phase_processed
                            # for the multiple phase_processed values that we will sometimes encounter through subsequent Orthanc change_seq_ids,
                            #####
                            # NOTE: ensuring that this is only pulling the info for ACTIVE uuids
                            # (i.e., non-retired, w/ POSITION(\'-\' IN accession_num) = 0 & change_seq_id > 0)
                            # lest previously (as above) retired versions of this trigger false multiples:
                            #
                            # NOTE: with Juneteenth 2025 alphanumeric upgrade comes the new active flag:
                            # AND with its testing came a manual test mistake of 2x acc_nums for 1x UUID,
                            # so, confirm just one active row for this UUID.
                            # NOTE: the previous SELECT DISTINCT(uuid) merely checked for MULTI-UUIDs by that accession_num,
                            # so now we look for multiple accessions from this UUID via the following COUNT(*) as num_rows:
                            # at the following SELECT MAX(phase_processed)....
                            curr_max_phase_results = self.LocutusDBconnSession.execute('SELECT '\
                                                'MAX(phase_processed) as max_phase_processed, '\
                                                'MIN(phase_processed) as min_phase_processed, '\
                                                'COUNT(*) as num_rows '\
                                                'FROM {0} '\
                                                'WHERE uuid=\'{1}\' '\
                                                '    AND active ;'.format(
                                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                curr_uuid))
                            curr_max_phase_row = curr_max_phase_results.fetchone()
                            curr_phase_processed = curr_max_phase_row['max_phase_processed']
                            curr_min_phase_processed = curr_max_phase_row['min_phase_processed']
                            # NOTE: no longer referencing as_change_seq_ids in previous  [See also: CHANGE-SEQUENCE CONSOLIDATION:] section
                            curr_uuid_num_rows = curr_max_phase_row['num_rows']

                            if curr_uuid_num_rows > 1:
                                print('r3m0 DEBUG ERROR: HOLD THE HORSES! '\
                                    'Found multiple ({0}) ROWS/ACCESSIONS listed in the STATUS table {1} for UUID=\'{2}\'! '\
                                    'This is DIFFERENT than an ERROR_MULTIPLE_CHANGE_UUIDS scenario, '\
                                    'and demands immediate attention to the DB, '\
                                    'if unable to robustly continue w/o this particular '\
                                    'accession=\'{3}\'.'.format(
                                        curr_uuid_num_rows,
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        curr_uuid,
                                        curr_uuid), flush=True)
                                # TODO: DEBUG: see if the following can help skip this accession (nope, not automagically, yet!)
                                manifest_status = 'ERROR_MULTIPLE_STATUS_ROWS_FOR_UUID' # NOTE: 'ERROR_MULTIPLE_CHANGE_UUIDS'
                                whynot_manifest_status = manifest_status
                                # AND, may just reset manifest_status to get caught at a subsequent conditional for:
                                #    if manifest_status is None and whynot_manifest_status is not None: [...]
                                manifest_status = None

                                # NOTE: NO LONGER raise an exception for this
                                #   (even after the manifest_status has been updated)
                                #   since we may want to keep processing all others in the meantime:
                                # NOT: post_manifest_update_exception = 'Locutus {0}.Process(): PHASE03 '\
                                #            'found more than one changed '\
                                #            'distinct uuids matching Accession# \'{1}\''.format(
                                #            CLASS_PRINTNAME,
                                #            self.remove_trailing_zeros(safe_acc_num_str))
                                # BUT at least note an exception for the end of this batch.
                                total_errors_encountered += 1
                                skip_to_next_accession = True
                                # TODO: further consider using the skip_to_next_accession flag in the code that follows,
                                # but for now, since this is such a manually manipulated anomoly anyhow (while testing scenarios),
                                # go ahead and just BAIL with a ValueError():
                                raise ValueError('OnPrem Found multiple ({0}) ROWS/ACCESSIONS listed '\
                                    'in the STATUS table {1} for UUID=\'{2}\'! '\
                                    'This is DIFFERENT than an ERROR_MULTIPLE_CHANGE_UUIDS scenario, '\
                                    'and demands immediate attention to the DB '\
                                    'for this particular accession=\'{3}\'.'.format(
                                        curr_uuid_num_rows,
                                        LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                        curr_uuid,
                                        curr_uuid))


                            ##########
                            # WARNING: the following code section is ^C+^V'd before each Phase03, Phase04 Sweep, & Phase05 Sweep, so...
                            # TODO: consolidate these into reusable functions.
                            print('{0}.Process(): PHASE03: examining Accession# \'{1}\' '\
                                    'with MIN/MAX of (phase_processed) = {2}/{3} '
                                    'to compare this manifest-based run with a previously processed matching changed uuid'.format(
                                    CLASS_PRINTNAME,
                                    safe_acc_num_str,
                                    curr_min_phase_processed,
                                    curr_phase_processed),
                                    flush=True)
                            same_manifest_attributes = False
                            preretired_prev_manifest_attributes = False

                            ########################################## ########################################## ##########################################
                            # NOTE: *** now CHECKING here at Phase03 for all (no more need to do so at Phase04 Sweep nor at Phase05 Sweep)***
                            #       (***corresponding to UPDATING at completion of Phase03, Phase04, or Phase05***)
                            #   ... since this THIRD PASS of the Internal Configuration Versioning Management
                            #       feature has expanded to support PARTIALLY-PROCESSED sessions,
                            #       as well as at_phase cfg dependencies, but is now all handled up front
                            #       here at Phase03
                            # Also check and compare against the internal configuration(s) used during processing:
                            ###########
                            # NOTE: now saving these internal config values with EACH update of the session's status
                            # upon EACH phase completion, such that they CAN be checked at Phase Sweeps for Phase04 & 5.
                            # This is to  help further prevent any inadvertant phase sweeps of sessions which had only been
                            # partially processed previously (e.g., up to or through Phase04 before encountering an error),
                            # but with different internal configuration(s).
                            ###########
                            # Phase03 CHECKING on behalf of subsequent Phase Sweeps as well,
                            # with curr_at_phase comparisons limited to the curr_phase_processed
                            # as well as check_prev=True:
                            ###########
                            retvals = self.compare_active_internal_configs_vs_processed(safe_acc_num_str,
                                                                                        curr_uuid,
                                                                                        curr_phase_processed,
                                                                                        LOCUTUS_ONPREM_DICOM_INT_CFGS_ACTIVES,
                                                                                        curr_at_phase=curr_phase_processed,
                                                                                        check_prev=True)


                            # above retvals expected as: (int_cfgs_all_match, active_int_cfgs, previously_processed_int_cfgs)
                            # NOTE: a later check of self.locutus_settings.LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED
                            # might set int_cfgs_match = True either way, in order to carry on, regardless.
                            int_cfgs_match = retvals[0]
                            int_cfgs_active = retvals[1]
                            int_cfgs_previous = retvals[2]
                            int_cfgs_max_match_phase = retvals[3]

                            # print the compare_active_internal_configs_vs_processed results regardless of VERBOSE
                            # (since its internals are much more verbose, and it might still be nice to just see the following):
                            print('{0}.Process(): PHASE03: NOTE: compare_active_internal_configs_vs_processed() '\
                                        'returned int_cfgs_match={1}, int_cfgs_active={2}, '\
                                        'int_cfgs_previous={3}, and  int_cfgs_max_match_phase={4}'.format(
                                        CLASS_PRINTNAME,
                                        int_cfgs_match,
                                        int_cfgs_active,
                                        int_cfgs_previous,
                                        int_cfgs_max_match_phase),
                                        flush=True)

                            # build up optional int_cfgs messages for PREVIOUS_PROCESSING_USED status messages when not int_cfgs_match:
                            int_cfgs_previous_msg = '' if int_cfgs_match else '/CFGs:{0}'.format(';'.join(int_cfgs_previous))
                            int_cfgs_active_msg = '' if int_cfgs_match else '/CFGs:{0}'.format(';'.join(int_cfgs_active))

                            # TODO: be sure to update for an object_info_04 if/when used from input manifest:
                            previous_manifest_result = self.LocutusDBconnSession.execute('SELECT subject_id, '\
                                                                'object_info_01, object_info_02, '\
                                                                'object_info_03, manifest_status FROM {0} '\
                                                                'WHERE accession_num=\'{1}\' '\
                                                                '    AND active ;'.format(
                                                                LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                                safe_acc_num_str))
                            previous_manifest_row = previous_manifest_result.fetchone()
                            prev_manifest_status = previous_manifest_row['manifest_status']
                            prev_subject_id = previous_manifest_row['subject_id']
                            prev_object_info_01 = previous_manifest_row['object_info_01']
                            prev_object_info_02 = previous_manifest_row['object_info_02']
                            prev_object_info_03 = previous_manifest_row['object_info_03']

                            # TODO: create a function for a single such comparison place of all attributes:
                            # TODO: be sure to update for an object_info_04 if/when used from input manifest:
                            if (previous_manifest_row is not None) \
                                and (curr_subject_id == prev_subject_id) \
                                and (curr_object_info_01 == prev_object_info_01) \
                                and (curr_object_info_02 == prev_object_info_02) \
                                and (curr_object_info_03 == prev_object_info_03):
                                    same_manifest_attributes = True

                            to_generate_whynot_manifest_status_for_previous = False

                            if (self.locutus_settings.LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED) \
                                and (same_manifest_attributes) and (not int_cfgs_match):
                                # NOTE: only allow the continued processing if same_manifest_attributes:
                                int_cfgs_match = True
                                print('{0}.Process(): PHASE03: WARNING: LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED enabled, '\
                                        'and manifest attributes are otherwise the same, '\
                                        'so ignoring the following changes from PREVIOUS cfgs ({1}), '\
                                        'as compared to the currently ACTIVE cfgs ({2}), and resuming processing anyhow.'.format(
                                        CLASS_PRINTNAME,
                                        int_cfgs_previous_msg,
                                        int_cfgs_active_msg),
                                        flush=True)
                            elif (self.locutus_settings.LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED) \
                                and (not same_manifest_attributes) and (not int_cfgs_match):
                                # NOTE: without the same_manifest_attributes, don't just continue processing;
                                # instead, will be handled via a below check of LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED
                                print('{0}.Process(): PHASE03: WARNING: LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED enabled, '\
                                        'but manifest attributes are NOT the same, '\
                                        'so UNABLE to continue processing this accession unless '\
                                        'LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED is also enabled '\
                                        'to all re-processing of the following changes from PREVIOUS cfgs ({1}), '\
                                        'as compared to the currently ACTIVE cfgs ({2}).'.format(
                                        CLASS_PRINTNAME,
                                        int_cfgs_previous_msg,
                                        int_cfgs_active_msg),
                                        flush=True)
                            elif (not self.locutus_settings.LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED) \
                                and (same_manifest_attributes) and (not int_cfgs_match):
                                # NOTE: with the same_manifest_attributes, and differing configs,
                                # but not LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED enabled,
                                # merely hint to the user of this setting's potential usage here:
                                print('{0}.Process(): PHASE03: HINT: '\
                                        'Since manifest attributes are otherwise the same, '\
                                        'please note that LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED is NOT enabled, '\
                                        'but COULD be enabled to ignore the following changes from PREVIOUS cfgs ({1}), '\
                                        'as compared to the currently ACTIVE cfgs ({2}), to resume processing anyhow.'.format(
                                        CLASS_PRINTNAME,
                                        int_cfgs_previous_msg,
                                        int_cfgs_active_msg),
                                        flush=True)


                            if same_manifest_attributes and int_cfgs_match:
                                whynot_manifest_status = 'PREVIOUSLY_PROCESSED_WITH_SAME_MANIFEST_ATTRIBUTES'
                            else:
                                # i.e.,: if (not same_manifest_attributes) or (not int_cfgs_match):
                                # NOTE: LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS would have already be handled before getting here.
                                #####
                                # TODO: Likewise, ensure that LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS is handled prior to here,
                                # such that we no longer encounter not same_manifest_attributes when FORCE_REPROCESS is set;
                                # For now, though, that is still under construction, and highlights issues here that can be improved....
                                #####
                                if self.locutus_settings.LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED:

                                    if (int_cfgs_max_match_phase == MIN_PROCESSING_PHASE) and (curr_phase_processed < MAX_PROCESSING_PHASE):
                                        print('{0}.Process(): PHASE03: NOTE: LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED; '\
                                                    'to roll back its STATUS to phase_processed={1} '\
                                                    'so aiming for a full pre-retirement of Accession# \'{2}\'... '.format(
                                                    CLASS_PRINTNAME,
                                                    int_cfgs_max_match_phase,
                                                    safe_acc_num_str),
                                                    flush=True)
                                        # merely set it to the max to fall into the next section,
                                        # as it will require a full retirement (including of its STATUS record)
                                        curr_phase_processed = MAX_PROCESSING_PHASE

                                    if curr_phase_processed == MAX_PROCESSING_PHASE:
                                        # print about the LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS regardless of VERBOSE:
                                        print('{0}.Process(): PHASE03: NOTE: LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED; '\
                                                    'retiring BOTH its previous manifest AND status in {1} for Accession# \'{2}\' '\
                                                    'to its negative of Accession# \'-{2}\''.format(
                                                    CLASS_PRINTNAME,
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    safe_acc_num_str),
                                                    flush=True)
                                        # general-purpose full preretirement for those accessions already previously processed fully:
                                        self.preretire_accession(safe_acc_num_str)
                                        # And since this previously PROCESSED is being retired for a brand new round of processing,
                                        # can leave off the step to INSERT a new blank manifest record.... but, may as well do for both
                                    else:
                                        # for a mid-process accession that does not need to go all the way
                                        # back to: int_cfgs_max_match_phase == MIN_PROCESSING_PHASE,
                                        # print about the LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS regardless of VERBOSE:
                                        print('{0}.Process(): PHASE03: NOTE: LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED; '\
                                                    'rolling back its STATUS to phase_processed={1} '\
                                                    'and retiring ONLY its previous MANIFEST for Accession# \'{2}\' '\
                                                    'to its negative of Accession# \'-{2}\''.format(
                                                    CLASS_PRINTNAME,
                                                    int_cfgs_max_match_phase,
                                                    safe_acc_num_str),
                                                    flush=True)
                                        # -vs- the preretirement specific to these mid-processed accessions, only retiring the manifest record:
                                        self.preretire_accession_manifest_only(safe_acc_num_str)
                                        # NOTE: the following are additional steps in this rolling back version w/ OnPrem
                                        # and, return its phase_processed to the max phase with a matched cfg:
                                        curr_phase_processed = int_cfgs_max_match_phase
                                        self.rollback_accession_status_phase_processed(safe_acc_num_str,
                                                                                    int_cfgs_max_match_phase)
                                    #####
                                    # Then, create a new blank manifest record with the current attributes:
                                    self.LocutusDBconnSession.execute('INSERT INTO {0} ('\
                                                    'subject_id, '\
                                                    'object_info_01, '\
                                                    'object_info_02, '\
                                                    'object_info_03, '\
                                                    'accession_num, '\
                                                    'active, '\
                                                    'deid_qc_status ) '\
                                                    'VALUES(\'{1}\', \'{2}\', \'{3}\', \'{4}\', '\
                                                    '\'{5}\', '\
                                                    'True. '\
                                                    '\'{6}\' );'.format(
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    curr_subject_id,
                                                    curr_object_info_01,
                                                    curr_object_info_02,
                                                    curr_object_info_03,
                                                    safe_acc_num_str,
                                                    curr_deid_qc_status))
                                    #####
                                    # then update same_manifest_attributes for continuation:
                                    preretired_prev_manifest_attributes = True
                                    same_manifest_attributes = True
                                    int_cfgs_match = True
                                    ###########################################################
                                    # NOTE:  PREVIOUS_PROCESSING_USED_* possibility 2 of 2 for Phase03....
                                    # This is the _PRERETIRED_SO_NOW_REPROCESSING_ version
                                    # (see also the similar _PRERETIRE_OR_PREDELETE_TO_REPROCESS_ version)
                                    ###########################################################
                                    to_generate_whynot_manifest_status_for_previous = True
                                    manifest_status = 'PROCESSING_CHANGE'
                                else:
                                    # (not same_manifest_attributes) OR (not int_cfgs_match)
                                    # AND (not LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS[_ONLY_CHANGED]):
                                    ###########################################################
                                    # NOTE:  PREVIOUS_PROCESSING_USED_* possibility 1 of 2 for Phase03....
                                    # This is the _PRERETIRE_OR_PREDELETE_TO_REPROCESS_ version
                                    # (see also the similar _PRERETIRED_SO_NOW_REPROCESSING_ version)
                                    ###########################################################
                                    to_generate_whynot_manifest_status_for_previous = True

                            # another PRE-LADDER, to align w/ same location as that for module_gcp_dicom.py:
                            print('r3m0 DEBUG: PRE-LADDER-01 vars before the following if/elif/else ladder: '\
                                        'same_manifest_attributes={0}, '\
                                        'curr_phase_processed={1}, '\
                                        'prev_phase_processed={2}, '\
                                        'MAX_PROCESSING_PHASE={3}, '\
                                        'LOCUTUS_DISABLE_PHASE_SWEEP={4}, '\
                                        'LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS={5}, '\
                                        'preretired_prev_manifest_attributes={6}, '\
                                        ' [...] '.format(
                                        same_manifest_attributes,
                                        curr_phase_processed,
                                        prev_phase_processed,
                                        MAX_PROCESSING_PHASE,
                                        self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP,
                                        self.locutus_settings.LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS,
                                        preretired_prev_manifest_attributes,
                                        ), flush=True)

                            if to_generate_whynot_manifest_status_for_previous:
                                whynot_manifest_status = self.generate_manifest_status_for_previous_processed_used(
                                                                prev_subject_id,
                                                                prev_object_info_01,
                                                                prev_object_info_02,
                                                                prev_object_info_03,
                                                                curr_subject_id,
                                                                curr_object_info_01,
                                                                curr_object_info_02,
                                                                curr_object_info_03,
                                                                int_cfgs_previous_msg,
                                                                int_cfgs_active_msg,
                                                                preretired_prev_manifest_attributes)

                                msg_processed = ''

                                if self.locutus_settings.LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS:
                                    msg_processed = 'WILL be FORCE_REPROCESS' # processed...
                                    print('{0}.Process(): PHASE03: LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS={1}; '\
                                            'Accession# \'{2}\' {3} processed, mid-status = \'{4}\''.format(
                                            CLASS_PRINTNAME,
                                            self.locutus_settings.LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS,
                                            safe_acc_num_str,
                                            msg_processed,
                                            whynot_manifest_status),
                                            flush=True)
                                else:
                                    if not preretired_prev_manifest_attributes:
                                        # the _PRERETIRE_OR_PREDELETE_TO_REPROCESS_ version
                                        # FROM: PREVIOUS_PROCESSING_USED_* possibility 1 of 2 for Phase03....
                                        msg_processed = 'has NOT been pre-retired and will NOT YET be'
                                        # therefore remove from the current list of manifest_accession_num_strs
                                        # such that it is not later picked up by a subsequent Phase Sweep:
                                        manifest_accession_num_strs.remove(safe_acc_num_str)
                                    else:
                                        # preretired_prev_manifest_attributes:
                                        # the _PRERETIRED_SO_NOW_REPROCESSING_ version
                                        # FROM: PREVIOUS_PROCESSING_USED_* possibility 2 of 2 for Phase03....
                                        msg_processed = 'HAS been pre-retired and WILL be'
                                    # combining the above via:
                                    print('{0}.Process(): PHASE03: LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED={1}; '\
                                            'Accession# \'{2}\' {3} processed, mid-status = \'{4}\''.format(
                                            CLASS_PRINTNAME,
                                            self.locutus_settings.LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED,
                                            safe_acc_num_str,
                                            msg_processed,
                                            whynot_manifest_status),
                                            flush=True)

                                print('r3m0 DEBUG: PRE-LADDER-02 vars before the following if/elif/else ladder: '\
                                        'same_manifest_attributes={0}, '\
                                        'curr_phase_processed={1}, '\
                                        'prev_phase_processed={2}, '\
                                        'MAX_PROCESSING_PHASE={3}, '\
                                        'LOCUTUS_DISABLE_PHASE_SWEEP={4}, '\
                                        'LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS={5}, '\
                                        'preretired_prev_manifest_attributes={6}, '\
                                        ' [...] '.format(
                                        same_manifest_attributes,
                                        curr_phase_processed,
                                        prev_phase_processed,
                                        MAX_PROCESSING_PHASE,
                                        self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP,
                                        self.locutus_settings.LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS,
                                        preretired_prev_manifest_attributes,
                                        ), flush=True)

                            if curr_phase_processed == prev_phase_processed \
                                and ((same_manifest_attributes and int_cfgs_match) or preretired_prev_manifest_attributes):
                                print('{0}.Process(): PHASE03: processing uuid {1} '\
                                                'from phase {2} now....'.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid,
                                                curr_phase_processed),
                                                flush=True)
                                manifest_status = 'PROCESSING_CHANGE'
                            elif curr_phase_processed == prev_phase_processed \
                                and not ((same_manifest_attributes and int_cfgs_match) or preretired_prev_manifest_attributes):
                                print('{0}.Process(): PHASE03: WARNING: processing of '\
                                                'uuid {1} WILL NOT currently resume at current phase {2} '\
                                                '(since LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS[_ONLY_CHANGED] is not configured); '\
                                                'will first need to be preretired due to changed manifest attributes.'.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid,
                                                curr_phase_processed),
                                                flush=True)
                                # NOTE: but leave the saved manifest_status as is.
                                # but do add the whynot_manifest_status from above IF there are changes in this,
                                # so that the status can reflect that not only will it not process now,
                                # BUT that it also needs to be preretired AND with phase_sweep, or something!
                                whynot_manifest_status = 'WARNING=[AWAITING LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED, '\
                                                            'AND.... {0}]'.format(
                                                            whynot_manifest_status)
                            elif curr_phase_processed == MAX_PROCESSING_PHASE:
                                # Q: why the subtle difference, with no actual manifest_status set here?
                                # NOTE: this is just for those which have not already had a manifest_status record created:
                                # TODO: compare against the last manifest values to determine if they were the same:

                                # Query the latest deid target, de-coupled from the outer SELECT DISTINCT(uuid), phase_processed:
                                curr_targets_results = self.LocutusDBconnSession.execute('SELECT DISTINCT(deidentified_targets) '\
                                                    'FROM {0} '\
                                                    'WHERE accession_num=\'{1}\' '\
                                                    '    AND active '\
                                                    'AND deidentified_targets IS NOT NULL '\
                                                    ';'.format(
                                                    LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                    safe_acc_num_str))
                                curr_targets_row = curr_targets_results.fetchone()
                                # NOTE: the above 'AND deidentified_targets IS NOT NULL' clause can result in a NULL return set
                                curr_targets = None
                                curr_target_most_recent = None
                                if curr_targets_row and len(curr_targets_row):
                                    curr_targets = curr_targets_row['deidentified_targets']
                                    if curr_targets:
                                        # strip out the LAST of the potentially comma-delimited previous target names:
                                        curr_target_most_recent = curr_targets[curr_targets.rfind(',')+1:]
                                else:
                                    # r3m0: TODO: enhance this quick exception... and follow it up!
                                    # quick little DEBUG with the MANIFEST_OUTPUT and a WHOA
                                    print("MANIFEST_OUTPUT:,# WHOA!!!!!  r3m0 say Dat Aint RIGHT!!!!!!  curr_targets_row is EMPTY, seemingly MISSING a deidentified_targets (continuing anyhow)", flush=True)
                                    #raise ValueError('UNEXPECTED ERROR: curr_targets_row is EMPTY, seemingly MISSING a deidentified_gs_target')
                                    # NOTE: looking MUCH better now.
                                    # r3m0: HERE: TODO:
                                    # NEXT: play with the ERROR further WITHOUT the above raise(),
                                    # to try to get the traceback to show.
                                    whynot_manifest_status = 'ERROR_PHASE03_Missing_PreviouslyProcessed_DeID_Target'

                                if (same_manifest_attributes and int_cfgs_match) \
                                    or (not self.locutus_settings.LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED):
                                    missing_targetname = curr_target_most_recent
                                    # TODO: create a function for the following fw_session_import_arg, since now used in multiple places:
                                    # NOTE: generate the helper column for downstream import into Flywheel via --session <age>d_<location>
                                    missing_fw_session_import_arg = '{0}d_{1}'.format(curr_object_info_02, curr_object_info_03)
                            elif curr_phase_processed == FAILED_PROCESSING_PHASE:
                                print('{0}.Process(): PHASE03: WARNING: processing of '\
                                                'uuid {1} WILL NOT resume beyond phase {2} '\
                                                '(since this indicates a FAILURE as indicated from the Manual QC step)'.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid,
                                                curr_phase_processed),
                                                flush=True)
                                # NOTE: but leave the saved manifest_status as is.
                                whynot_manifest_status = 'FAILED Manual DeID QC with: [{0}]'.format(
                                                curr_deid_qc_status.replace('FAIL:',''))
                            elif (same_manifest_attributes and int_cfgs_match) \
                                and (not self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP):
                                # mid-process accession with same attributes:
                                print('{0}.Process(): PHASE03: phase sweep will resume processing '\
                                                'of uuid {1} beyond phase {2} momentarily '\
                                                '(after all uuids at phase {3})....'.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid,
                                                curr_phase_processed,
                                                prev_phase_processed),
                                                flush=True)
                                manifest_status = 'PROCESSING_CHANGE_MOMENTARILY'
                                # NOTE: such uuids delayed for processing will NOT
                                # have their manifest_status set to 'PROCESSING'; instead,
                                # status will remain as 'PROCESSING_CHANGE_MOMENTARILY'
                                # until set as completely 'PROCESSED' by end of Phase05.
                                #################################
                                # And since this will be picked up in a subsequent Phase Sweep,
                                # save the current CSV-derived manifest-based attributes,
                                # for easier comparison then:
                                #
                                # ENSURE compatibility between this initial manifest-read string accession_num,
                                # and the later Decimal ones derived from the uuid status table at each phase sweep.
                                safe_acc_num_str = str(self.remove_trailing_zeros(curr_accession_num))
                                # check for possible duplicate accession numbers with different attributes within the same manifest!
                                if safe_acc_num_str in momentary_manifest_attrs:
                                    # TODO: throw an exception here for duplicates? OR just mark it as such?
                                    # As a first pass safety measure, throw exception, but....
                                    # TODO: really want to check for such duplicates NOT just for the PROCESSING_CHANGES_MOMENTARILY,
                                    # but for ALL!  Still, this is a good safety measure specific to these for the phase sweeps:
                                    raise ValueError('Locutus {0}.Process(): PHASE03 '\
                                                'encountered a duplicate PROCESSING_CHANGE_MOMENTARILY '\
                                                'for Accession# \'{1}\''.format(
                                                CLASS_PRINTNAME,
                                                safe_acc_num_str))
                                # and start adding this accession's attributes into the dictionary:
                                momentary_manifest_attrs[safe_acc_num_str] = {}
                                momentary_manifest_attrs[safe_acc_num_str]['subject_id'] = curr_subject_id
                                momentary_manifest_attrs[safe_acc_num_str]['object_info_01'] = curr_object_info_01
                                momentary_manifest_attrs[safe_acc_num_str]['object_info_02'] = curr_object_info_02
                                momentary_manifest_attrs[safe_acc_num_str]['object_info_03'] = curr_object_info_03
                                # TODO: be sure to update to curr_object_info_04 once utilized.
                                if (self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS):
                                    momentary_manifest_attrs[safe_acc_num_str][MANIFEST_HEADER_DEID_QC_STATUS] = curr_deid_qc_status
                                    momentary_manifest_attrs[safe_acc_num_str][MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL] = curr_deid_qc_api_study_url

                            elif (same_manifest_attributes and int_cfgs_match):
                                # and self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP:
                                print('{0}.Process(): PHASE03: WARNING: processing of '\
                                                'uuid {1} WILL NOT currently resume beyond phase {2} '\
                                                '(since LOCUTUS_DISABLE_PHASE_SWEEP is configured)'.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid,
                                                curr_phase_processed),
                                                flush=True)
                                # NOTE: but leave the saved manifest_status as is.
                                whynot_manifest_status = 'WARNING=[processing WILL NOT currently resume beyond phase {0} (since LOCUTUS_DISABLE_PHASE_SWEEP is configured)]'.format(
                                                curr_phase_processed)
                            else:
                                # not (same_manifest_attributes and int_cfgs_match):
                                # and self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP:
                                print('{0}.Process(): PHASE03: WARNING: processing of '\
                                                'uuid {1} WILL NOT currently resume beyond phase {2} '\
                                                '(since LOCUTUS_DISABLE_PHASE_SWEEP is configured, '\
                                                'BUT will first need to be preretired due to changed manifest attributes, anyhow)'.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid,
                                                curr_phase_processed),
                                                flush=True)
                                # NOTE: but leave the saved manifest_status as is.
                                # but do add the whynot_manifest_status from above IF there are changes in this,
                                # so that the status can reflect that not only will it not process now,
                                # BUT that it also needs to be preretired AND with phase_sweep, or something!
                                whynot_manifest_status = 'WARNING=[AWAITING LOCUTUS_DISABLE_PHASE_SWEEP, AND.... {0}]'.format(
                                                whynot_manifest_status)
                        else:
                            raise ValueError('Locutus {0}.Process(): PHASE03 '\
                                                'unable to read staged change uuid row from {1} '\
                                                'for Accession# \'{2}\''.format(
                                                CLASS_PRINTNAME,
                                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                safe_acc_num_str))

            if manifest_status is None and whynot_manifest_status is not None:
                # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                #
                # ONLY for those accession_nums without even having a manifest_status defined,
                # but having a whynot_manifest_status:
                # NOTE: be sure to leave any empty MANIFEST_HEADER_MANIFEST_VER column at the end of the normally expected headers,
                # and BEFORE additional output fields (manifest_status, etc):
                ##########
                # NOTE: accession number either printing safely, w/o any text (if successful),
                # otherwise, printed into MANIFEST_OUT with the text via manifest_accession_str if in a related error:
                print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                            MANIFEST_OUTPUT_PREFIX,
                            curr_subject_id,
                            curr_object_info_01,
                            curr_object_info_02,
                            curr_object_info_03,
                            safe_acc_num_str if not whynot_manifest_status else manifest_accession_str,
                            curr_deid_qc_status,
                            '',
                            whynot_manifest_status,
                            curr_deid_qc_api_study_url,
                            missing_targetname,
                            missing_fw_session_import_arg), flush=True)
            if manifest_status is not None:
                # IF any manifest_status to update at all, do it:
                test_msg = ""
                if self.locutus_settings.LOCUTUS_TEST:
                    test_msg = " TEST MODE: not actually"
                if self.locutus_settings.LOCUTUS_VERBOSE:
                    print('{0}.Process(): PHASE03:{1} updating manifest status '\
                                            'row for Accession# \'{2}\' to '\
                                            'manifest_status=\'{3}\''.format(
                                            CLASS_PRINTNAME,
                                            test_msg,
                                            safe_acc_num_str,
                                            manifest_status),
                                            flush=True)
                if not self.locutus_settings.LOCUTUS_TEST:
                    self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                            'last_datetime_processed=now(), '\
                                            'manifest_status=\'{1}\' '\
                                            'WHERE accession_num=\'{2}\' '\
                                            '    AND active ;'.format(
                                            LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                            manifest_status,
                                            safe_acc_num_str))

                if post_manifest_update_exception:
                    raise ValueError(post_manifest_update_exception)

                if manifest_status != 'PROCESSING_CHANGE':
                    # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                    # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                    #
                    # ONLY for those accession_nums having just had their status updated as above, (even as PROCESSING_CHANGE_MOMENTARILY, etc.)
                    # but not actually about to process, further below, since they will achieve their own PROCESSED or ERROR status update.
                    missing_targetname='n/a'
                    missing_fw_session_import_arg='n/a'
                    # NOTE: be sure to leave any empty MANIFEST_HEADER_MANIFEST_VER column at the end of the normally expected headers,
                    # and BEFORE additional output fields (manifest_status, etc):
                    print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                                MANIFEST_OUTPUT_PREFIX,
                                curr_subject_id,
                                curr_object_info_01,
                                curr_object_info_02,
                                curr_object_info_03,
                                safe_acc_num_str,
                                curr_deid_qc_status,
                                '',
                                manifest_status,
                                curr_deid_qc_api_study_url,
                                missing_targetname,
                                missing_fw_session_import_arg), flush=True)

                caught_exception = None
                # only continue on with the actual Phase03 processing if applicable:
                if manifest_status == 'PROCESSING_CHANGE' \
                    and not self.locutus_settings.LOCUTUS_TEST:
                    # TODO: else if LOCUTUS_TEST also add a print message when NOT processing a 'PROCESSING_CHANGE'
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.Process(): PHASE03: about to process uuid: \'{1}\''.format(
                                CLASS_PRINTNAME,
                                curr_uuid),
                                flush=True)
                    curr_uuid_errs = 0
                    err_desc = ''
                    try:
                        (curr_uuid_errs, curr_uuid_errs_msg, curr_deid_qc_api_study_url) = self.Process_Phase03_DownloadFromOrthanc(
                                                            prev_phase_processed,
                                                            curr_uuid,
                                                            curr_subject_id,
                                                            curr_object_info_01,
                                                            curr_object_info_02,
                                                            curr_object_info_03,
                                                            safe_acc_num_str,
                                                            curr_deid_qc_status,
                                                            processRemainingPhases)
                        # NOTE: the update of LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
                        # to manifest_status = 'PROCESSED' is done at the end of
                        # Process_Phase05...() to support anything not completed here.
                        # UNLESS an error is returned or caught:
                        if curr_uuid_errs > 0:
                            err_desc = curr_uuid_errs_msg
                            print('{0}.Process(): PHASE03: ERROR: encountered {1} error(s) '\
                                            'while processing Phase03 for uuid \'{2}\'.'.format(
                                            CLASS_PRINTNAME,
                                            curr_uuid_errs,
                                            curr_uuid),
                                            flush=True)
                        elif processRemainingPhases:
                            # impled successfully PROCESSED, if processRemainingPhases through to the final Phase05
                            total_accessions_processed_successfully += 1
                    except Exception as locally_caught_exception:
                        print(locally_caught_exception, flush=True)
                        safe_locally_caught_exception_str = str(locally_caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                        caught_exception = '{0}'.format(safe_locally_caught_exception_str)
                        print('{0}.Process(): PHASE03: ERROR: Caught Exception \'{1}\' '\
                                        'while processing Phase03 for \'{2}\'.'.format(
                                        CLASS_PRINTNAME,
                                        caught_exception,
                                        curr_uuid),
                                        flush=True)
                        #######################
                        # r3m0: WARNING: SQL_UPDATE_QUOTE_TO_REPLACE replacement happens TWICE here
                        # TODO = Q: is that as expected/needed? If not, then tidy this up!
                        # NOTE: this is in both GCP and OnPrem modules (since OnPrem ^C/^V'd from GCP during its Level-Up)
                        #######################
                        # at least save some bits from the last exception into status:
                        safe_exception_str = str(caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                        err_desc = '{0}'.format(safe_exception_str)
                        # TODO: handle more of the exception, but for now, just
                        # save and re-raise it after updating manifest_status:
                        curr_uuid_errs += 1

                    if curr_uuid_errs > 0:
                        total_errors_encountered += curr_uuid_errs

                        # remove from the current list of manifest_accession_num_strs
                        # such that it is not later picked up by a subsequent Phase Sweep:
                        manifest_accession_num_strs.remove(safe_acc_num_str)

                        # NOTE: this is just the first such Phase entry point
                        # which updates a manifest_status=ERROR, as it is the
                        # only place which already has the accession_num loaded.
                        # TODO: set this up around other Process_Phase0x*() calls,
                        # and will need to determine the accession_num for these
                        # other phases where it isn't otherwise immediately available.
                        # TODO: further logging of actual error/exception, etc:

                        # NOTE: only prepend current ERROR_PHASE if not already prefixed w/ ERROR_PHASE:
                        manifest_status = err_desc
                        if (len(err_desc) < len(self.locutus_settings.ERR_PHASE_PREFIX)) or (err_desc[:len(self.locutus_settings.ERR_PHASE_PREFIX)] != self.locutus_settings.ERR_PHASE_PREFIX):
                            # NOTE: manifest_status MUST start with ERR_PHASE_PREFIX = 'ERROR_PHASE' for this to work.
                            # only prepend ERROR_PHASE03viaProcess if a phase isn't already noted:
                            manifest_status = 'ERROR_PHASE03viaProcess=[{0}]'.format(err_desc)

                        self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                    'last_datetime_processed=now(), '\
                                                    'manifest_status=\'{1}\' '\
                                                    'WHERE accession_num=\'{2}\' '\
                                                    '    AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    manifest_status.replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT),
                                                    safe_acc_num_str))

                        # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                        # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                        #
                        # ONLY for those accession_nums having just had their status updated as above,
                        # but not actually about to process, further below, since they will achieve their own PROCESSED or ERROR status update.
                        missing_targetname='n/a'
                        missing_fw_session_import_arg='n/a'
                        # NOTE: be sure to leave any empty MANIFEST_HEADER_MANIFEST_VER column at the end of the normally expected headers,
                        # and BEFORE additional output fields (manifest_status, etc):
                        print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                                MANIFEST_OUTPUT_PREFIX,
                                curr_subject_id,
                                curr_object_info_01,
                                curr_object_info_02,
                                curr_object_info_03,
                                safe_acc_num_str,
                                curr_deid_qc_status,
                                '',
                                manifest_status,
                                curr_deid_qc_api_study_url,
                                missing_targetname,
                                missing_fw_session_import_arg), flush=True)
                        # WAS: if caught_exception:
                        if err_desc:
                            # NOTE: should already be so prefaced now that it's looking at err_desc.
                            # STILL: check for ERROR preface before mindlessly adding another:
                            # WARNING: this has also been taken care of above in the manifest_status; may want to switch to that.
                            new_exception_msg = '{0}'.format(err_desc)
                            if (len(err_desc) < len(self.locutus_settings.ERR_PHASE_PREFIX)) or (err_desc[:len(self.locutus_settings.ERR_PHASE_PREFIX)] != self.locutus_settings.ERR_PHASE_PREFIX):
                                # NOTE: manifest_status MUST start with ERR_PHASE_PREFIX = 'ERROR_PHASE' for this to work.
                                # only prepend ERROR_PHASE03viaProcess if a phase isn't already noted:
                                new_exception_msg = 'ERROR_PHASE03viaProcess=[{0}]'.format(err_desc)
                            #####
                            print('{0}.Process(): PHASE03viaProcess wrapping and re-throwing the caught exception: {1}.'.format(
                                CLASS_PRINTNAME,
                                new_exception_msg),
                                flush=True)
                            ############################# ############################# #############################
                            # NOTE: CANCELed THESE close() + raise() sets within def Process() itself, UNLESS an actual FATAL exception (e.g., GCP_INVALID_CREDS_ERR).
                            # In general, we want Process() to carry on through to the completion of this manifest, if at all possible;
                            # even moreso when LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE is enabled.
                            # TODO: instead, ensure that the MANIFEST has been updated with the latest manifest_status.
                            #####
                            #self.manifest_infile.close()
                            #self.LocutusDBconnSession.close()
                            #self.StagerDBconnSession.close()
                            #raise ValueError(new_exception_msg)
                            #####
                            # NOTE: will automatically fall down into the next try read manifest line, and so on
                            print('{0}.Process(): PHASE03viaProcess: such that Process can complete remainder of the manifest, '\
                                    '**NOT** re-throwing a caught NON-FATAL exception: {1}.'.format(
                                CLASS_PRINTNAME,
                                new_exception_msg),
                                flush=True)
                            #############################
                            traceback_clues = traceback.format_exc()
                            print("PHASE03viaProcess ERROR DEBUG: suppressed traceback MAY look like: {0}".format(
                                        traceback_clues), flush=True)
                            #############################
                            # NOTE: it **seems** that no more outstanding manifest_status updates are needed for this
                            ############################# ############################# #############################

            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            ######################################################################
            # BEFORE the next manifest accession is processed, check Locutus DB..... System Status section:
            check_Docker_node=True
            alt_node_name=None
            check_module=False
            module_name=SYS_STAT_MODULENAME
            ####
            (curr_sys_stat_node, sys_msg_node, sys_node_name) = self.locutus_settings.get_Locutus_system_status(self.locutus_settings, check_Docker_node, alt_node_name, check_module, module_name, self.LocutusDBconnSession)
            print('{0},{1},node={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "gcp_GET_LOCUTUS_SYS_STATUS", sys_node_name, curr_sys_stat_node, sys_msg_node), flush=True)
            ####
            check_Docker_node=False
            check_module=True
            (curr_sys_stat_module, sys_msg_module, sys_module_name) = self.locutus_settings.get_Locutus_system_status(self.locutus_settings, check_Docker_node, alt_node_name, check_module, module_name, self.LocutusDBconnSession)
            print('{0},{1},module={2},{3},{4}'.format(src_modules.settings.CFG_OUT_PREFIX, "gcp_GET_LOCUTUS_SYS_STATUS", sys_module_name, curr_sys_stat_module, sys_msg_module), flush=True)
            ####
            # only need to evaluate curr_sys_stat_node & curr_sys_stat_module, since it takes into account curr_sys_stat_overall during its calculation
            if (curr_sys_stat_node and curr_sys_stat_module):
                print('r3m0 DEBUG: OnPrem sees Locutus System Status is active for this node & module (and overall).... carry on!')
            else:
                run_loop = False
                print('FATAL ERROR: OnPrem sees Locutus System Status is NOT currently active for this '\
                    'module (status={0}) or node (status={1}); '\
                    'setting manifest_done to end the current manifest processing loop '\
                    '(and activating LOCUTUS_DISABLE_PHASE_SWEEP).'.format(
                        curr_sys_stat_module, curr_sys_stat_node), flush=True)
                # bail w/ this_run_has_fatal_errors:
                #this_run_has_fatal_errors = True
                # howzabout the less ominous manifest_done?
                manifest_done = True
                # Further, ensure that phase sweeps don't get picked up:
                self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP = True
            ###########
            if not manifest_done:
                try:
                    # partB == tail-end manifest reading within and at end of the while not manifest_done loop
                    curr_manifest_row = next(self.manifest_reader)
                    while len(curr_manifest_row) == 0 or \
                        (not any(field.strip() for field in curr_manifest_row)) or \
                        curr_manifest_row[0] == '' or \
                        curr_manifest_row[0][0] == '#':
                        # read across any blank manifest lines (or lines with only blank fields, OR lines starting with a comment, '#'):
                        print('{0}.Process(): partB ignoring input manifest body line of "{1}"'.format(CLASS_PRINTNAME, curr_manifest_row), flush=True)
                        if MANIFEST_OUTPUT_INCLUDE_COMMENT_LINES:
                            # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                            # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                            #if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0},{1}'.format(
                                        MANIFEST_OUTPUT_PREFIX,
                                        ','.join(curr_manifest_row)),
                                        flush=True)
                        curr_manifest_row = next(self.manifest_reader)
                    print('{0}.Process(): partB tail of while not manifest_done parsing input manifest body line of "{1}"'.format(CLASS_PRINTNAME, curr_manifest_row), flush=True)
                except StopIteration as e:
                    manifest_done = True

        if processed_uuids_in_this_phase:
            print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
            nowtime = datetime.datetime.utcnow()
            print('@ {0} UTC: END of PHASE03 batch'.format(nowtime), flush=True)
            print('{0}.Process(): END of PHASE03: phase processing complete for this batch.'.format(CLASS_PRINTNAME), flush=True)

        if not self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP:
            # PHASE_SWEEP: Processing Phase04:
            ###########################################################
            # == Deidentify unzip'd DICOM Dir, the next of many processed phases.
            #
            # NOTE: although the Orthanc patient media includes a top-level
            #   DICOMDIR file, this is an unnecessary Table of Contents of sorts,
            #   and poses problems for the deidentification tool, dicom_anon.
            #   Only deidentify the IMAGES folder within.
            #
            # Now, whether or not any new changes actually processed
            # in the above Phase03, go ahead and process any still at Last Phase=3.
            # NOTE: assuming that uuids are now distinct, as multi-change uuids have
            # been previously squashed in the Phase03 migration to download DICOMDir
            prev_phase_processed = 3
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process(): PHASE_SWEEP for PHASE04: DE-ID, processing LOCUTUS_ONPREM_DICOM_STATUS_TABLE '\
                                                'changes from Phase {1}:'.format(
                                                CLASS_PRINTNAME,
                                                prev_phase_processed),
                                                flush=True)
            result = self.LocutusDBconnSession.execute('SELECT DISTINCT(uuid), '\
                                                'accession_num, '\
                                                'identified_local_path, change_seq_id '\
                                                ' FROM {0} '\
                                                'WHERE phase_processed={1} '\
                                                '    AND active ;'.format(
                                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                prev_phase_processed))

            processed_uuids_in_this_phase = False
            processRemainingPhases = True
            for row in result:
                if not processed_uuids_in_this_phase:
                    print('{0}.Process(): ============================================================='.format(CLASS_PRINTNAME), flush=True)
                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                    print('{0}.Process(): START of PHASE_SWEEP for PHASE04: phase processing this batch...'.format(CLASS_PRINTNAME), flush=True)
                    nowtime = datetime.datetime.utcnow()
                    print('@ {0} UTC: START of PHASE_SWEEP 4 batch'.format(nowtime), flush=True)
                    processed_uuids_in_this_phase = True
                    print('{0},### PHASE_SWEEP 4,####,#######,LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST={1} with additional sweeps to follow in no particular order:'.format(
                            MANIFEST_OUTPUT_PREFIX,
                            self.locutus_settings.LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST), flush=True)

                curr_uuid = row['uuid']
                curr_accession_num = row['accession_num']
                curr_uuid_id_hostpath = row['identified_local_path']
                curr_uuid_change_seq_id = row['change_seq_id']

                safe_acc_num_str = str(self.remove_trailing_zeros(curr_accession_num))
                if ((not self.locutus_settings.LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST) \
                        and (not safe_acc_num_str in manifest_accession_num_strs)):
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.Process(): PHASE_SWEEP for PHASE04: LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST disabled, '\
                            'so ignoring phase sweep of accession_num: {1}'.format(
                                CLASS_PRINTNAME,
                                self.remove_trailing_zeros(curr_accession_num)),
                                flush=True)
                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                elif self.locutus_settings.LOCUTUS_TEST:
                    print('{0}.Process(): PHASE_SWEEP for PHASE04: TEST MODE: not actually processing uuid: \'{1}\''.format(
                            CLASS_PRINTNAME,
                            curr_uuid),
                            flush=True)
                else:
                    # PHASE_SWEEP for PHASE04: about to process Accession, IF matching any previously existing manifest attributes:
                    # NOTE: any preretiring due to LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED would have already occurred.
                    # Now only need to compare the current and previous manifest attributes:
                    ##########################################
                    # TODO: first check that curr_accession_num exists in the momentary_manifest_attrs dictionary:
                    curr_subject_id = None
                    curr_object_info_01 = None
                    curr_object_info_02 = None
                    curr_object_info_03 = None
                    curr_deid_qc_status = None
                    # ENSURE compatibility between this initial manifest-read string accession_num,
                    # and the later Decimal ones derived from the uuid status table at each phase sweep.
                    safe_acc_num_str = str(self.remove_trailing_zeros(curr_accession_num))
                    if safe_acc_num_str in momentary_manifest_attrs:
                        curr_subject_id = momentary_manifest_attrs[safe_acc_num_str]['subject_id']
                        curr_object_info_01 = momentary_manifest_attrs[safe_acc_num_str]['object_info_01']
                        curr_object_info_02 = momentary_manifest_attrs[safe_acc_num_str]['object_info_02']
                        curr_object_info_03 = momentary_manifest_attrs[safe_acc_num_str]['object_info_03']
                        curr_deid_qc_status = ''
                        curr_deid_qc_api_study_url = ''
                        if (self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS):
                            curr_deid_qc_status = momentary_manifest_attrs[safe_acc_num_str][MANIFEST_HEADER_DEID_QC_STATUS]
                            curr_deid_qc_api_study_url = momentary_manifest_attrs[safe_acc_num_str][MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL]
                    # TODO: be sure to update to curr_object_info_04 once utilized.

                    curr_uuid_errs = 0
                    err_desc = ''

                    if safe_acc_num_str not in momentary_manifest_attrs:
                        # This accession was NOT flagged for PROCESSING_CHANGE_MOMENTARILY,
                        # and likely experienced an error in its initial processing through Phase03, et al,
                        # arriving here again through a phase sweep.
                        # NOT: don't bother doubling up curr_uuid_errs += 1
                        err_desc = 'ignoring unanticipated sweep of incomplete Accession# \'{0}\' until next round of processing'.format(curr_accession_num)
                        print('{0}.Process(): ERROR: PHASE_SWEEP for PHASE04: {1}'.format(
                                CLASS_PRINTNAME,
                                err_desc),
                                flush=True)
                    else:
                        # Read to PHASE_SWEEP 5 with the same_manifest_attributes:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): PHASE_SWEEP for PHASE04: about to process Accession# \'{1}\' with uuid: \'{2}\''.format(
                                    CLASS_PRINTNAME,
                                    curr_accession_num,
                                    curr_uuid),
                                    flush=True)
                        curr_uuid_errs = 0
                        caught_exception = None
                        err_desc = ''
                        print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                        try:
                            (curr_uuid_errs, curr_uuid_errs_msg, curr_deid_qc_api_study_url) = self.Process_Phase04_DeidentifyAndZip(prev_phase_processed,
                                                                                curr_uuid_change_seq_id,
                                                                                curr_uuid,
                                                                                curr_accession_num,
                                                                                curr_uuid_id_hostpath,
                                                                                curr_deid_qc_status,
                                                                                processRemainingPhases)
                            # NOTE: the update of LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
                            # to manifest_status = 'PROCESSED' is done at the end of
                            # Process_Phase05...() to support anything not completed here.
                            # UNLESS an error is returned or caught:
                            if curr_uuid_errs > 0:
                                err_desc = curr_uuid_errs_msg
                                print('{0}.Process(): PHASE_SWEEP for PHASE04: ERROR: encountered {1} error(s) ({2}) '\
                                                'while deidentifying uuid \'{3}\''.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid_errs,
                                                err_desc,
                                                curr_uuid),
                                                flush=True)
                            elif processRemainingPhases:
                                # impled successfully PROCESSED, if processRemainingPhases through to the final Phase05
                                total_accessions_processed_successfully += 1
                        except Exception as locally_caught_exception:
                            safe_locally_caught_exception_str = str(locally_caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                            caught_exception = '{0}'.format(safe_locally_caught_exception_str)
                            print('{0}.Process(): PHASE_SWEEP for PHASE04: ERROR: Caught Exception \'{1}\' '\
                                                'while deidentifying uuid \'{2}\''.format(
                                                CLASS_PRINTNAME,
                                                caught_exception,
                                                curr_uuid),
                                                flush=True)
                            #######################
                            # r3m0: WARNING: SQL_UPDATE_QUOTE_TO_REPLACE replacement happens TWICE here
                            # TODO = Q: is that as expected/needed? If not, then tidy this up!
                            # NOTE: this is in both GCP and OnPrem modules (since OnPrem ^C/^V'd from GCP during its Level-Up)
                            #######################
                            # at least save some bits from the last exception into status:
                            safe_exception_str = str(caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                            err_desc = '{0}'.format(safe_exception_str)
                            # save and re-raise it after updating manifest_status
                            curr_uuid_errs += 1

                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)

                    if curr_uuid_errs > 0:
                        total_errors_encountered += curr_uuid_errs

                        # remove from the current list of manifest_accession_num_strs
                        # such that it is not later picked up by a subsequent Phase Sweep:
                        manifest_accession_num_strs.remove(safe_acc_num_str)

                        # TODO: further logging of actual error/exception, etc:
                        # WAS: manifest_status = 'ERROR_SWEEP_PHASE04=[{0}]'.format(err_desc)
                        manifest_status = '{0}'.format(err_desc)
                        if (len(err_desc) < len(self.locutus_settings.ERR_PHASE_PREFIX)) or (err_desc[:len(self.locutus_settings.ERR_PHASE_PREFIX)] != self.locutus_settings.ERR_PHASE_PREFIX):
                            # NOTE: manifest_status MUST start with ERR_PHASE_PREFIX = 'ERROR_PHASE' for this to work.
                            # only prepend ERROR_PHASE04_SWEEP if a phase isn't already noted:
                            manifest_status = 'ERROR_PHASE04_SWEEP=[{0}]'.format(err_desc)

                        self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                    'last_datetime_processed=now(), '\
                                                    'manifest_status=\'{1}\' '\
                                                    'WHERE accession_num=\'{2}\' '\
                                                    '   AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    manifest_status.replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT),
                                                    curr_accession_num))
                        # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                        # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                        #
                        # ONLY for those accession_nums having just had their status updated as above.
                        missing_targetname='n/a'
                        missing_fw_session_import_arg='n/a'
                        # NOTE: be sure to leave any empty MANIFEST_HEADER_MANIFEST_VER column at the end of the normally expected headers,
                        # and BEFORE additional output fields (manifest_status, etc):
                        print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                                MANIFEST_OUTPUT_PREFIX,
                                curr_subject_id,
                                curr_object_info_01,
                                curr_object_info_02,
                                curr_object_info_03,
                                self.remove_trailing_zeros(curr_accession_num),
                                curr_deid_qc_status,
                                '',
                                manifest_status,
                                curr_deid_qc_api_study_url,
                                missing_targetname,
                                missing_fw_session_import_arg), flush=True)
                        # WAS: if caught_exception:
                        if err_desc:
                            # NOTE: should already be so prefaced now that it's looking at err_desc.
                            # STILL: check for ERROR preface before mindlessly adding another:
                            # WARNING: this has also been taken care of above in the manifest_status; may want to switch to that.
                            new_exception_msg = '{0}'.format(err_desc)
                            if (len(err_desc) < len(self.locutus_settings.ERR_PHASE_PREFIX)) or (err_desc[:len(self.locutus_settings.ERR_PHASE_PREFIX)] != self.locutus_settings.ERR_PHASE_PREFIX):
                                # NOTE: manifest_status MUST start with ERR_PHASE_PREFIX = 'ERROR_PHASE' for this to work.
                                # only prepend ERROR_PHASE04_SWEEP if a phase isn't already noted:
                                new_exception_msg = 'ERROR_PHASE04_SWEEP=[{0}]'.format(err_desc)
                            #####
                            print('{0}.Process(): PHASE04_SWEEP wrapping and re-throwing the caught exception: {1}.'.format(
                                CLASS_PRINTNAME,
                                new_exception_msg),
                                flush=True)
                            ############################# ############################# #############################
                            # NOTE: CANCELed THESE close() + raise() sets within def Process() itself, UNLESS an actual FATAL exception (e.g., GCP_INVALID_CREDS_ERR).
                            # In general, we want Process() to carry on through to the completion of this manifest, if at all possible;
                            # even moreso when LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE is enabled.
                            # TODO: instead, ensure that the MANIFEST has been updated with the latest manifest_status.
                            #####
                            #self.manifest_infile.close()
                            #self.LocutusDBconnSession.close()
                            #self.StagerDBconnSession.close()
                            #raise ValueError(new_exception_msg)
                            #####
                            # NOTE: will automatically fall down into the next try read manifest line, and so on
                            print('{0}.Process(): PHASE04_SWEEP: such that Process can complete remainder of the manifest, '\
                                    '**NOT** re-throwing a caught NON-FATAL exception: {1}.'.format(
                                CLASS_PRINTNAME,
                                new_exception_msg),
                                flush=True)
                            #############################
                            traceback_clues = traceback.format_exc()
                            print("ERROR_PHASE04_SWEEP DEBUG: suppressed traceback MAY look like: {0}".format(
                                        traceback_clues), flush=True)
                            #############################
                            # NOTE: it **seems** that no more outstanding manifest_status updates are needed for this
                            ############################# ############################# #############################
                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)

            if processed_uuids_in_this_phase:
                print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                print('{0}.Process(): END of PHASE_SWEEP for PHASE04: phase processing complete for this batch.'.format(CLASS_PRINTNAME), flush=True)
                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: END of PHASE_SWEEP 4 batch'.format(nowtime), flush=True)
                print('{0}.Process(): ============================================================='.format(CLASS_PRINTNAME), flush=True)

            # PHASE_SWEEP: Processing Phase05:
            ###########################################################
            # == Transfer the Deidentified DICOM Dir, the last of many phases.
            #
            # Again, whether or not any new changes actually processed
            # in the above Phase04, go ahead and process any still at Last Phase=4.
            # NOTE: assuming that uuids are now distinct, as multi-change uuids have
            # been previously squashed in the Phase03 migration to download DICOMDir
            prev_phase_processed = 4
            if self.locutus_settings.LOCUTUS_VERBOSE:
                print('{0}.Process():PHASE_SWEEP for PHASE05: XFER->S3, processing LOCUTUS_ONPREM_DICOM_STATUS_TABLE '\
                                                'changes from Phase {1}:'.format(
                                                CLASS_PRINTNAME,
                                                prev_phase_processed),
                                                flush=True)
            result = self.LocutusDBconnSession.execute('SELECT DISTINCT(uuid), '\
                                                'accession_num, '\
                                                'deidentified_local_path, change_seq_id '\
                                                'FROM {0} '\
                                                'WHERE phase_processed={1} '\
                                                '    AND active ;'.format(
                                                LOCUTUS_ONPREM_DICOM_STATUS_TABLE,
                                                prev_phase_processed))

            processed_uuids_in_this_phase = False
            processRemainingPhases = True
            for row in result:
                if not processed_uuids_in_this_phase:
                    print('{0}.Process(): ============================================================='.format(CLASS_PRINTNAME), flush=True)
                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                    print('{0}.Process(): START of PHASE_SWEEP for PHASE05: phase processing this batch...'.format(CLASS_PRINTNAME), flush=True)
                    nowtime = datetime.datetime.utcnow()
                    print('@ {0} UTC: START of PHASE_SWEEP 5 batch'.format(nowtime), flush=True)
                    processed_uuids_in_this_phase = True
                    print('{0},### PHASE_SWEEP 5,####,#######,LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST={1} with additional sweeps to follow in no particular order:'.format(
                            MANIFEST_OUTPUT_PREFIX,
                            self.locutus_settings.LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST), flush=True)

                curr_uuid = row['uuid']
                curr_accession_num = row['accession_num']
                curr_uuid_deid_hostpath = row['deidentified_local_path']
                curr_uuid_change_seq_id = row['change_seq_id']

                safe_acc_num_str = str(self.remove_trailing_zeros(curr_accession_num))
                if ((not self.locutus_settings.LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST) \
                        and (not safe_acc_num_str in manifest_accession_num_strs)):
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.Process(): PHASE_SWEEP for PHASE05: LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST disabled, '\
                            'so ignoring phase sweep of accession_num: {1}'.format(
                                CLASS_PRINTNAME,
                                self.remove_trailing_zeros(curr_accession_num)),
                                flush=True)
                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                elif self.locutus_settings.LOCUTUS_TEST:
                    print('{0}.Process(): PHASE_SWEEP for PHASE05: TEST MODE: not actually '\
                            'processing uuid: \'{1}\''.format(
                            CLASS_PRINTNAME,
                            curr_uuid),
                            flush=True)
                elif self.locutus_settings.num_targets_configured <= 0:
                    if self.locutus_settings.LOCUTUS_VERBOSE:
                        print('{0}.Process(): halting before PHASE_SWEEP for PHASE05 since no targets configured.'.format(
                                CLASS_PRINTNAME),
                                flush=True)
                else:
                    # PHASE_SWEEP for PHASE05: about to process Accession, IF matching any previously existing manifest attributes:
                    # NOTE: any preretiring due to LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED would have already occurred.
                    # Now only need to compare the current and previous manifest attributes:
                    ##########################################
                    # TODO: first check that curr_accession_num exists in the momentary_manifest_attrs dictionary:
                    curr_subject_id = None
                    curr_object_info_01 = None
                    curr_object_info_02 = None
                    curr_object_info_03 = None
                    curr_deid_qc_status = None
                    # ENSURE compatibility between this initial manifest-read string accession_num,
                    # and the later Decimal ones derived from the uuid status table at each phase sweep.
                    safe_acc_num_str = str(self.remove_trailing_zeros(curr_accession_num))
                    if safe_acc_num_str in momentary_manifest_attrs:
                        curr_subject_id = momentary_manifest_attrs[safe_acc_num_str]['subject_id']
                        curr_object_info_01 = momentary_manifest_attrs[safe_acc_num_str]['object_info_01']
                        curr_object_info_02 = momentary_manifest_attrs[safe_acc_num_str]['object_info_02']
                        curr_object_info_03 = momentary_manifest_attrs[safe_acc_num_str]['object_info_03']
                        curr_deid_qc_status = ''
                        curr_deid_qc_api_study_url = ''
                        if (self.locutus_settings.LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS):
                            curr_deid_qc_status = momentary_manifest_attrs[safe_acc_num_str][MANIFEST_HEADER_DEID_QC_STATUS]
                            curr_deid_qc_api_study_url = momentary_manifest_attrs[safe_acc_num_str][MANIFEST_HEADER_OUTPUT_DEID_QC_STUDY_URL]
                    # TODO: be sure to update to curr_object_info_04 once utilized.

                    curr_uuid_errs = 0
                    err_desc = ''
                    if safe_acc_num_str not in momentary_manifest_attrs:
                        # This accession was NOT flagged for PROCESSING_CHANGE_MOMENTARILY,
                        # and likely experienced an error in its initial processing through Phase03, et al,
                        # arriving here again through a phase sweep.
                        # NOT: don't bother doubling up curr_uuid_errs += 1
                        err_desc = 'ignoring unanticipated sweep of incomplete Accession# \'{0}\' until next round of processing'.format(curr_accession_num)
                        print('{0}.Process(): ERROR: PHASE_SWEEP for PHASE05: ignoring unanticipated sweep of incomplete Accession# \'{1}\' until next round of processing'.format(
                                CLASS_PRINTNAME,
                                curr_accession_num),
                                flush=True)
                    else:
                        # Read to PHASE_SWEEP 5 with the same_manifest_attributes:
                        if self.locutus_settings.LOCUTUS_VERBOSE:
                            print('{0}.Process(): PHASE_SWEEP for PHASE05: about to process Accession# \'{1}\' with uuid: \'{2}\' with De-ID hostpath of: \'{3}\''.format(
                                    CLASS_PRINTNAME,
                                    curr_accession_num,
                                    curr_uuid,
                                    curr_uuid_deid_hostpath),
                                    flush=True)
                        caught_exception = None
                        print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                        try:
                            (curr_uuid_errs, curr_uuid_errs_msg, curr_deid_qc_api_study_url) = self.Process_Phase05_UploadToTargets(
                                                        prev_phase_processed,
                                                        curr_uuid_change_seq_id,
                                                        curr_uuid,
                                                        curr_accession_num,
                                                        curr_uuid_deid_hostpath,
                                                        processRemainingPhases)
                            # NOTE: the update of LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE
                            # to manifest_status = 'PROCESSED' is done at the end of
                            # Process_Phase05...() to support anything not completed here.
                            # UNLESS an error is returned or caught:
                            if curr_uuid_errs > 0:
                                err_desc = curr_uuid_errs_msg
                                print('{0}.Process(): PHASE_SWEEP for PHASE05: ERROR: encountered {1} error(s) ({2}) '\
                                                'while uploading uuid \'{3}\' to s3'.format(
                                                CLASS_PRINTNAME,
                                                curr_uuid_errs,
                                                err_desc,
                                                curr_uuid),
                                                flush=True)
                            else:
                                # impled successfully PROCESSED, if processRemainingPhases through to the final Phase05
                                total_accessions_processed_successfully += 1
                        except Exception as locally_caught_exception:
                            safe_locally_caught_exception_str = str(locally_caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                            caught_exception = '{0}'.format(safe_locally_caught_exception_str)
                            print('{0}.Process(): PHASE_SWEEP for PHASE05: ERROR: Caught Exception \'{1}\' '\
                                                'while uploading uuid \'{2}\' to s3.'.format(
                                                CLASS_PRINTNAME,
                                                caught_exception,
                                                curr_uuid),
                                                flush=True)
                            #######################
                            # r3m0: WARNING: SQL_UPDATE_QUOTE_TO_REPLACE replacement happens TWICE here
                            # TODO = Q: is that as expected/needed? If not, then tidy this up!
                            # NOTE: this is in both GCP and OnPrem modules (since OnPrem ^C/^V'd from GCP during its Level-Up)
                            #######################
                            # at least save some bits from the last exception into status:
                            safe_exception_str = str(caught_exception).replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT)
                            err_desc = '{0}'.format(safe_exception_str)
                            # save and re-raise it after updating manifest_status
                            curr_uuid_errs += 1

                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)

                    if curr_uuid_errs > 0:
                        total_errors_encountered += curr_uuid_errs

                        # remove from the current list of manifest_accession_num_strs
                        # such that it is not later picked up by a subsequent Phase Sweep (even if none beyond, YET....  LOL!):
                        manifest_accession_num_strs.remove(safe_acc_num_str)

                        # TODO: further logging of actual error/exception, etc:
                        # WAS: manifest_status = 'ERROR_SWEEP_PHASE05=[{0}]'.format(err_desc)
                        manifest_status = '{0}'.format(err_desc)
                        if (len(err_desc) < len(self.locutus_settings.ERR_PHASE_PREFIX)) or (err_desc[:len(self.locutus_settings.ERR_PHASE_PREFIX)] != self.locutus_settings.ERR_PHASE_PREFIX):
                            # NOTE: manifest_status MUST start with ERR_PHASE_PREFIX = 'ERROR_PHASE' for this to work.
                            # only prepend ERROR_PHASE05_SWEEP if a phase isn't already noted:
                            manifest_status = 'ERROR_PHASE05_SWEEP=[{0}]'.format(err_desc)

                        self.LocutusDBconnSession.execute('UPDATE {0} SET '\
                                                    'last_datetime_processed=now(), '\
                                                    'manifest_status=\'{1}\' '\
                                                    'WHERE accession_num=\'{2}\' '\
                                                    '   AND active ;'.format(
                                                    LOCUTUS_ONPREM_DICOM_MANIFEST_TABLE,
                                                    manifest_status.replace(self.locutus_settings.SQL_UPDATE_QUOTE_TO_REPLACE, self.locutus_settings.SQL_UPDATE_QUOTE_REPLACEMENT),
                                                    curr_accession_num))
                        # NOTE: quick stdout log crumb to easily create an output CSV via: grep MANIFEST_OUTPUT
                        # (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
                        #
                        # ONLY for those accession_nums having just had their status updated as above.
                        missing_targetname='n/a'
                        missing_fw_session_import_arg='n/a'
                        # NOTE: be sure to leave any empty MANIFEST_HEADER_MANIFEST_VER column at the end of the normally expected headers,
                        # and BEFORE additional output fields (manifest_status, etc):
                        print('{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}'.format(
                                MANIFEST_OUTPUT_PREFIX,
                                curr_subject_id,
                                curr_object_info_01,
                                curr_object_info_02,
                                curr_object_info_03,
                                self.remove_trailing_zeros(curr_accession_num),
                                curr_deid_qc_status,
                                '',
                                manifest_status,
                                curr_deid_qc_api_study_url,
                                missing_targetname,
                                missing_fw_session_import_arg), flush=True)
                        # WAS: if caught_exception:
                        if err_desc:
                            # NOTE: should already be so prefaced now that it's looking at err_desc.
                            # STILL: check for ERROR preface before mindlessly adding another:
                            # WARNING: this has also been taken care of above in the manifest_status; may want to switch to that.
                            new_exception_msg = '{0}'.format(err_desc)
                            if (len(err_desc) < len(self.locutus_settings.ERR_PHASE_PREFIX)) or (err_desc[:len(self.locutus_settings.ERR_PHASE_PREFIX)] != self.locutus_settings.ERR_PHASE_PREFIX):
                                # NOTE: manifest_status MUST start with ERR_PHASE_PREFIX = 'ERROR_PHASE' for this to work.
                                # only prepend ERROR_PHASE05_SWEEP if a phase isn't already noted:
                                new_exception_msg = 'ERROR_PHASE05_SWEEP=[{0}]'.format(err_desc)
                            #####
                            print('{0}.Process(): PHASE05_SWEEP wrapping and re-throwing the caught exception: {1}.'.format(
                                CLASS_PRINTNAME,
                                new_exception_msg),
                                flush=True)
                            #####
                            # 9/24/2024: NOTE: added these close() before the raise() (highlighting in case needing to alter):
                            ############################# ############################# #############################
                            # NOTE: CANCELed THESE close() + raise() sets within def Process() itself, UNLESS an actual FATAL exception (e.g., GCP_INVALID_CREDS_ERR).
                            # In general, we want Process() to carry on through to the completion of this manifest, if at all possible;
                            # even moreso when LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE is enabled.
                            # TODO: instead, ensure that the MANIFEST has been updated with the latest manifest_status.
                            #####
                            #self.manifest_infile.close()
                            #self.LocutusDBconnSession.close()                            # and another, after the fact:
                            #self.StagerDBconnSession.close()
                            #raise ValueError(new_exception_msg)
                            #####
                            # NOTE: will automatically fall down into the next try read manifest line, and so on
                            print('{0}.Process(): PHASE05_SWEEP: such that Process can complete remainder of the manifest, '\
                                    '**NOT** re-throwing a caught NON-FATAL exception: {1}.'.format(
                                CLASS_PRINTNAME,
                                new_exception_msg),
                                flush=True)
                            #############################
                            traceback_clues = traceback.format_exc()
                            print("ERROR_PHASE05_SWEEP DEBUG: suppressed traceback MAY look like: {0}".format(
                                        traceback_clues), flush=True)
                            #############################
                            # NOTE: it **seems** that no more outstanding manifest_status updates are needed for this
                            ############################# ############################# #############################

                    print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)

            if processed_uuids_in_this_phase:
                print('{0}.Process(): -------------------------------------'.format(CLASS_PRINTNAME), flush=True)
                print('{0}.Process(): END of PHASE_SWEEP for PHASE05: phase processing complete for this batch.'.format(CLASS_PRINTNAME), flush=True)
                nowtime = datetime.datetime.utcnow()
                print('@ {0} UTC: END of PHASE_SWEEP 5 batch'.format(nowtime), flush=True)
                print('{0}.Process(): ============================================================='.format(CLASS_PRINTNAME), flush=True)

        else:
            # i.e., if self.locutus_settings.LOCUTUS_DISABLE_PHASE_SWEEP,
            # print out message regardless of LOCUTUS_VERBOSE:
            print('{0}.Process(): WARNING: LOCUTUS_DISABLE_PHASE_SWEEP set for concurrent processing, '\
                                                'did not sweep through stragglers needing '\
                                                'PHASE04 or PHASE05, only Phases 1-3.'.format(
                                                CLASS_PRINTNAME),
                                                flush=True)

        # r3m0: HERE: ====> TODO: add a Summary count of total errors encountered AND total processed, regardless....
        if total_errors_encountered:
            # TODO: eventually enhance with propagating specific error message(s)
            raise ValueError('Locutus {0}.Process() encountered a total of ({1}) error(s) (see run log for specific details)'.format(
                            CLASS_PRINTNAME,
                            total_errors_encountered))

        self.manifest_infile.close()

        if self.locutus_settings.LOCUTUS_VERBOSE:
            print('{0}.Process() says Goodbye'.format(CLASS_PRINTNAME), flush=True)
        # WAS: return total_errors_encountered
        # r3m0: UPDATING for convergence:
        return (this_fatal_errors, total_errors_encountered, total_accessions_processed_successfully)
        # end of Process()
