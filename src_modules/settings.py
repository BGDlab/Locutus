#!/usr/local/bin/python

import yaml
import os

# for get_Locutus_system_status(): socket.gethostname() to determine local hostname for status:
import socket
# create_engine() for temporary get_Locutus_system_status connection, if/where needed:
from sqlalchemy import create_engine
# for nowtime of system status messages:
import datetime

APP_NAME = 'Locutus'
# NOTE: just found more to do with: APP_VERSION = '2025.06.13at0717=enhance_deploy_scripts_to_use_cmdline_args_for_manifest_and_suffix'
# BUT FIRST....
#APP_VERSION = '2025.07.03at0751_enhance_accession_num_2B_alpha-numeric'
# AND THEN:
APP_VERSION = '2025.08.28at1017_enhance_Migrator_to_better_handle_already_upgrade_alphanumerics'
#WAS: APP_VERSION = '2025.08.27at0838_reUPGRADE_to_enhance_alphanumeric_accession_nums_ESP_with_MANIFEST_table'
#WAS: MID: APP_VERSION = '2025.08.25at1754_deprecate_accession-num-src_from_old_multiUUID_SPLITS_to_enhance_alphanumeric_accession_nums'
# BEWARE of Code Smells all throughout Locutus; so sorry about that, fellow developers.
# Locutus is most definitely long overdue for a refactoring to clean up some of those smells.
# Please see, for example: https://refactoring.guru/refactoring/smells

# LINT NOTE: to manually find any potential print continuation lines without a space at the end from bash,
# please refer to the following example:
# $ cat -n src_modules/module_onprem_dicom.py | grep "\'\\\\" | grep -v format | grep -v " \'\\\\"

# NOTE: NOT moving CFG_OUT_* #defines into the Settings class itself to increase accessibility,
# but leaving here for now, encouraging cmds such as the Summmarizer to also
#   # NOTE: for the general settings in Locutus's settings.py:
#   import src_modules.settings
# in addition to using the self.locutus_settings object
#####
# TODO: consider someday moving it on into the Settings class itself,
# but also beware of its use in these more static methods up top, such as:
#   get_config_val()
##################################################################
#
# NOTE: quick stdout log crumb to easily create a CFG output CSV via: grep CFG_OUT
# (eventually TODO: use an actual CSV writer to stdout, but for now, a quick hard-code)
# NOTE: as a quick pass solution, this will merely print out such a CFG_OUT whenever a setting is read,
# and MIGHT therefore emit a few such values (e.g., at initial setting, at config read, and at os.environ override)
# where only the final one will have superseded all others. As such, an enhanced approach could aim to do so after the fact.
# Furthermore, it would be well worth creating a helper method to couple both the reading of the var and the CFG_OUT printing.
# But for now, here is the first pass at it....
CFG_OUT_PREFIX = "CFG_OUT:"
CFG_OUT_HEADER_KEY_TYPE = 'KEY_TYPE'  # e.g., cfg, env, or hardcoded, etc.
CFG_OUT_HEADER_KEY = 'KEY'
CFG_OUT_HEADER_VAL = 'VALUE'

# constants to pass in as the first parameter to get_env_or_config_val(type_bool,...)
TYPE_BOOL = True
TYPE_NON_BOOL = False

CLASS_PRINTNAME = 'Settings'

def get_config_val(config, key_name, def_val='', print_cfg_out=True, verbose=True):
    # helper to get a value from a configuration key (w/ the config typically being a config.yaml as stored in Vault)
    # NOTE: unlike the other such get_*_val() helpers which may instead or also read from the environment,
    # this reads from a yaml file and can automatically interpret a boolean value without the need
    # for a correspondingly dedicated get_*_val_bool() helper.
    # NOTE: wrapping quotes around strings that have commas, so that the resulting CSV maintains the grouping.
    # TODO: considering adding this to the other below methods of getting such vals (but there aren't yet any needed)
    key_val = def_val
    try:
        if config[key_name] is not None:
            key_val = config[key_name]
    except Exception:
        # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val()
        #if verbose:
        #    print('{0}: VERBOSE: Locutus.Settings.get_config_val(): unable to read configuration for setting: \'{1}\'; '\
        #            'using default value=\'{2}\''.format(
        #            CLASS_PRINTNAME, key_name, key_val), flush=True)
        # NOTE: just leaving the current else structure, even if now otherwise empty w/o the verbose
        throwaway_var=True
    if print_cfg_out:
        printable_key_val = key_val
        if isinstance(key_val, str) and key_val.find(',') >= 0:
            # if commas within, then wrap in quotes:
            printable_key_val = '"{0}"'.format(key_val)
        print('{0},cfg,{1},{2}'.format(CFG_OUT_PREFIX, key_name, printable_key_val), flush=True)
    return key_val


def get_env_val(key_name, def_val='', print_cfg_out=True, verbose=True):
    # helper to get a (typically string) value from an environment variable, key_name,
    # as usually used for an optional override of a config value
    # (though also used for stand-alone environment variables)
    key_val = def_val
    # NOTE: be aware that the the following os.environ.get() is case sensitive:
    if os.environ.get(key_name) is not None:
        key_val = os.environ.get(key_name)
    else:
        # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val()
        #if verbose:
        #    print('{0}: VERBOSE: Locutus.Settings.get_env_val(): no environment variable to override setting: \'{1}\'; '\
        #            'using default value=\'{2}\''.format(
        #            CLASS_PRINTNAME, key_name, key_val), flush=True)
        # NOTE: just leaving the current else structure, even if now otherwise empty w/o the verbose
        throwaway_var=True
    if print_cfg_out:
        print('{0},env,{1},{2}'.format(CFG_OUT_PREFIX, key_name, key_val), flush=True)
    return key_val


def get_env_val_bool(key_name, def_val=False, print_cfg_out=True, verbose=True):
    # helper to get and validate a boolean value from an environment variable (key_name)
    # noting that such boolean values injected into the environment by Jenkins, for example,
    # require a further comparison against the string "true" to actually become a boolean True
    key_val = def_val
    env_override_val = get_env_val(key_name, '', print_cfg_out=print_cfg_out, verbose=verbose)
    if env_override_val is not None:
        if isinstance(env_override_val, str) and env_override_val.lower() == "true":
            key_val = True
            # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val()
            #if verbose:
            #    print('{0}: VERBOSE: Locutus.Settings.get_env_val_bool(): true boolean environment variable overriding '\
            #            'boolean setting: \'{1}\'; setting to value=\'{2}\''.format(
            #            CLASS_PRINTNAME, key_name, key_val), flush=True)
        elif isinstance(env_override_val, str) and env_override_val.lower() == "false":
            key_val = False
            # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val()
            #if verbose:
            #    print('{0}: VERBOSE: Locutus.Settings.get_env_val_bool(): false boolean environment variable overriding '\
            #            'boolean setting: \'{1}\'; setting to value=\'{2}\''.format(
            #            CLASS_PRINTNAME, key_name, key_val), flush=True)
        elif isinstance(env_override_val, bool):
            key_val = env_override_val
            # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val()
            #if verbose:
            #    print('{0}: VERBOSE: Locutus.Settings.get_env_val_bool(): boolean environment variable overriding '\
            #            'boolean setting: \'{1}\'; setting to value=\'{2}\''.format(
            #            CLASS_PRINTNAME, key_name, key_val), flush=True)
        else:
            # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val()
            #if verbose:
            #    print('{0}: VERBOSE: Locutus.Settings.get_env_val_bool(): non-bool-ish value (\'{1}\') of environment variable '\
            #            '\'{2}\' attempted to override boolean setting; using default value=\'{3}\''.format(
            #            CLASS_PRINTNAME, env_override_val, key_name, key_val), flush=True)
            # NOTE: just leaving the current else structure, even if now otherwise empty w/o the verbose
            throwaway_var=True
    else:
        # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val()
        #if verbose:
        #    print('{0}: VERBOSE: Locutus.Settings.get_env_val_bool(): no environment variable to override setting \'{1}\'; '\
        #            'using default value=\'{2}\''.format(
        #            CLASS_PRINTNAME, key_name, key_val), flush=True)
        # NOTE: just leaving the current else structure, even if now otherwise empty w/o the verbose
        throwaway_var02=True
    if print_cfg_out:
        print('{0},env-bool,{1},{2}'.format(CFG_OUT_PREFIX, key_name, key_val), flush=True)
    return key_val


def get_env_or_config_val(type_bool, env_key_name, config, cfg_key_name, def_val=False, print_cfg_out=True, verbose=True):
    # helper to get and validate a (potentially boolean, or otherwise) value from an environment variable (key_name)
    # noting that such boolean values, in particular, when injected into the environment by Jenkins, for example,
    # require a further comparison against the string "true" to actually become a boolean True.
    #
    # TODO: once all env_key_names are harmonized with all cfg_key_names, can turn that back to a single key_name parameter
    # but for now, please do NOTE: that some of the longer configuration names were shortened
    # when exposing env var ovverrides to Jenkins.
    #   NOTE: it appears that any such naming discrepencies are specific to the manifest variables
    #   (e.g., "onprem_dicom_images_manifest.csv" in Jenkins gets passed as env var "onprem_dicom_images_manifest"
    #   but config uses"locutus_onprem_dicom_input_manifest_csv")
    type_bool_msg = "boolean" if type_bool else "non-boolean"
    if env_key_name != cfg_key_name:
        # NOTE: print out WARNING whether or not in verbose mode:
        print('{0}: DEBUG: WARNING: Locutus.Settings.get_env_or_config_val({1}): found the following two differing '\
            'key names to harmonize: \'{2}\' (as via env var) and \'{3}\' (as via config)'.format(
            CLASS_PRINTNAME, type_bool_msg, env_key_name, cfg_key_name), flush=True)
    UNDEF_STRING = 'undef_str'
    key_val = get_env_val_bool(env_key_name, def_val=UNDEF_STRING, print_cfg_out=False, verbose=verbose) \
            if type_bool \
            else get_env_val(env_key_name, def_val=UNDEF_STRING, print_cfg_out=False, verbose=verbose)
    if (key_val != UNDEF_STRING) and (key_val is not None) and (not isinstance(key_val, str) or len(key_val)):
        # non-empty override found in the optional environment override, get it from the config:
        # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val(), but maybe not even this one!
        #if verbose:
        #    print('{0}: VERBOSE: Locutus.Settings.get_env_or_config_val({1}): environment variable \'{2}\' found to '\
        #            'override setting: \'{3}\'; using env value=\'{4}\''.format(
        #            CLASS_PRINTNAME, type_bool_msg, env_key_name, cfg_key_name, key_val), flush=True)
        print('{0},env-{1},{2},{3}'.format(CFG_OUT_PREFIX, type_bool_msg, env_key_name, key_val), flush=True)
    else:
        # nothing found in the optional environment override, get it from the config:
        key_val = get_config_val(config, cfg_key_name, def_val=def_val, print_cfg_out=False, verbose=verbose)
        # NOTE: disabling verbose for get_env_val(), etc., leaving it for just get_env_or_config_val(), but maybe not even this one!
        #if verbose:
        #    print('{0}: VERBOSE: Locutus.Settings.get_env_or_config_val({1}): no valid environment variable \'{2}\' found to '\
        #            'override setting: \'{3}\'; using config/default value=\'{4}\''.format(
        #            CLASS_PRINTNAME, type_bool_msg, env_key_name, cfg_key_name, key_val), flush=True)
        print('{0},cfg-{1},{2},{3}'.format(CFG_OUT_PREFIX, type_bool_msg, cfg_key_name, key_val), flush=True)
    return key_val


class Settings:
    #ENV = 'local'
    #if os.environ.get("CURR_ENV") is not None:
    #    ENV = os.environ.get("CURR_ENV")

    # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
    print('{0},{1},{2},{3}'.format(
                CFG_OUT_PREFIX,
                CFG_OUT_HEADER_KEY_TYPE,
                CFG_OUT_HEADER_KEY,
                CFG_OUT_HEADER_VAL), flush=True)


    # START with the very first CFG_OUTs as the hard-coded APP_NAME and APP_VER values of Locutus itself:
    #
    # APP_NAME:
    print('{0},locutus-hardcoded,{1},{2}'.format(CFG_OUT_PREFIX, "APP_NAME", APP_NAME), flush=True)
    # APP_VERSION:
    print('{0},locutus-hardcoded,{1},{2}'.format(CFG_OUT_PREFIX, "APP_VERSION", APP_VERSION), flush=True)


    ####################################################
    # NOTE: If called from within a Docker container,
    # gethostname() grabs the docker container's info, e.g,: b0bd488d28a3, a5346c62a5b2, etc.
    # As such, utilize DOCKERHOST_HOSTNAME, if so supplied by the deploy_*.sh script to this Docker container.
    if os.environ.get('DOCKERHOST_HOSTNAME'):
        this_hostname = os.environ.get('DOCKERHOST_HOSTNAME')
        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},locutus-env-docker-hostname,{1},{2}'.format(CFG_OUT_PREFIX, "DOCKERHOST_HOSTNAME", this_hostname), flush=True)
    else:
        # NOTE: utilize elements borrowed from module_gcp_dicom.py's Setup()...
        ####################################################
        # determine current hostname, for prefacing stored paths to outputs:
        #this_hostname = socket.getfqdn()
        # just the hostname portion:
        this_hostname = socket.gethostname()
        # TODO: ensure that this gethostname() does return only the hostname; on some VMs, might return the FQDN
        #####
        # NOTE: support quick stdout log crumb of any additional CFGs for a CFG output CSV via: grep CFG_OUT
        print('{0},locutus-socket-gethostname,{1},{2}'.format(CFG_OUT_PREFIX, "this_hostname", this_hostname), flush=True)
        ####################################################

    # LOCUTUS_SYS_STATUS_TABLE:
    #############################
    # TODO: consider where to also CREATE TABLE for initial self.locutus_settings.LOCUTUS_SYS_STATUS_TABLE
    # perhaps best in main_locutus(), or here in settings, though it doesn't yet have a DBconn (see get/set_Locutus_system_status())
    # OR, as now also noted in main_locutus.py, perhaps a call similar to the get/set_Locutus_system_status(), e.g., :
    # a new Settings.create_Locutus_system_status_table(Settings.LOCUTUS_SYS_STATUS_TABLE, no_sysDBconnSession, locutus_target_db)
    # that itself shall do a CREATE TABLE if not exists
    #############################
    # the LOCUTUS-general status table (TODO: to eventually also reference from within each module_*.py)
    LOCUTUS_SYS_STATUS_TABLE = 'aaa_locutus_status'
    print('{0},locutus-hardcoded,{1},{2}'.format(CFG_OUT_PREFIX, "LOCUTUS_SYS_STATUS_TABLE", LOCUTUS_SYS_STATUS_TABLE), flush=True)
    LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_COL = 'status_type'
    LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_OVERALL = 'locutus_system_status_overall'
    LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_PERNODE = 'locutus_system_status_pernode'
    LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_PER_MODULE = 'locutus_system_status_per_module'
    # NOTE: yes, the above values for PERNODE and PER_MODULE have underscore-inconsistencies; perhaps it even helps visually delineate?
    LOCUTUS_SYS_STATUS_TABLE_STATUS_NODE_COL = 'status_node'
    LOCUTUS_SYS_STATUS_TABLE_STATUS_NODE_VAL_OVERALL = 'system-wide'
    LOCUTUS_SYS_STATUS_TABLE_STATUS_DESC_COL = 'status_desc'
    LOCUTUS_SYS_STATUS_TABLE_STATUS_DESC_PREFIX = 'Locutus system status' # (to be followed by the optional " for <nodename>", if applicable)
    LOCUTUS_SYS_STATUS_TABLE_STATUS_DATETIME_COL = 'date_updated'
    LOCUTUS_SYS_STATUS_TABLE_STATUS_ACTIVE_COL = 'active'   # True or False
    # defer calls to get_Locutus_system_status() to main_locutus.py itself,
    # as well as applicable modules (especially those for DeID which can take quite some time with large manifests)

    ##########################################################
    # Orthanc endpoints of note for downloading (TODO: consider moving into towards the Stager or a DICOMSsource object)
    ORTHANC_DOWNLOAD_DICOMDIR_ENDPOINT = '/media'
    ORTHANC_DOWNLOAD_ZIP_ENDPOINT = '/archive'
    ##########################################################

    ##########################################################
    # TODO? *maybe* update cmd_dicom_summarize_status to use the following SQL_OUT_PREFIX, but only used there.
    #   as this was ^C/^V'd from cmd_dicom_summarize_status.py
    # Used here only for System Status
    #####
    # and for SQL output, preface with:
    # An additional preface field for generating a SQL_OUT: in stdout (e.g., from DRY RUNs to manually perform the SQL), as via:
    #    grep SQL_OUT [log] | sed 's/SQL_OUT:,//` > sql_out.csv
    SQL_OUT_PREFIX = "SQL_OUT:"
    ##########################################################
    # and for manifest table updates of manifest_status with potential errors of exceptions with quotes:
    # to help cleanse the UPDATE's manifest_status such as with:
    #   manifest_status.replace(SQL_UPDATE_QUOTE_TO_REPLACE, SQL_UPDATE_QUOTE_REPLACEMENT),
    SQL_UPDATE_QUOTE_TO_REPLACE='\''
    SQL_UPDATE_QUOTE_REPLACEMENT='|'
    ##########################################################

    ##########################################################
    # delimter to help separate hostname from filepath:
    HOSTPATH_DELIMITER = ':'
    ##########################################################

    ##########################################################
    # MANIFEST_OUTPUT_STATUS_* for processing modules, as expected by cmd_dicom_summarize_status.py
    MANIFEST_OUTPUT_STATUS_NOT_FOUND = "NOT_FOUND"
    MANIFEST_OUTPUT_STATUS_PENDING = 'PENDING_CHANGE'
    MANIFEST_OUTPUT_STATUS_PROCESSED = 'PROCESSED'
    MANIFEST_OUTPUT_STATUS_PROCESSED_SUBACCESSIONS = 'PROCESSED SPLIT SUB-ACCESSIONS (as included in the above total PROCESSED)'
    MANIFEST_OUTPUT_STATUS_PROCESSED_WHOLEACCESSIONS_EST = 'PROCESSED ESTIMATED "Whole" ACCESSIONS (as in the above total PROCESSED)'
    MANIFEST_OUTPUT_STATUS_PROCESSED_WHOLEACCESSIONS = 'PROCESSED "Whole" ACCESSIONS (as in the above total PROCESSED)'
    #####
    MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_PREFIX = 'PREVIOUS_PROCESSING_USED_'
    # TODO: use these _PREFIX and _MIDFIX at, e.g., each whynot_manifest_status, but...
    # these 2x #defines just don't quite pop out the same in the code w/o the text syntax highlighting.
    MANIFEST_OUTPUT_STATUS_PREVIOUS_PROCESSING_MIDFIX = '_PRERETIRE_OR_PREDELETE_OR_FORCE_TO_REPROCESS_'
    #####
    MANIFEST_OUTPUT_STATUS_PROCESSING_PREFIX = 'PROCESSING_CHANGE'
    MANIFEST_OUTPUT_STATUS_PROCESSING_MOMENTARILY_PREFIX = 'PROCESSING_CHANGE_MOMENTARILY'
    MANIFEST_OUTPUT_STATUS_MULTIPLE_SPLITS_PREFIX = 'ACCESSION_HAS_MULTIPLE_SPLITS'
    MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX = 'ERROR_MULTIPLE_CHANGE_UUIDS'
    MANIFEST_OUTPUT_SHOW_MULTIUUIDS_MANIFEST_STATUS = 'SHOW_MULTIUUIDS_UUID'
    MANIFEST_OUTPUT_STATUS_ERROR_OTHER_PREFIX = 'ERROR'
    ##########################################################
    # help reduce Phase-nested errors by starting each handled exception
    # with the same ERR_PHASE_PREFIX:
    ERR_PHASE_PREFIX = 'ERROR_PHASE'
    ##########################################################

    ##########################################################
    # ONDECK_* for the Preloader to generate via cmd_dicom_summarize_status.py, and the processing modules to honor
    # cmd_dicom_summarize_status sets the following in its preload_new_accessions_per_manifest():
    #       ONDECK_PROCESSING_CHANGE="ZZZ-ONDECK-4-PROCESSING_CHANGE"+custom_suffx
    # for example:
    #       ZZZ-ONDECK-4-PROCESSING_CHANGE:SCIT605-batch13_preLoadedSansText
    ##########################################################
    # NOTE: revising the manifest_status for Preloading accessions,
    # such that are conveyed not as PROCESSING_MOMENTARILY (as typically more applicable to PHASE_SWEEPs, etc)
    # but are more "ON_DECK" for a future run, using ZZZs to preface such that an alpha sort of the manifest_status
    # will show these at the very bottom.
    #####
    ONDECK_PENDING_CHANGE_PREFIX="ZZZ-ONDECK-" + MANIFEST_OUTPUT_STATUS_PENDING    # == "PENDING_CHANGE"
    ONDECK_PROCESSING_CHANGE_PREFIX="ZZZ-ONDECK-4-" + MANIFEST_OUTPUT_STATUS_PROCESSING_PREFIX # == "PROCESSING_CHANGE"
    ONDECK_REPROCESSING_CHANGE_PREFIX="ZZZ-ONDECK-4-RE-" + MANIFEST_OUTPUT_STATUS_PROCESSING_PREFIX # RE-PROCESSING_CHANGE"
    ONDECK_MULTIPLES_CHANGE_PREFIX="ZZZ-ONDECK-2-RESOLVE-" + MANIFEST_OUTPUT_STATUS_ERROR_MULTIPLE_PREFIX # "ERROR_MULTIPLE_CHANGE_UUIDS"
    #####
    ## for any ERROR%, including those ERROR_MULTIPLE_CHANGE_UUIDS, where already noted as such:
    # (as used in cmd_dicom_summarize_status.py)
    ONDECK_OTHER_PREFIX="ZZZ-ONDECK-"
    ##########################################################
    # alias to the above ONDECK_MULTIPLES_CHANGE_PREFIX (as used in module_gcp_dicom.py),
    #   to accommodate variant naming elsewhere (in cmd_dicom_summarize_status.py):
    ONDECK_ERROR_MULTIPLE_CHANGE_UUIDS_PREFIX=ONDECK_MULTIPLES_CHANGE_PREFIX
    # TODO: align these, to remove such an alias
    ##########################################################

    #####
    PROCESSING_SUFFIX_DELIM=':'
    # as used for preset_accession_for_reprocessing(), or more focused subset of the Preloader:
    REPROCESSING_CHANGE_MOMENTARILY="RE-" + MANIFEST_OUTPUT_STATUS_PROCESSING_MOMENTARILY_PREFIX # "RE-PROCESSING_CHANGE_MOMENTARILY"
    REPROCESSING_SUFFIX_DELIM=':'
    #####

    # and additional post-resolution STATUS possibilities:
    MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_MERGE = 'PENDING_CHANGE_RADIOLOGY_MERGE'
    MANIFEST_OUTPUT_STATUS_PENDING_RADIOLOGY_RESEND = 'PENDING_CHANGE_RADIOLOGY_RESEND'
    MANIFEST_OUTPUT_STATUS_PENDING_AFTER_LOCAL_CONSOLIDATION = 'PENDING_CHANGE_RESOLVED_LOCALLY'
    # 2/06/2025: updated the following to better align with the Pre-loader, saving ':' as delimeter to the Pre-load suffix:
    MANIFEST_OUTPUT_STATUS_OTHER_AFTER_LOCAL_CONSOLIDATION = 'RESOLVED_MULTIUUIDS_CONSOLIDATED_LOCALLY'
    MANIFEST_OUTPUT_STATUS_AFTER_LOCAL_CHOICE = 'RESOLVED_MULTIUUIDS_CHOSEN_LOCALLY'
    MANIFEST_OUTPUT_STATUS_AFTER_LOCAL_DELETE = 'RESOLVED_MULTIUUIDS_DELETED_LOCALLY'
    MANIFEST_OUTPUT_STATUS_AFTER_LOCAL_DELETE_STAGE_ONLY = 'RESOLVED_MULTIUUIDS_DELETED_onSTAGE_ONLY'
    # generic cmd-less versions:
    MANIFEST_OUTPUT_STATUS_OTHER_AFTER_RESOLVE_GENERIC = 'RESOLVED_MULTIUUIDS'
    MANIFEST_OUTPUT_STATUS_PENDING_AFTER_RESOLVE_GENERIC = 'PENDING_CHANGE_RESOLVED_MULTIUUIDS'

    ##########################################################

    # For Processing modules (and those that rely upon them, such as the Summarizer), define MIN and MAX Processing Phase:
    DICOM_GCP_MIN_PROCESSING_PHASE = 2
    DICOM_GCP_MAX_PROCESSING_PHASE = 5
    DICOM_ONPREM_MIN_PROCESSING_PHASE = 2
    DICOM_ONPREM_MAX_PROCESSING_PHASE = 5

    # next START with the next first CFG_OUT as JOB_DESCRIPTION & INPUT_MANIFEST_NAME to facilitate easier management of these outputs:
    JOB_DESCRIPTION = ''
    JOB_DESCRIPTION = get_env_val("JOB_DESCRIPTION", def_val=JOB_DESCRIPTION, print_cfg_out=True, verbose=True)
    INPUT_MANIFEST_NAME = ''
    INPUT_MANIFEST_NAME = get_env_val("INPUT_MANIFEST_NAME", def_val=INPUT_MANIFEST_NAME, print_cfg_out=True, verbose=True)
    ENV_CONFIG_PATH = '.'
    ENV_CONFIG_PATH = get_env_val("PATH_TO_CONFIG", def_val=ENV_CONFIG_PATH, print_cfg_out=True, verbose=True)

    config = yaml.safe_load(open('{0}/config.yaml'.format(ENV_CONFIG_PATH), 'r').read())

    ####################################################
    ####################################################
    # general Locutus configuration follows, with each such config being required...
    # (noting that the module configs are only required if their process_<module> is enabled)

    # Debug settings for LOCUTUS_VERBOSE and LOCUTUS_TEST modes
    # Allow these both to come in through an env var override or from the config:
    LOCUTUS_VERBOSE = False
    LOCUTUS_VERBOSE = get_env_or_config_val(TYPE_BOOL, 'locutus_verbose', config, 'locutus_verbose', def_val=LOCUTUS_VERBOSE, print_cfg_out=True, verbose=True)
    LOCUTUS_TEST = False
    LOCUTUS_TEST = get_env_or_config_val(TYPE_BOOL, 'locutus_test', config, 'locutus_test', def_val=LOCUTUS_TEST, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)

    # TODO: consider eventually passing GOOGLE_APPLICATION_CREDENTIALS in via the config.yaml,
    # depending opon the deployment methodology.
    # For now, though, read from the environment (the Docker container's local credentialsfile):
    LOCUTUS_GOOGLE_APPLICATION_CREDENTIALS = ''
    LOCUTUS_GOOGLE_APPLICATION_CREDENTIALS = get_env_val("GOOGLE_APPLICATION_CREDENTIALS", def_val=LOCUTUS_GOOGLE_APPLICATION_CREDENTIALS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    # and an optional variation of this to point back to the original credentials vault path)
    LOCUTUS_GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH = ''
    LOCUTUS_GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH = get_env_val("GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH", def_val=LOCUTUS_GOOGLE_APPLICATION_CREDENTIALS_SOURCE_VAULT_PATH, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)

    # Emit Jenkins job information:
    JENKINS_BUILD_NAME = ''
    JENKINS_BUILD_NAME = get_env_val("JOB_NAME", def_val=JENKINS_BUILD_NAME, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    JENKINS_BUILD_NUMBER = ''
    JENKINS_BUILD_NUMBER = get_env_val("BUILD_NUMBER", def_val=JENKINS_BUILD_NUMBER, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)

    # Allow the Docker container name to help create a CFG_OUT: (as usually contains the environment,
    # e.g. "locutus_onprem_dicom_only_prod2dev")
    LOCUTUS_DOCKERHOST_CONTAINER_NAME = ''
    LOCUTUS_DOCKERHOST_CONTAINER_NAME = get_env_val("DOCKERHOST_CONTAINER_NAME", def_val=LOCUTUS_DOCKERHOST_CONTAINER_NAME, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    # AND the Docker image tag, to also help create a CFG_OUT:
    # NOTE: this may suffice in the production environment where deployed via Docker containers from our infrastructure,
    # BUT a further enhancement of this might be necessary for any locally-based deployments without such a DOCKERHOST_CONTAINER_NAME.
    # TODO: Q: shall the following become a required parameter in order to help provide more replicatable tracing,
    # even if such local non-Docker deployments might require a manual setting of that variable with the following,
    # for example:
    # export DOCKERHOST_CONTAINER_NAME="localrun_"`git branch | grep "*" | awk '{print $2}'`\
    #                                           "@"`git log | head -1 | awk '{print $2}'`
    # echo $DOCKERHOST_CONTAINER_NAME
    #       localrun_develop@87792c1648a25f2b9d741e8ac8fdf69d7035a4f7
    # NOTING that even this local approach will not be showing any differences from the most recent git commit,
    # and could be further elaborated upon
    LOCUTUS_DOCKERHOST_IMAGE_TAG = ''
    LOCUTUS_DOCKERHOST_IMAGE_TAG = get_env_val("DOCKERHOST_IMAGE_TAG", def_val=LOCUTUS_DOCKERHOST_IMAGE_TAG, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)

    # NOTE: VAULT_NAMESPACE is read along with VAULT_ADDR for the new RIS Vault server in main_locutus.py

    # Locutus Target DB: w/ DEV_SUFFIX == "" (for prod DB) or "_dev" (for dev DB)
    # and DROP_TABLES likely only to be used for RUN_MODE="single" w/ nightly deployment
    LOCUTUS_DB_VAULT_PATH = 'unknown_DB_vault_path'
    LOCUTUS_DB_VAULT_PATH = get_config_val(config, 'locutus_DB_vault_path', def_val=LOCUTUS_DB_VAULT_PATH, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    LOCUTUS_DB_USE_DEV_SUFFIX = False
    LOCUTUS_DB_USE_DEV_SUFFIX = get_config_val(config, 'locutus_DB_use_dev_suffix', def_val=LOCUTUS_DB_USE_DEV_SUFFIX, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    LOCUTUS_DB_DEV_SUFFIX = ''
    LOCUTUS_DB_DEV_SUFFIX = get_config_val(config, 'locutus_DB_dev_suffix', def_val=LOCUTUS_DB_DEV_SUFFIX, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    LOCUTUS_DB_DROP_TABLES = False
    LOCUTUS_DB_DROP_TABLES = get_config_val(config, 'locutus_DB_drop_tables', def_val=LOCUTUS_DB_DROP_TABLES, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #
    #####################################
    # TODO: introduce a mechanism into each module to incorporate the following new configurable target outputs:
    # NOTE: each module is currently using just the LOCUTUS_TARGET_S3_BUCKET (without LOCUTUS_TARGET_USE_S3!),
    # and will want to begin checking each LOCUTUS_TARGET_USE_* flag accordingly.
    # TODO: AND eventually implement in each module the logic (possibly using a shared library!)
    #
    # TODO: these target selections could eventually be a per-module setting in the config itself,
    # but for now, just a quick selection on a global level (& can have several Jenkins jobs as needed)
    num_targets_configured = 0
    #####################################
    # Locutus Target: AWS s3 bucket:
    LOCUTUS_TARGET_USE_S3 = False
    LOCUTUS_TARGET_USE_S3 = get_config_val(config, 'locutus_target_use_s3', def_val=LOCUTUS_TARGET_USE_S3, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    LOCUTUS_TARGET_S3_BUCKET = None
    if LOCUTUS_TARGET_USE_S3:
        num_targets_configured += 1
        LOCUTUS_TARGET_S3_BUCKET = get_config_val(config, 'locutus_target_s3_bucket', def_val='', print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################################
    # Locutus Target: local isilon path:
    LOCUTUS_TARGET_USE_ISILON = False
    LOCUTUS_TARGET_USE_ISILON = get_config_val(config, 'locutus_target_use_isilon', def_val=LOCUTUS_TARGET_USE_ISILON, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    LOCUTUS_TARGET_ISILON_PATH = None
    if LOCUTUS_TARGET_USE_ISILON is not None and LOCUTUS_TARGET_USE_ISILON:
        num_targets_configured += 1
        LOCUTUS_TARGET_ISILON_PATH = get_config_val(config, 'locutus_target_isilon_path', def_val='', print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################################
    # Locutus Target: GCP gs bucket:
    # TODO: first check to see that this is even in the various configs:
    LOCUTUS_TARGET_USE_GS = False
    LOCUTUS_TARGET_USE_GS = get_config_val(config, 'locutus_target_use_gs', def_val=LOCUTUS_TARGET_USE_GS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    LOCUTUS_TARGET_GS_BUCKET = None
    if LOCUTUS_TARGET_USE_GS is not None and LOCUTUS_TARGET_USE_GS:
        num_targets_configured += 1
        LOCUTUS_TARGET_GS_BUCKET = get_config_val(config, 'locutus_target_gs_bucket', def_val='', print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    # for early detection of main_locutus's Settings.locutus_target_GSclient.get_bucket(), and beyond:
    # allow any GCP_INVALID_CREDS_ERR to bypass FORCE_SUCCESS because GCP will no longer work with the now old creds:
    GCP_INVALID_CREDS_ERR = 'invalid_grant: Invalid JWT Signature.'
    # an error message with the above can be sooooo very verbose, even repeating the above a few times
    # when so encountered, therefore replace with the concise version, just the name itself:
    GCP_INVALID_CREDS_ERR_NAME = 'GCP_INVALID_CREDS_ERR'
    # NOTE: JWT == JSON Web Token, as per: https://cloud.google.com/endpoints/docs/frameworks/java/troubleshoot-jwt
    # "When a client application includes a JSON Web Token (JWT) in a request to an API,
    #  the Extensible Service Proxy (ESP) validates the JWT before sending the request to
    #  the API backend. This page provides troubleshooting information if the JWT validation
    #  fails and ESP returns an error in the response to the client.
    #  See RFC 7519 for more information about JWTs."
    #####################################
    # TODO: eventually allow the various _STATUS tables to accommodate multiple target locations,
    # but for now, module & manifest processing is really only setup for one, so limit that here:
    if num_targets_configured >= 1:
        if LOCUTUS_VERBOSE:
            print('VERBOSE: Locutus.Settings: {0} destination target(s) configured, good!'.format(num_targets_configured), flush=True)
    else:
        # NOTE: allowing processing up to, but not including, the target uploads:
        print('WARNING: Locutus.Settings: NO destination targets configured, processing up to that point anyhow.'.format(
                num_targets_configured), flush=True)

    ##############################################
    # additional Locutus-specific configurations:
    # LOCUTUS_FORCE_SUCCESS to encourage any of the sub-modules to return a success
    # regardless of errors, such that more complex multiple call scenarios
    # (such as Aperio's processing loop) will still print out any ERRORS/WARNINGS
    # but will return a 0 rather than a non-0 error status (for non-fatal errors,
    # at least) since non-0 exits from a Dockerized job called by Jenkins can
    # cause the overall Jenkins job to halt, even if is more to process.
    #
    # TODO: find other options that might allow an error to still be propagated:
    LOCUTUS_FORCE_SUCCESS = False
    LOCUTUS_FORCE_SUCCESS = get_env_or_config_val(TYPE_BOOL, 'locutus_force_success', config, 'locutus_force_success', def_val=LOCUTUS_FORCE_SUCCESS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    ##############################################
    # Run mode: single -vs- run_mode: continuous (with wait_secs for continuous)
    LOCUTUS_RUN_MODE = 'single'
    LOCUTUS_RUN_MODE = get_config_val(config, 'locutus_run_mode', def_val=LOCUTUS_RUN_MODE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    ###
    # with locutus_dicom_run_mode_continue_to_manifest_convergence
    # as a new config setting to supplement the above LOCUTUS_RUN_MODE,
    # such that LOCUTUS_RUN_MODE is not itself directly exposed to the env & Jenkins,
    # thereby reducing the risk of an inadvertent fully "continuous" run
    #
    # FURTHER DevNotes:
    ###########
    # locutus_dicom_continue_to_manifest_convergence: a user-exposed run_mode qualifier of sorts
    LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE = False
    LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE = get_env_or_config_val(TYPE_BOOL, 'locutus_dicom_run_mode_continue_to_manifest_convergence', config, 'locutus_dicom_run_mode_continue_to_manifest_convergence', def_val=LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #
    # and a limit to ensure that we terminate convergence attempts at "something reasonable",
    # lest we potentially end up in a runaway loop:
    ########
    # for LARGER expectations:
    #LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE_MAX_ITERATIONS = 21
    # for MIDWAY expectations:
    LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE_MAX_ITERATIONS = 11
    # for SMALLER expectations:
    # r3m0 DEBUG: testing with a tighter MAX:
    #LOCUTUS_DICOM_RUN_MODE_CONTINUE_TO_MANIFEST_CONVERGENCE_MAX_ITERATIONS = 5
    ##############
    LOCUTUS_CONTINUOUS_WAIT_SECS = 1
    LOCUTUS_CONTINUOUS_WAIT_SECS = get_config_val(config, 'locutus_continuous_wait_secs', def_val=LOCUTUS_CONTINUOUS_WAIT_SECS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    # DEBUG option to leave processed interim files around:
    LOCUTUS_KEEP_INTERIM_FILES = False
    LOCUTUS_KEEP_INTERIM_FILES = get_config_val(config, 'locutus_debug_keep_interim_files', def_val=LOCUTUS_KEEP_INTERIM_FILES, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    # allow overnight testing of large manifests consisting of just a single unique test accession, repeated:
    LOCUTUS_ALLOW_PROCESSING_OF_DUPLICATES = False
    LOCUTUS_ALLOW_PROCESSING_OF_DUPLICATES = get_env_or_config_val(TYPE_BOOL, 'locutus_allow_processing_of_duplicates', config, 'locutus_allow_processing_of_duplicates', def_val=LOCUTUS_ALLOW_PROCESSING_OF_DUPLICATES, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    # a summarizer/stagecmp/gcp/onprem(TODO: CONFIRM!) option to automatically string any text from input manifest accessions:
    LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS = False
    LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS = get_env_or_config_val(TYPE_BOOL, 'locutus_dicom_remove_text_from_input_accessions', config, 'locutus_dicom_remove_text_from_input_accessions', def_val=LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    print('WARNING: Settings() just configured LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS={0}.  '\
                'Please note that this REMOVE_TEXT setting has been deprecated with the Juneteenth 2025 Upgrade '\
                'to a friendlier alpha-numeric accession aware Locutus'.format(LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS), flush=True)
    ##############
    # Migration: a gcp/onprem option to automatically no-longer-used (in the Stager) "zombie" changes/accessions from the Locutus tables, during Phase02 migration:
    LOCUTUS_DICOM_FORCE_ALPHANUM_REUPGRADE_DURING_MIGRATION = False
    LOCUTUS_DICOM_FORCE_ALPHANUM_REUPGRADE_DURING_MIGRATION = get_env_or_config_val(TYPE_BOOL, 'locutus_dicom_force_alphanum_reupgrade_during_migration', config, 'locutus_dicom_force_alphanum_reupgrade_during_migration', def_val=LOCUTUS_DICOM_FORCE_ALPHANUM_REUPGRADE_DURING_MIGRATION, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    ##############
    # Migration: a gcp/onprem option to automatically no-longer-used (in the Stager) "zombie" changes/accessions from the Locutus tables, during Phase02 migration:
    LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION = False
    LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION = get_env_or_config_val(TYPE_BOOL, 'locutus_dicom_remove_zombie_change_seq_ids_at_migration', config, 'locutus_dicom_remove_zombie_change_seq_ids_at_migration', def_val=LOCUTUS_DICOM_REMOVE_ZOMBIE_CHANGE_SEQ_IDS_AT_MIGRATION, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    ##############
    # Migration: a gcp/onprem option to automatically bypass the Phase02 migration portion entirely, a hot fix to bypass it in the unusual event of failing migration:
    LOCUTUS_DICOM_BYPASS_MIGRATION = False
    LOCUTUS_DICOM_BYPASS_MIGRATION = get_env_or_config_val(TYPE_BOOL, 'locutus_dicom_bypass_migration', config, 'locutus_dicom_bypass_migration', def_val=LOCUTUS_DICOM_BYPASS_MIGRATION, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################

    #####################
    # The above general settings WITHOUT get_env_or_config_val() are currently only configurable through the config.yaml via get_config_al()
    # but many can also be overridden by Environment Variables, via a subsequent call to get_env_val()/get_env_val_bool().

    #####################
    # An option to support concurrent deployment (through multiple Jenkins jobs etc.),
    # to ensure that no jobs pick up any other jobs at the ending phase sweep
    # (following the manifest Phase 1-3, finding any stragglers for Phase 4 and Phase 5)
    # Should really, therefore, only set locutus_disable_phase_sweep to True when deploying concurrently.
    # NOTE: currently only supported by Aperio & ONPREM/GCP DICOM (to eventually be used by all):
    #####################
    LOCUTUS_DISABLE_PHASE_SWEEP = False
    LOCUTUS_DISABLE_PHASE_SWEEP = get_env_or_config_val(TYPE_BOOL, 'locutus_disable_phase_sweep', config, 'locutus_disable_phase_sweep', def_val=LOCUTUS_DISABLE_PHASE_SWEEP, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################
    # And a counterpart for expanding phase sweeps beyond the current input manifest,
    # when not otherwise disabled as with the above LOCUTUS_DISABLE_PHASE_SWEEP
    # NOTE: currently only adding support for the DICOM modules
    # TODO: add to other modules (which will default to a phase sweep of ALL until such time)
    LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST = False
    LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST = get_env_or_config_val(TYPE_BOOL, 'locutus_expand_phase_sweep_beyond_manifest', config, 'locutus_expand_phase_sweep_beyond_manifest', def_val=LOCUTUS_EXPAND_PHASE_SWEEP_BEYOND_MANIFEST, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################

    #####################
    # Introducing... Locutus Workspaces!
    LOCUTUS_WORKSPACES_ENABLE = False
    LOCUTUS_WORKSPACES_ENABLE = get_env_or_config_val(TYPE_BOOL, 'locutus_workspaces_enable', config, 'locutus_workspaces_enable', def_val=LOCUTUS_WORKSPACES_ENABLE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    LOCUTUS_WORKSPACE_NAME = "default"
    LOCUTUS_WORKSPACE_NAME = get_env_or_config_val(TYPE_NON_BOOL, 'locutus_workspace_name', config, 'locutus_workspace_name', def_val=LOCUTUS_WORKSPACE_NAME, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################

    #####################
    # And additional storage for items later generated from the above config,
    # for sub_modules (such as OnPremDicom) to most easily access
    #####################
    locutus_target_db_name = ""
    locutus_target_db = ""
    # For remote AWS s3 targets:
    locutus_target_s3client = None
    locutus_target_s3resource = None
    # For remote GCP GS targets:
    locutus_target_GSclient = None


    ########################################################################################################
    ########################################################################################################
    # each sub-module specific configuration follows...


    ####################################################
    ####################################################
    # Configs for the Dicom_summarize_status command:
    # including its source OnPrem-DICOM-Stage configuration of Orthanc Server, etc.
    # But first, an initial set of unknown defaults (in case subsequently overridden with environment variables):
    PROCESS_DICOM_SUMMARIZE_STATS = False
    LOCUTUS_DICOM_SUMMARIZE_STATS_MANIFEST_CSV = "unknown_LOCUTUS_DICOM_SUMMARIZE_STATS_INPUT_MANIFEST.csv"
    # and, expected DICOM module table for the summarize/split (either "ONPREM or "GCP"):
    LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE = "unknown_ONPREM_or_GCP"
    # and, an option to support more concise weekly manifest-driven stats-only summary reports:
    # (essentially a not LOCUTUS_DICOM_SUMMARIZE_STATS_ONLY_HIDE_ACCESSIONS)
    LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS = True
    # and, for when showing each accession MANIFEST_OUTPUT line (though not any inputs in the build logs!), 
    # and still wanting to redact the current accession num MANIFEST_OUTPUT field:
    LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS = False
    # with a summarizer option to help better investigate (and eventually resolve) multi-uuids, without needing to change the manifest:
    LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS = False
    #####################  ##################### #####################
    # AND a corresponding SAFE_MODE (via ENABLE_UPDATES) as the default, without any such WRITES
    # TODO: now that resolve_multiuuids can use the following to enable any Orthanc DELETES as well,
    # consider renaming this to LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_AND_ORTHANC_UPDATES; until then, though:
    LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES = False
    # on which the following settings rely on being enabled for anything more than a safe dry-run test.
    #####################
    # WARNING: a writable summarizer mode to help resolve multi-uuids:
    LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST = False
    LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST_PREPROCESSING_SUFFIX = 'summarizerPreLoaded'
    #####################
    # WARNING: a writable summarizer mode to help resolve multi-uuids:
    LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS = False
    #####################
    # WARNING: a writable summarizer mode to help preset PROCESSING_MOMENTARILY* statuses prior to a force_reprocess, or similar::
    LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS = False
    LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS_SUFFIX = 'reprocessing in 3... 2... 1...'
    #####################  ##################### #####################
    # NOTE: r3m0 pulling these RESOLVER_STAGE_DB details OUT,
    # to replace with the full dicom_summarize_dicom_stage_config_vault_path
    #####
    # as well as a pointer to the Stager DB (no need for a full Vault path, merely its name)
    #WAS: LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_NAME = "unknown_Stager_DB_name"
    #WAS: LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_DEV_SUFFIX = "unknwown_DB_dev_suffix"
    #WAS: LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_USE_DEV_SUFFIX = False
    #####
    # full Vault path now needed for show_multiuuids,
    # so utilze it for the resolver as well:
    LOCUTUS_DICOM_SUMMARIZE_STATS_STAGE_VAULT_PATH = "undefined_LOCUTUS_DICOM_SUMMARIZE_STATS_STAGE_VAULT_PATH"
    #####################  ##################### #####################
    #
    LOCUTUS_DICOM_SUMMARIZE_ONPREM_FLYWHEEL_GROUP = "OnPrem"
    LOCUTUS_DICOM_SUMMARIZE_ONPREM_FLYWHEEL_PROJECT = "FW_Project"
    #####################
    #####################
    # And now, the optional configurations:
    PROCESS_DICOM_SUMMARIZE_STATS = get_env_or_config_val(TYPE_BOOL, 'process_dicom_summarize_stats', config, 'process_dicom_summarize_stats', def_val=PROCESS_DICOM_SUMMARIZE_STATS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################
    if PROCESS_DICOM_SUMMARIZE_STATS:

        ##################### ##################### ##################### ##################### #####################
        ##################### ##################### ##################### ##################### #####################
        # In addition to PROCESS_DICOM_SUMMARIZE_STATS,
        # the above PROCESS_DICOM_SUMMARIZE_STATS settings are currently only configurable through the config.yaml via get_config_val()
        # but the following can also be overridden by Environment Variables,
        # via the combined calls to get_env_or_config_val():
        #####################
        #####################
        # expected input manifests:
        # TODO: harmonize the following two key names across the config and env vars:
        LOCUTUS_DICOM_SUMMARIZE_STATS_MANIFEST_CSV = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_stats_manifest_csv', config, 'dicom_summarize_stats_manifest_csv', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_MANIFEST_CSV, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # expected DICOM module table for the summarize/split (either "ONPREM or "GCP"):
        # DONE: allow a drop-down for selection of these choices in Jenkins.
        LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_stats_module', config, 'dicom_summarize_stats_module', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_FOR_DICOM_MODULE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # to support more concise weekly manifest-driven stats-only summary reports:
        # (essentially a not LOCUTUS_DICOM_SUMMARIZE_STATS_ONLY_HIDE_ACCESSIONS)
        LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS = get_env_or_config_val(TYPE_BOOL, 'dicom_summarize_stats_show_accessions', config, 'dicom_summarize_stats_show_accessions', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_ACCESSIONS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # with a summarizer option to help better investigate (and eventually resolve) multi-uuids, without needing to change the manifest:
        LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS = get_env_or_config_val(TYPE_BOOL, 'dicom_summarize_stats_show_multiuuids', config, 'dicom_summarize_stats_show_multiuuids', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_SHOW_MULTIUUIDS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # and for both the SHOW_MULTIUUIDS and RESOLVE_MULTIUUIDS:
        # NOTE: the following SUMMARIZE_DICOM_STAGE may be the same as the GCP_DICOM_STAGE &/or ONPREM_DICOM_STAGE:
        LOCUTUS_DICOM_SUMMARIZE_STATS_STAGE_VAULT_PATH = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_dicom_stage_config_vault_path', config, 'dicom_summarize_dicom_stage_config_vault_path', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_STAGE_VAULT_PATH, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # NOTE: GCP's Stage Vault Path was just via config, no env;
        #   get_config_val(config, 'gcp_dicom_stage_config_vault_path', def_val=GCP_DICOM_STAGE_VAULT_PATH, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # but we may want expose this Summarizer one to Jenkins....
        # ... at least, until we can update our config.yaml files into Vault
        #####################
        # if showing accessions, option to automatically redact from MANIFEST_OUTPUT lines any fields KNOWN/SEEN to be accession_num.
        # NOTE: WARNING that other accession numbers might still appear in the wild of comments throughout the accession, etc.
        LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS = get_env_or_config_val(TYPE_BOOL, 'dicom_summarize_stats_redact_accessions', config, 'dicom_summarize_stats_redact_accessions', def_val=LOCUTUS_DICOM_SUMMARIZE_REDACT_ACCESSIONS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################  ##################### #####################
        # AND a corresponding SAFE_MODE (via ENABLE_DB_UPDATES) as the default, without any such WRITES:
        LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES = get_env_or_config_val(TYPE_BOOL, 'dicom_summarize_stats_enable_db_updates', config, 'dicom_summarize_stats_enable_db_updates', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # on which the following settings rely on being enabled for anything more than a safe dry-run test.
        #####################
        # WARNING: a writable summarizer mode to help: preload_new_accessions_per_manifest:
        LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST = get_env_or_config_val(TYPE_BOOL, 'dicom_summarize_stats_preload_new_accessions_per_manifest', config, 'dicom_summarize_stats_preload_new_accessions_per_manifest', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST_PREPROCESSING_SUFFIX = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_stats_preload_new_accessions_per_manifest_preprocessing_suffix', config, 'dicom_summarize_stats_preload_new_accessions_per_manifest_preprocessing_suffix', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_PRELOAD_NEW_ACCESSIONS_PER_MANIFEST_PREPROCESSING_SUFFIX, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # WARNING: a writable summarizer mode to help resolve multi-uuids:
        # see also LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES for:
        LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS = get_env_or_config_val(TYPE_BOOL, 'dicom_summarize_stats_resolve_multiuuids', config, 'dicom_summarize_stats_resolve_multiuuids', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_RESOLVE_MULTIUUIDS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # WARNING: a writable summarizer mode to help resolve multi-uuids:
        # see also LOCUTUS_DICOM_SUMMARIZE_STATS_ENABLE_DB_UPDATES for:
        LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS = get_env_or_config_val(TYPE_BOOL, 'dicom_summarize_stats_preset_reprocessing_status', config, 'dicom_summarize_stats_preset_reprocessing_status', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS_SUFFIX = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_stats_preset_reprocessing_status_suffix', config, 'dicom_summarize_stats_preset_reprocessing_status_suffix', def_val=LOCUTUS_DICOM_SUMMARIZE_STATS_PRESET_REPROCESSING_STATUS_SUFFIX, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################  ##################### #####################
        #####################
        # as well as a pointer to the Stager DB (no need for a full Vault path, merely its name)
        # noting that this is a quick shortcut, rather than having to go through all of the Stager's config to get it)
        # DANGER: runs the risk of being out of sync, should the Stager config ever change!
        #WAS: LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_NAME = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_resolver_stage_DB_name', config, 'dicom_summarize_resolver_stage_DB_name', def_val=LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_NAME, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #WAS: LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_DEV_SUFFIX = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_resolver_stage_DB_dev_suffix', config, 'dicom_summarize_resolver_stage_DB_dev_suffix', def_val=LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_DEV_SUFFIX, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #WAS: LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_USE_DEV_SUFFIX = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_resolver_stage_DB_use_dev_suffix', config, 'dicom_summarize_resolver_stage_DB_use_dev_suffix', def_val=LOCUTUS_DICOM_SUMMARIZE_RESOLVER_STAGE_DB_USE_DEV_SUFFIX, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################

        #####################
        # Flywheel group & project, as optional output for cmd_dicom_summarize_status:
        LOCUTUS_DICOM_SUMMARIZE_ONPREM_FLYWHEEL_GROUP = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_onprem_flywheel_group', config, 'dicom_summarize_onprem_flywheel_group', def_val=LOCUTUS_DICOM_SUMMARIZE_ONPREM_FLYWHEEL_GROUP, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        LOCUTUS_DICOM_SUMMARIZE_ONPREM_FLYWHEEL_PROJECT = get_env_or_config_val(TYPE_NON_BOOL, 'dicom_summarize_onprem_flywheel_project', config, 'dicom_summarize_onprem_flywheel_project', def_val=LOCUTUS_DICOM_SUMMARIZE_ONPREM_FLYWHEEL_PROJECT, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        ##################### ##################### ##################### ##################### #####################
        ##################### ##################### ##################### ##################### #####################



    ####################################################
    ####################################################
    # Configs for the System_Status command (as now built-in to main_locutus.py itself)
    # But first, an initial set of unknown defaults (in case subsequently overridden with environment variables):
    PROCESS_LOCUTUS_SYSTEM_STATUS = False
    #####################
    # for an overall status (if !USE_SYSTEM_STATUS_NODE), or a specific node (if USE_SYSTEM_STATUS_NODE):
    LOCUTUS_USE_SYSTEM_STATUS_NODE = False
    # and, if so node-enabled, the node name to GET or SET the corresponding System Status:
    LOCUTUS_USE_SYSTEM_STATUS_NODE_NAME = 'UnknownNode'
    #####################
    # for an overall status (if !USE_SYSTEM_STATUS_MODULE), or a specific module (if USE_SYSTEM_STATUS_MODULE):
    LOCUTUS_USE_SYSTEM_STATUS_MODULE = False
    # and, if so module-enabled, the module name to GET or SET the corresponding System Status:
    LOCUTUS_USE_SYSTEM_STATUS_MODULE_NAME = 'UnknownModule'
    ##################### ##################### #####################
    # AND a corresponding SAFE_MODE (via ENABLE_UPDATES) as the default, without any such WRITES
    # WARNING: to write the above set system status into the DB, must also enable:
    LOCUTUS_SYSTEM_STATUS_ENABLE_DB_UPDATES = False
    #####################
    # If PROCESS_LOCUTUS_SYSTEM_STATUS, then...
    # *EITHER* sub-command, SET:
    LOCUTUS_SET_SYSTEM_STATUS = False
    # *OR*, its default opposite is impled, namely, sub-command GET:
    ######
    # and the set value (default=False)
    LOCUTUS_SET_SYSTEM_STATUS_TO_VALUE = False
    #####################
    #####################
    # And now, the optional configurations:
    PROCESS_LOCUTUS_SYSTEM_STATUS = get_env_or_config_val(TYPE_BOOL, 'process_locutus_system_status', config, 'process_locutus_system_status', def_val=PROCESS_LOCUTUS_SYSTEM_STATUS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################
    if PROCESS_LOCUTUS_SYSTEM_STATUS:

        ##################### ##################### ##################### ##################### #####################
        ##################### ##################### ##################### ##################### #####################
        # In addition to PROCESS_LOCUTUS_SYSTEM_STATUS,
        # the above PROCESS_LOCUTUS_SYSTEM_STATUS settings are currently only configurable through the config.yaml via get_config_val()
        # but the following can also be overridden by Environment Variables,
        # via the combined calls to get_env_or_config_val():
        #####################
        #####################
        # for an overall status (if !USE_SYSTEM_STATUS_NODE), or a specific node (if USE_SYSTEM_STATUS_NODE):
        LOCUTUS_USE_SYSTEM_STATUS_NODE = get_env_or_config_val(TYPE_BOOL, 'locutus_use_system_status_node', config, 'locutus_use_system_status_node', def_val=LOCUTUS_USE_SYSTEM_STATUS_NODE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # and, if so node-enabled, the node name to GET or SET the corresponding System Status:
        LOCUTUS_USE_SYSTEM_STATUS_NODE_NAME = get_env_or_config_val(TYPE_NON_BOOL, 'locutus_use_system_status_node_name', config, 'locutus_use_system_status_node_name', def_val=LOCUTUS_USE_SYSTEM_STATUS_NODE_NAME, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # for an overall status (if !USE_SYSTEM_STATUS_MODULE), or a specific module (if USE_SYSTEM_STATUS_MODULE):
        LOCUTUS_USE_SYSTEM_STATUS_MODULE = get_env_or_config_val(TYPE_BOOL, 'locutus_use_system_status_module', config, 'locutus_use_system_status_module', def_val=LOCUTUS_USE_SYSTEM_STATUS_MODULE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # and, if so module-enabled, the module name to GET or SET the corresponding System Status:
        LOCUTUS_USE_SYSTEM_STATUS_MODULE_NAME = get_env_or_config_val(TYPE_NON_BOOL, 'locutus_use_system_status_module_name', config, 'locutus_use_system_status_module_name', def_val=LOCUTUS_USE_SYSTEM_STATUS_MODULE_NAME, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################  ##################### #####################
        # AND a corresponding SAFE_MODE (via ENABLE_DB_UPDATES) as the default, without any such WRITES:
        LOCUTUS_SYSTEM_STATUS_ENABLE_DB_UPDATES = get_env_or_config_val(TYPE_BOOL, 'locutus_system_status_enable_db_updates', config, 'locutus_system_status_enable_db_updates', def_val=LOCUTUS_SYSTEM_STATUS_ENABLE_DB_UPDATES, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # on which the following settings rely on being enabled for anything more than a safe dry-run test.
        #####################
        # WARNING: a writable System Status mode to set new values:
        LOCUTUS_SET_SYSTEM_STATUS = get_env_or_config_val(TYPE_BOOL, 'locutus_set_system_status', config, 'locutus_set_system_status', def_val=LOCUTUS_SET_SYSTEM_STATUS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        LOCUTUS_SET_SYSTEM_STATUS_TO_VALUE = get_env_or_config_val(TYPE_NON_BOOL, 'locutus_set_system_status_to_value', config, 'locutus_set_system_status_to_value', def_val=LOCUTUS_SET_SYSTEM_STATUS_TO_VALUE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        ##################### ##################### ##################### ##################### #####################
        ##################### ##################### ##################### ##################### #####################


    ####################################################
    ####################################################
    # Configs for the ONPREM_DICOM module:
    # including its source OnPrem-DICOM-Stage configuration of Orthanc Server, etc.
    # But first, an initial set of unknown defaults (in case subsequently overridden with environment variables):
    PROCESS_ONPREM_DICOM_IMAGES = False
    ONPREM_DICOM_STAGE_VAULT_PATH = "undefined_ONPREM_DICOM_STAGE_VAULT_PATH"
    LOCUTUS_ONPREM_DICOM_ZIP_DIR = "undefined_ZIP_DIR"
    LOCUTUS_ONPREM_DICOM_DEID_DIR = "undefined_PREDEID_DIR"
    LOCUTUS_ONPREM_DICOM_BUCKET_PATH_TOP_LEVEL = ".TopLevel"
    LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV = "undefined_LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST.csv"
    LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS = False
    LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS = False
    LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS = False
    LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED = False
    LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED = False
    #####
    #  NOTE: prior to 3/24/2025 LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE was absent ~= False == DICOMDIR (flat IMAGES/IM** structure)
    LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE = True
    #####
    #  NOTE: prior to 4/09/2025 LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP was absent ~= False == expanded/unaligned dicom_anon mode (redacting dates to 190101 rather than '', etc)
    LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP = True
    #####
    # WAS: LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE = False
    # NOTE: consider also changing this everywhere to an ENABLE so that even more by default it is automatically disabled:
    LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE = True
    # NOTE: moved the QC VAULT_PATH below the above QC_DISABLE to better indicate this dependency;
    # TODO: likewise swap the order of these 2x configs in the README; just after first committing its update
    # of the default locutus_onprem_dicom_deid_pause4manual_QC_disable to True (so as to better highlight)
    LOCUTUS_ONPREM_DICOM_MANUAL_DEID_QC_ORTHANC_CONFIG_VAULT_PATH = "undefined_MANUAL_DEID_QC_ORTHANC_CONFIG_VAULT_PATH"
    LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS = False
    LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC = False
    LOCUTUS_ONPREM_DICOM_SUBJECT_ID_PREFACE = ""
    #####################
    #####################
    # And now, the optional configurations:
    PROCESS_ONPREM_DICOM_IMAGES = get_env_or_config_val(TYPE_BOOL, 'process_onprem_dicom_images', config, 'process_onprem_dicom_images', def_val=PROCESS_ONPREM_DICOM_IMAGES, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    #####################
    # a ONPREM-DICOM-specific (as from GCP-DICOM) testing option to force the reprocessing of an accession numbers status record,
    # to allow easily re-running the same accession # for benchmarking purposes, etc.,
    LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS = get_env_or_config_val(TYPE_BOOL, 'locutus_debug_onprem_dicom_force_reprocess_accession_status', config, 'locutus_debug_onprem_dicom_force_reprocess_accession_status', def_val=LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
    # NOTE: moved ^^^^ UP HERE, rather than below, to be read by the Summarizer, even if *NOT* PROCESS_ONPREM_DICOM_IMAGES
    #####################
    if PROCESS_ONPREM_DICOM_IMAGES:
        ONPREM_DICOM_STAGE_VAULT_PATH = get_config_val(config, 'onprem_dicom_stage_config_vault_path', def_val=ONPREM_DICOM_STAGE_VAULT_PATH, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # and additional Locutus-specific configs around ONPREM-Dicom-Stage:
        LOCUTUS_ONPREM_DICOM_ZIP_DIR = get_config_val(config, 'locutus_onprem_dicom_zip_dir', def_val=LOCUTUS_ONPREM_DICOM_ZIP_DIR, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        LOCUTUS_ONPREM_DICOM_DEID_DIR = get_config_val(config, 'locutus_onprem_dicom_deidentified_dir', def_val=LOCUTUS_ONPREM_DICOM_DEID_DIR, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        ##################### ##################### ##################### ##################### #####################
        ##################### ##################### ##################### ##################### #####################
        # In addition to PROCESS_ONPREM_DICOM_IMAGES,
        # the above ONPREM_DICOM settings are currently only configurable through the config.yaml via get_config_val()
        # but the following can also be overridden by Environment Variables,
        # via the combined calls to get_env_or_config_val():
        #####################
        # S3/GS bucket path top level:
        LOCUTUS_ONPREM_DICOM_BUCKET_PATH_TOP_LEVEL = get_env_or_config_val(TYPE_NON_BOOL, 'locutus_onprem_dicom_bucket_path_top_level', config, 'locutus_onprem_dicom_bucket_path_top_level', def_val=LOCUTUS_ONPREM_DICOM_BUCKET_PATH_TOP_LEVEL, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # expected input manifests:
        # TODO: harmonize the following two key names across the config and env vars:
        LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV = get_env_or_config_val(TYPE_NON_BOOL, 'onprem_dicom_images_manifest', config, 'locutus_onprem_dicom_input_manifest_csv', def_val=LOCUTUS_ONPREM_DICOM_INPUT_MANIFEST_CSV, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # a ONPREM-DICOM-specific (as from GCP-DICOM) testing option to force the reprocessing of an accession numbers status record,
        # to allow easily re-running the same accession # for benchmarking purposes, etc.,
        # MOVED UP: LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS = get_env_or_config_val(TYPE_BOOL, 'locutus_debug_onprem_dicom_force_reprocess_accession_status', config, 'locutus_debug_onprem_dicom_force_reprocess_accession_status', def_val=LOCUTUS_ONPREM_DICOM_FORCE_REPROCESS_ACCESSION_STATUS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # NOTE: moved ^^^^ UP to be read by the Summarizer, even if *NOT* PROCESS_ONPREM_DICOM_IMAGES
        #####################
        # a ONPREM-DICOM-specific (as from GCP-DICOM) testing option to allow the pre-deletion of an accession numbers status record,
        # to allow easily re-running the same accession # for benchmarking purposes, etc.,
        LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS = get_env_or_config_val(TYPE_BOOL, 'locutus_debug_onprem_dicom_predelete_accession_status', config, 'locutus_debug_onprem_dicom_predelete_accession_status', def_val=LOCUTUS_ONPREM_DICOM_PREDELETE_ACCESSION_STATUS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        # a ONPREM-DICOM-specific (GCP-DICOM to have its own) to allow the pre-retire of an accession numbers manifest attributes,
        # by updating its accession_num to a negative of itself
        # to allow easily re-running the same accession # for benchmarking purposes, etc.,
        LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS = get_env_or_config_val(TYPE_BOOL, 'locutus_debug_onprem_dicom_preretire_accession_status', config, 'locutus_debug_onprem_dicom_preretire_accession_status', def_val=LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # and its counterpart to limit to only those with different manifest input attributes:
        LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED = get_env_or_config_val(TYPE_BOOL, 'locutus_debug_onprem_dicom_preretire_accession_status_only_changed', config, 'locutus_debug_onprem_dicom_preretire_accession_status_only_changed', def_val=LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        # and a related counterpart for allowing the continued processing (via Phase Sweep) of an accession number
        # when only some of its previously processed internal configurations have changed
        # (as might be useful when such a change of internal configs is relatively minor
        #  and does not necessitate reprocessing):
        # TODO: perhaps rename the following more appropriately to mention PHASE_SWEEP, if so specific?
        # or, at least further investigate how this differs from the above LOCUTUS_ONPREM_DICOM_PRERETIRE_ACCESSION_STATUS_ONLY_CHANGED....
        # as it might not be specific to PHASE_SWEEPS (?)
        # TODO: consider whether or not it needs to likewise be included in the GCP module.
        LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED = get_env_or_config_val(TYPE_BOOL, 'locutus_debug_onprem_dicom_allow_continued_processing_if_only_cfgs_changed', config, 'locutus_debug_onprem_dicom_allow_continued_processing_if_only_cfgs_changed', def_val=LOCUTUS_ONPREM_DICOM_ALLOW_CONTINUED_PROCESSING_IF_ONLY_CFGS_CHANGED, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #####################
        #  NOTE: prior to 3/24/2025 LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE was absent ~= False == DICOMDIR (flat IMAGES/IM** structure) = LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE
        LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE = get_env_or_config_val(TYPE_BOOL, 'locutus_dicom_use_zip_archive_structure', config, 'locutus_dicom_use_zip_archive_structure', def_val=LOCUTUS_ONPREM_DICOM_USE_ZIP_ARCHIVE_STRUCTURE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        #  NOTE: prior to 4/09/2025 LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP was absent ~= False == expanded/unaligned dicom_anon mode (redacting dates to 190101 rather than '', etc)
        LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP = get_env_or_config_val(TYPE_BOOL, 'locutus_onprem_dicom_alignment_mode_GCP', config, 'locutus_onprem_dicom_alignment_mode_GCP', def_val=LOCUTUS_ONPREM_DICOM_ANON_USE_ALIGNMENT_MODE_GCP, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        ###### ##### ##### ##### #####
        # Manual De-ID QC step (NOW Jenkins overrideable):
        LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE = get_config_val(config, 'locutus_onprem_dicom_deid_pause4manual_QC_disable', def_val=LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        LOCUTUS_ONPREM_DICOM_MANUAL_DEID_QC_ORTHANC_CONFIG_VAULT_PATH = get_config_val(config, 'locutus_onprem_dicom_manual_deid_QC_orthanc_config_vault_path', def_val=LOCUTUS_ONPREM_DICOM_MANUAL_DEID_QC_ORTHANC_CONFIG_VAULT_PATH, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        ##### #####
        # TODO: confirm that.... NOTE: the following apply only if the above LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE/locutus_onprem_dicom_deid_pause4manual_QC_disable is False (i.e., QC is enabled):
        # NOTE: changied that initial default LOCUTUS_ONPREM_DICOM_MANUAL_DEID_PAUSE4MANUAL_QC_DISABLE to True so that it is NOT automatically enabled
        LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS = get_env_or_config_val(TYPE_BOOL, 'locutus_onprem_dicom_use_manifest_QC_status', config, 'locutus_onprem_dicom_use_manifest_QC_status', def_val=LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC = get_env_or_config_val(TYPE_BOOL, 'locutus_onprem_dicom_use_manifest_QC_status_if_fail_remove_study_from_deidqc', config, 'locutus_onprem_dicom_use_manifest_QC_status_if_fail_remove_study_from_deidqc', def_val=LOCUTUS_ONPREM_DICOM_USE_MANIFEST_QC_STATUS_IF_FAIL_REMOVE_STUDY_FROM_DEIDQC, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        ##### #####
        LOCUTUS_ONPREM_DICOM_SUBJECT_ID_PREFACE = get_env_or_config_val(TYPE_NON_BOOL, 'locutus_onprem_dicom_subject_ID_preface', config, 'locutus_onprem_dicom_subject_ID_preface', def_val=LOCUTUS_ONPREM_DICOM_SUBJECT_ID_PREFACE, print_cfg_out=True, verbose=LOCUTUS_VERBOSE)
        ###### ##### ##### ##### #####
        ##################### ##################### ##################### ##################### #####################
        ##################### ##################### ##################### ##################### #####################


    ##################### ##################### ##################### ##################### #####################
    ##################### ##################### ##################### ##################### #####################

    ########################################## ########################################## #####################
    # Settings methods for other classes to enjoy:

    # an ASCII to Integer implementation to assist with any of the modules w/ LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS
    def atoi(s):
        # ascii to int, assuming unsigned base10, merely ignoring any other characters whilst building up the int,
        # e.g., "+-0abc123de45xy6z" -> 123456
        rtr=0
        for c in s:
            if ord(c) in range(ord('0'),ord('0')+10):
                rtr=rtr*10 + ord(c) - ord('0')
        return rtr


    # a minimal Integer to ASCII implementation to assist with any of the modules w/ LOCUTUS_DICOM_REMOVE_TEXT_FROM_INPUT_ACCESSIONS
    def itoa(num):
        return str(num)


    # set_Locutus_system_status():
    # modify the current DB-based Locutus system status, incoporating either overall or node-specific values.
    # UPDATE: added module-specific values as well, to allow setting any one at a time
    # if supplied, parmDBconnSession will reuse existing DB connections such as: LocutusDBconnSession
    # otherwise, if no parmDBconnSession, but parmDBconnectionString is supplied, a temporary DB connection will be opened and closed
    def set_Locutus_system_status(settings, new_sys_stat_val=False, use_Docker_node=False, node_name=None, use_module=False, module_name=None, db_updates_enabled=False, parmDBconnSession=None, parmDBconnectionString=None):
        # NOTE: the sys_status_set returned by here does not indicate the value set,
        # only whether or not this provided value WAS set (use get_Locutus_system_status() thereafter to confirm its set value)
        sys_table = settings.LOCUTUS_SYS_STATUS_TABLE
        sys_status_set=False
        status_msg='StubbyScrubbyDubDubby'

        SysDBconnSession = None
        if parmDBconnSession:
            SysDBconnSession = parmDBconnSession
        elif parmDBconnectionString:
            # work it with the new parmDBconnectionString:
            sysDicomDBengine = create_engine(parmDBconnectionString)
            SysDBconnSession = sysDicomDBengine.connect()
        else:
            sys_status_set=False
            status_msg='set_Locutus_system_status(new_sys_stat_val={0}) called with for sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}, w/ NEITHER a DBconnSession=[None] OR parmDBconnectionString=[REDACTED], one of which is currently required.'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, use_module, module_name, parmDBconnSession, parmDBconnectionString)
            # TODO: update the following to include module as well.... WAS: return (sys_status_set, status_msg)
            return (sys_status_set, status_msg, node_name)

        ##############
        # Q: only allow updates of system_status for nodes which already have defined settings?
        # Nahhhhh, allow this to create new such rows?  even if Jenkins might not fully be able to utilize directly,
        # can still create these from the cmd-line with a config.yaml, so yeah.... why not!
        ##############

        ##############
        # NOTE: if a node is specified, check if it already exists in the SYS STAT table
        # if so, do the UPDATE, and if not, do an INSERT, yeah?
        ##############
        # default WHERE clause to query empty set:
        sys_stat_where_clause='true=false'
        # and a default insert_cols, for a populated insert_vals to follow
        insert_or_update_sql=''
        insert_cols='{0}, {1}, {2}, {3}, {4}'.format(
            settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_COL, settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_NODE_COL,
            settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_DESC_COL, settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_DATETIME_COL,
            settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_ACTIVE_COL)
        insert_vals=''

        if use_Docker_node and use_module:
            # ERROR, sorry, currently equipped to handle but a single setting at a time;
            # NOTE: default to the use_module
            use_Docker_node = False

            # TODO:===<> EMIT a WARNING:
            #   only 1 can be set at a time;
            #   prioritizing module & ignoring node;
            #   may want to set just the node as well? No, as that could prove problematic across other nodes.
            print('WARNING: set_Locutus_system_status() supports setting 1 status at a time, '\
                'and found BOTH use_Docker_node & use_module set; ignoring node to prioritize module, '\
                'please set node separately', flush=True)
            # TODO: do consider supporting a dual-setting after all, as this could be mighty convenient.
            # At least with the capability of setting one at a time, though, we're good, so no hurries.

        if (not use_Docker_node) and (not use_module):
            node_name = settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_NODE_VAL_OVERALL
            sys_stat_where_clause='{0}=\'{1}\' AND {2}=\'{3}\''.format(
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_COL, settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_OVERALL,
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_NODE_COL, node_name)
            # build up parts for an "INSERT INTO sys_table (<insert_cols>) VALUES (insert_vals)"
            insert_vals='\'{0}\', \'{1}\', \'{2}\', now(), {3}'.format(
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_OVERALL,
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_NODE_VAL_OVERALL,
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_DESC_PREFIX,
                new_sys_stat_val)
        elif use_Docker_node:
            # use_Docker_node==True
            sys_stat_where_clause='{0}=\'{1}\' AND {2}=\'{3}\''.format(
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_COL, settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_PERNODE,
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_NODE_COL, node_name)
            # build up parts for an "INSERT INTO sys_table (<insert_cols>) VALUES (insert_vals)"
            insert_vals='\'{0}\', \'{1}\', \'{2}\', now(), {3}'.format(
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_PERNODE,
                node_name,
                '{0} for node {1}'.format(settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_DESC_PREFIX, node_name),
                new_sys_stat_val)
        elif use_module:
            # use_module==True
            sys_stat_where_clause='{0}=\'{1}\' AND {2}=\'{3}\''.format(
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_COL, settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_PER_MODULE,
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_NODE_COL, module_name)
            # build up parts for an "INSERT INTO sys_table (<insert_cols>) VALUES (insert_vals)"
            insert_vals='\'{0}\', \'{1}\',\'{2}\', now(), {3}'.format(
                settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_PER_MODULE,
                module_name,
                '{0} for module {1}'.format(settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_DESC_PREFIX, module_name),
                new_sys_stat_val)

        # NOTE: count for the expected number of records, whether for overall, or pernode
        #
        # TODO: wrap a try/catch around the following SysDBconnSession?
        sql_query='SELECT count(*) as num_node '\
                    'FROM {0} WHERE {1};'.format(sys_table, sys_stat_where_clause)
        #print('r3m0 DEBUG: set_Locutus_system_status MID01: about to sql_query={0}'.format(sql_query))

        count_sys_stat_node_result = SysDBconnSession.execute(sql_query)
        count_sys_stat_node_row = count_sys_stat_node_result.fetchone()
        curr_count_sys_stat_node = count_sys_stat_node_row['num_node']

        status_msg='r3m0:WIP=setting up set_Locutus_system_status(new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}) with count(sys_stat_node={3})=={6}'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, use_module, module_name, curr_count_sys_stat_node)
        if curr_count_sys_stat_node > 1:
            # > 1 node record == ERROR for get & set
            print('set_Locutus_system_status() ERROR: more than 1x SYS STAT record found for module/node={3} via (new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}) with count(sys_stat_node={3})=={6}'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, curr_count_sys_stat_node), flush=True)
            status_msg='ERROR: more than 1x SYS STAT record found for set_Locutus_system_status(new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}) with count(sys_stat_node={3})=={6}'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, use_module, module_name, curr_count_sys_stat_node)
        elif curr_count_sys_stat_node < 1:
            # < 1 node record == WARNING for set (ERROR for get)
            print('set_Locutus_system_status() WARNING: no SYS STAT record found for module/node={3} via (new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}) with count(sys_stat_node={3})=={6}; INSERTING (if LOCUTUS_SYSTEM_STATUS_ENABLE_DB_UPDATES)...'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, use_module, module_name, curr_count_sys_stat_node), flush=True)
            # INSERT: but ONLY if db_updates_enabled....
            if not db_updates_enabled:
                # well, okay, DON'T really do the INSERT:
                print('set_Locutus_system_status() **NOT** inserting new SYS STAT record in DB since **NOT** LOCUTUS_SYSTEM_STATUS_ENABLE_DB_UPDATES, for node={0}.'.format(
                    node_name), flush=True)
                status_msg='**NOT** INSERTING new SYS STAT record for set_Locutus_system_status(new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}) with count(sys_stat_node={3})=={6}'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, use_module, module_name, curr_count_sys_stat_node)
            else:
                # okay, really DO do it:
                status_msg='INSERTING new SYS STAT record for set_Locutus_system_status(new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}) with count(sys_stat_node={3})=={6}'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, use_module, module_name, curr_count_sys_stat_node)
                # TODO: wrap a try/catch around the following SysDBconnSession?
                ##############################
                insert_or_update_sql='INSERT into {0} '\
                            '( {1} ) '\
                            'VALUES ( {2} ) ;'.format(
                            sys_table,
                            insert_cols,
                            insert_vals)
                ##############################
                # THEN: set sys_status_set=True
        else:
            # GOOD! curr_count_sys_stat_node == 1
            #print('set_Locutus_system_status(): DEBUG: YAY 1x SYS STAT record found for node={3} via (new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}) with count(sys_stat_node={3})=={4}; UPDATING (if LOCUTUS_SYSTEM_STATUS_ENABLE_DB_UPDATES)...'.format(
            #        new_sys_stat_val, sys_table, use_Docker_node, node_name, curr_count_sys_stat_node), flush=True)
            # UPDATE: but ONLY if db_updates_enabled....
            if not db_updates_enabled:
                # well, okay, DON'T really do it:
                print('set_Locutus_system_status(): DEBUG: **NOT** updating existing SYS STAT record in DB since **NOT** LOCUTUS_SYSTEM_STATUS_ENABLE_DB_UPDATES, for node={0}.'.format(
                    node_name), flush=True)
                status_msg='**NOT** UPDATING new SYS STAT record for set_Locutus_system_status(new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}) with count(sys_stat_node={3})=={6}'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, use_module, module_name, curr_count_sys_stat_node)
            else:
                # okay, really DO do the UPDATE:
                status_msg='UPDATING SYS STAT record for set_Locutus_system_status(new_sys_stat_val={0}, sys_table={1}, for use_Docker_node={2}, node_name={3}, use_module={4}, module_name={5}) with count(sys_stat_node={3})=={6}'.format(
                    new_sys_stat_val, sys_table, use_Docker_node, node_name, use_module, module_name, curr_count_sys_stat_node)
                # TODO: wrap a try/catch around the following SysDBconnSession?
                ##############################
                insert_or_update_sql='UPDATE {0} '\
                            'SET {1}=\'{2}\', '\
                            '{3}=now() '\
                            'WHERE {4} ;'.format(
                            sys_table,
                            settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_ACTIVE_COL, new_sys_stat_val,
                            settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_DATETIME_COL,
                            sys_stat_where_clause)
                ##############################
                sys_status_set=True

        if insert_or_update_sql and len(insert_or_update_sql):
            print(settings.SQL_OUT_PREFIX, insert_or_update_sql, flush=True)
            SysDBconnSession.execute(insert_or_update_sql)
            status_msg='status set to {0}.'.format(new_sys_stat_val)


        if not parmDBconnSession and parmDBconnectionString:
            # NOTE: no DBconnSession passed in (e.g., via GCP using its own)
            # but parmDBconnectionString was passed in to create a temporary SysDBconnSession.
            # So.....
            SysDBconnSession.close()

        return (sys_status_set, status_msg, node_name)
        # end of set_Locutus_system_status()


# TODO: be sure to update ALL calls to get_Locutus_system_status from main_locutus and module_gcp,
# NOTING to swap the order of alt_Docker_node_name to after the check_Docker_name!
    # get_Locutus_system_status():
    # determine the current DB-based Locutus system status, incoporating both overall and node-specific values.
    # UPDATE: added module-specific values as well, to allow getting any one at a time, or even a cascade of all
    # if supplied, parmDBconnSession will reuse existing DB connections such as: LocutusDBconnSession
    # otherwise, if no parmDBconnSession, but parmDBconnectionString is supplied, a temporary DB connection will be opened and closed
    def get_Locutus_system_status(settings, check_Docker_node=True, alt_Docker_node_name=None, check_module=False, alt_module_name=None, parmDBconnSession=None, parmDBconnectionString=None):
        sys_table = settings.LOCUTUS_SYS_STATUS_TABLE
        node_name = 'overall'
        module_name = 'overall'
        sys_status=False
        status_msg=''

        #print('r3m0 DEBUG: HELLO from get_Locutus_system_status()')
        #print('r3m0 DEBUG: get_Locutus_system_status() called with for sys_table={1}, for '\
        #            'check_Docker_node={2}, node_name={3}, '\
        #            'check_module={4}, alt_module_name={5}, to replace default module_name={6}.'.format(
        #            False, sys_table, check_Docker_node, alt_Docker_node_name,
        #            check_module, alt_module_name, module_name), flush=True)

        if check_Docker_node and check_module:
            # ERROR, sorry, currently equipped to handle but a single setting at a time;
            # NOTE: default to the use_module
            check_Docker_node = False

            # TODO:===<> EMIT a WARNING:
            #   only 1 can currently be gotten at a time;
            #   prioritizing module & ignoring node;
            print('WARNING: get_Locutus_system_status() supports getting 1 status at a time, '\
                'and found BOTH check_Docker_node & check_module set; ignoring node to prioritize module, '\
                'please get node separately', flush=True)
            # TODO: do consider supporting a dual-getting after all, as this could be mighty convenient.
            # At least with the capability of getting one at a time, though, we're good, so no hurries.

        if check_Docker_node:
            if alt_Docker_node_name:
                node_name = alt_Docker_node_name
            else:
                node_name = os.environ.get('DOCKERHOST_HOSTNAME')
            #print('r3m0 DEBUG: get_Locutus_system_status(), w/ check_Docker_node={0} alt_Docker_node_name={1} => check node_name={2}'.format(
            #    check_Docker_node, alt_Docker_node_name, node_name), flush=True)
        if check_module:
            if alt_module_name:
                module_name = alt_module_name
            #print('r3m0 DEBUG: get_Locutus_system_status(), w/ check_module={0} alt_module_name={1} => check module_name={2}'.format(
            #    check_module, alt_module_name, module_name), flush=True)

        # more formally, the full active type
        curr_status_type = settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_PERNODE if check_Docker_node else settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_PER_MODULE
        # less formally for messages, an abbreviated active type
        curr_short_type = "mode" if check_Docker_node else "module" if check_module else "overall"
        # and the subtype, i.e., the specific node or module being checked:
        curr_status_subtype = node_name if check_Docker_node else module_name

        #print('r3m0 DEBUG: MID00 get_Locutus_system_status(), curr_status_type={0}, curr_status_subtype={1}'.format(
        #    curr_status_type, curr_status_subtype), flush=True)

        SysDBconnSession = None
        if parmDBconnSession:
            SysDBconnSession = parmDBconnSession
        elif parmDBconnectionString:
            # work it with the new parmDBconnectionString:
            sysDicomDBengine = create_engine(parmDBconnectionString)
            SysDBconnSession = sysDicomDBengine.connect()
        else:
            sys_status=False
            #status_msg='get_Locutus_system_status() called with for sys_table={0}, for check_Docker_node={1}, node_name={2}, w/ NEITHER a DBconnSession=[None] OR parmDBconnectionString=[REDACTED], one of which is currently required.'.format(
            #        sys_table, check_Docker_node, node_name, parmDBconnSession, parmDBconnectionString)
            status_msg='get_Locutus_system_status() called with for sys_table={1}, for check_Docker_node={2}, node_name={3}, check_module={4}, module_name={5}, w/ NEITHER a DBconnSession=[None] OR parmDBconnectionString=[REDACTED], one of which is currently required.'.format(
                    False, sys_table, check_Docker_node, alt_Docker_node_name, check_module, alt_module_name, parmDBconnSession, parmDBconnectionString)
            return (sys_status, status_msg, node_name)

        status_msg='r3m0:WIP=setting up get_Locutus_system_status(sys_table={0}, DBconnSession={1} & check_Docker_node={2}, node_name={3}, & check_module={4}, module_name={5})'.format(
                    sys_table, parmDBconnSession, check_Docker_node, node_name, check_module, alt_module_name)
        #print('r3m0 DEBUG: MID01 from get_Locutus_system_status(), status_msg={0}'.format(status_msg), flush=True)

        # TODO: wrap a try/catch around the following SysDBconnSession?
        overall_sys_stat_result = SysDBconnSession.execute('SELECT active '\
                                                        'FROM {0} WHERE status_type=\'{1}\'  '\
                                                        'AND status_node=\'system-wide\';'.format(sys_table, settings.LOCUTUS_SYS_STATUS_TABLE_STATUS_TYPE_VAL_OVERALL))
        overall_sys_stat_row = overall_sys_stat_result.fetchone()
        curr_overall_sys_status = overall_sys_stat_row['active']

        if not curr_overall_sys_status:
            status_msg='sys_table={0} shows Locutus overall status as {1}'.format(sys_table, curr_overall_sys_status)
            #print('r3m0 DEBUG: MID02a from get_Locutus_system_status(), status_msg={0}'.format(status_msg), flush=True)
        elif check_Docker_node or check_module:
            #print('r3m0 DEBUG: MID02b from get_Locutus_system_status() for check node OR module...', flush=True)
            curr_nodemodule_sys_status = False
            ####################
            # NOTE: start w/ a SELECT COUNT(*) to ensure that one and only one record exists for this node, if a node/module is set.
            #
            # TODO: wrap a try/catch around the following SysDBconnSession?
            sql_query = 'SELECT count(*) as num_node '\
                        'FROM {0} WHERE status_type=\'{1}\'  '\
                        'AND status_node=\'{2}\';'.format(
                                sys_table,
                                curr_status_type,
                                curr_status_subtype)
            #print('r3m0 DEBUG: MID02c from get_Locutus_system_status() for sql_query={0}...'.format(sql_query), flush=True)
            count_sys_stat_nodemodule_result = SysDBconnSession.execute(sql_query)
            count_sys_stat_nodemodule_row = count_sys_stat_nodemodule_result.fetchone()
            curr_count_sys_stat_nodemodule = count_sys_stat_nodemodule_row['num_node']

            if curr_count_sys_stat_nodemodule > 1:
                # > 1 node record == ERROR for get & set
                # NOTE: leaving sys_status=False
                #print('get_Locutus_system_status() ERROR: more than 1x SYS STAT record found for node or module via (sys_table={0}, for check_Docker_node={1}, node_name={2}, check_Docker_module={3}, module_name={4}) with count()=={5}'.format(
                #        sys_table, check_Docker_node, node_name, check_module, module_name, curr_count_sys_stat_nodemodule), flush=True)

                status_msg='ERROR: more than 1x SYS STAT record found for {0}={1}, count()=={2}'.format(
                        curr_short_type, curr_status_subtype, curr_count_sys_stat_nodemodule)
            elif curr_count_sys_stat_nodemodule < 1:
                # < 1 node record == ERROR for get (just a WARNING for set)
                # NOTE: leaving sys_status=False
                #print('get_Locutus_system_status() ERROR: no SYS STAT record found for node or module via (sys_table={0}, for check_Docker_node={1}, node_name={2}, check_Docker_module={3}, module_name={4}) with count()=={5}.'.format(
                #        sys_table, check_Docker_node, node_name, check_module, module_name, curr_count_sys_stat_nodemodule), flush=True)

                status_msg='ERROR: no SYS STAT record found for {0}={1}, count()=={2}; use set_Locutus_system_status() to create one'.format(
                        curr_short_type, curr_status_subtype, curr_count_sys_stat_nodemodule)
            else:
                # GOOD! curr_count_sys_stat_node == 1
                #print('get_Locutus_system_status(): DEBUG: YAY 1x SYS STAT record found for node={3} via (sys_table={0}, for check_Docker_node={1}, node_name={2}) with count(sys_stat_node={2})=={3}.'.format(
                #        sys_table, check_Docker_node, node_name, curr_count_sys_stat_node), flush=True)
                # NOTE: and, if active overall, check the node specifically:
                # TODO: wrap a try/catch around the following SysDBconnSession?

                sql_query = 'SELECT active '\
                            'FROM {0} WHERE status_type=\'{1}\'  '\
                            'AND status_node=\'{2}\';'.format(
                                    sys_table,
                                    curr_status_type,
                                    curr_status_subtype)
                #print('r3m0 DEBUG: MID03 from get_Locutus_system_status() for sql_query={0}...'.format(sql_query), flush=True)
                nodemodule_sys_stat_result = SysDBconnSession.execute(sql_query)
                if not nodemodule_sys_stat_result:
                    # NOTE: leaving sys_status=False
                    status_msg='{0} shows Locutus overall status as {1}, but NOTHING for node/module_name {2}, returning status {3}'.format(
                                            sys_table, curr_overall_sys_status, node_name, curr_nodemodule_sys_status)
                else:
                    # if a non-None node_sys_stat_result:
                    nodemodule_sys_stat_row = nodemodule_sys_stat_result.fetchone()
                    if not nodemodule_sys_stat_row:
                        # NOTE: leaving sys_status=False
                        status_msg='{0} shows Locutus overall status as {1}, but NOTHING for node/module_name {2}, returning status {3}'.format(
                                            sys_table, curr_overall_sys_status, node_name, curr_nodemodule_sys_status)
                    else:
                        curr_nodemodule_sys_status = nodemodule_sys_stat_row['active']
                        if not curr_nodemodule_sys_status:
                            # NOTE: leaving sys_status=False
                            # WAS: status_msg='{0} shows Locutus overall status as {1}, BUT that node_name {2} has status {3}'.format(
                            #                sys_table, curr_overall_sys_status, node_name, curr_node_sys_status)
                            # NOTE: for this VALID result, reset status_msg so that only the subsquent time appears as a concise msg
                            status_msg=''
                        else:
                            sys_status=True
                            # WAS: status_msg='{0} shows Locutus overall status as {1}, overall AND for node_name {2}'.format(
                            #                sys_table, curr_overall_sys_status, node_name, curr_node_sys_status)
                            # NOTE: for this VALID result, reset status_msg so that only the subsquent time appears as a concise msg
                            status_msg=''
            ####################
        else:
            # main status active, with no node queried
            #status_msg='{0} shows Locutus overall status as {1}, [NOTE: check_Docker_node={2}, not checking node specifically]'.format(
            #                    sys_table, curr_overall_sys_status, check_Docker_node)
            # NOTE: for this VALID result, reset status_msg so that only the subsquent time appears as a concise msg
            status_msg=''

            # Q: w/o a node provided, leave the status as False?
            # Nope, if only asking of the overall, then it is True
            # (only once the check_Docker_node is enabled shall that impact the returned status)
            sys_status=curr_overall_sys_status

        if not parmDBconnSession and parmDBconnectionString:
            # NOTE: no DBconnSession passed in (e.g., via GCP using its own)
            # but parmDBconnectionString was passed in to create a temporary SysDBconnSession.
            # So.....
            SysDBconnSession.close()

        nowtime = datetime.datetime.utcnow()
        #status_msg += " @ " + format(nowtime)
        status_msg += " @" + format(nowtime)

        #print('r3m0 DEBUG: GOODBYE from get_Locutus_system_status()')
        #print('r3m0 DEBUG: FOR curr_status_type={0}, curr_status_subtype={1}, '\
        #    'ABOUT TO RETURN sys_status={2}, status_msg={3}, curr_status_subtype={4}'.format(
        #    curr_status_type, curr_status_subtype, sys_status, status_msg, curr_status_subtype), flush=True)
        # r3m0 DEBUG newlines
        #print('\n\n', flush=True)

        return (sys_status, status_msg, curr_status_subtype)
        # end of get_Locutus_system_status()
