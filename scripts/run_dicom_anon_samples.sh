#!/bin/bash
# run_dicom_anon_samples.sh:
# Sample run of dicom_anon for a few individual uuid unzipped ID'd files, with the following notes:
# * --null_modalities_allowed: required on some of my test files, otherwise some end up in quarantine
#	(and note that even though the files go all the way through, they have NOT been checked for PHI!!!)
# --modalities mr,ct,cr,ot: required to ensure that OT modalities are included along with the other defaults,
# id_dir   (/tmp/locutus/onprem-dicom/phase03_orthanc_ids): subject to change, from each individual phase03 uuid dir
# anon_dir (/tmp/locutus/onprem-dicom/phase04_dicom_deids): subject to change, to each individual phase04 uuid output dir
# 
# NOTE: testing with the one Study Description field to allow on through without being scrubbed, 
#	See below use of: --spec_file ./dicom_anon_spec_files/ext_keep_series_desc.dat
# NOTE: MOVED AWAY from dicom_anon's use of a local DB, identity.db
#	as used with self.audit_file for: --audit_file', type=str, default='identity.db', help='Name of sqlite audit file')
#	introduced an AUDITLESS mode into dicom_anon == --xclude_audit_file
#
# TODO: Introduce some error checking when calling dicom_anon to determine if any files have been quarantined,
#	and ways to move them back out of quarantine if applicable.
# TODO: determine how to best call these from the Locutus OnPremDicom module,
# 	and especially with such changes locally in the dicom_anon, 
#	may want JUST a local copy, rather than a linked repo dir?
#	Even if these changes do get merged back into the main branch,
#	it still might be easiest to call this locally, directly by locutus?
#	Doing exactly as such for now, via a Popen() call to ./dicom_anon.py
#
# NOTE: could consider grabbing an .egg from github: https://github.com/chop-dbhi/dicom-anon
# USES: snapshot up to latest known master branch commit, of Nov 30, 2017:
#	https://github.com/chop-dbhi/dicom-anon/commit/64fa7bb79e4d38abda2c3c3b60c45c2f7d452662

# NOTE: sample run for batch mode, of ALL phase03 uuid dirs to phase04:
# (without some of the latest cmdline args yet added here)
#./dicom_anon.py --null_modalities_allowed --modalities mr ct cr ot -- /tmp/locutus/onprem-dicom/phase03_orthanc_ids /tmp/locutus/onprem-dicom/phase04_dicom_deids

# NOW, testing the above with each individual dir, rather than one lump sum:
# AND NOTE: the initial phase04_dicom_deids dir did NOT exist before starting these!
# ==> dicom_anon is good about creating where necessary.


# TODO: could put these into a foreach loop, but for this quick test, here we go for now:
# TODO: also check to ensure that these ID paths DO exist!

echo "Running dicom_anon for uuid_2f9018ea-c4637cf8-1dbad893-b1efc1d9-45b35790..."
time ./dicom_anon.py --xclude_audit_file --spec_file ./dicom_anon_spec_files/ext_keep_series_desc.dat --null_modalities_allowed --modalities mr,ct,cr,ot /tmp/locutus/onprem-dicom/phase03_orthanc_ids/uuid_2f9018ea-c4637cf8-1dbad893-b1efc1d9-45b35790 /tmp/locutus/onprem-dicom/phase04_dicom_deids/uuid_2f9018ea-c4637cf8-1dbad893-b1efc1d9-45b35790

echo "Running dicom_anon for uuid_820ff3aa-cc952abe-7e888360-9fafc034-91e08a4c..."
time ./dicom_anon.py --xclude_audit_file --spec_file ./dicom_anon_spec_files/ext_keep_series_desc.dat --null_modalities_allowed --modalities mr,ct,cr,ot /tmp/locutus/onprem-dicom/phase03_orthanc_ids/uuid_820ff3aa-cc952abe-7e888360-9fafc034-91e08a4c /tmp/locutus/onprem-dicom/phase04_dicom_deids/uuid_820ff3aa-cc952abe-7e888360-9fafc034-91e08a4c


# and COULD, since the last one takes a bit longer (>30secs), just bail this test here:
# exit 0

echo "Running dicom_anon for uuid_0b47e92b-47ec306f-57c4c67d-ccca343e-7817d8f1..."
time ./dicom_anon.py --xclude_audit_file --spec_file ./dicom_anon_spec_files/ext_keep_series_desc.dat --null_modalities_allowed --modalities mr,ct,cr,ot /tmp/locutus/onprem-dicom/phase03_orthanc_ids/uuid_0b47e92b-47ec306f-57c4c67d-ccca343e-7817d8f1 /tmp/locutus/onprem-dicom/phase04_dicom_deids/uuid_0b47e92b-47ec306f-57c4c67d-ccca343e-7817d8f1

