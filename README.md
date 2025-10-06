# Locutus

<IMG SRC="./docs/images/Locutus_logo.png" WIDTH="400" HEIGHT="100" />

## **REFERENCE ONLY**

The CHOP/UPenn Brain-Gene Development Lab ([BGD](https://www.bgdlab.org)), in partnership with CHOP's Translational Research Informatics Group ([TRiG](https://www.research.chop.edu/dbhi-translational-informatics)), is proud to present to you Locutus, our de-identification workflow framework. 

Please note that this is a _**reference snapshot**_ of Locutus, as from an internal repo at the Children's Hospital of Philadelphia Research Institute.  We include for your reference a sample Locutus module (**OnPrem DICOM De-ID**, as used to de-identify clinical radiology for BGD's research), and a sample Locutus command (**the Summarizer**, to assist in preloading and monitoring a batch of accessions for de-identification).

While we would very much like to offer a ready-to-play turnkey solution, there are many internal infrastructure dependencies that will currently require customization to integrate within your own infrastructure.  For example, an internal "TRiG Secrets Manager" package that was developed for secure access to [Vault](https://www.hashicorp.com/en/products/vault)-based TRiG unified secrets is still referenced by, though not included, in the Locutus code for this reference release.   Within such secrets lie the configuration and connection information for our Locutus databases (within an instance of [Postgres](https://www.postgresql.org)) and our Research PACS (an instance of [Orthanc](https://www.orthanc-server.com)).

Should you be interested in helping generalize and enhance Locutus to make it more plug-and-playable outside of our internal CHOP infrastructure, please reach out to: williamsrms@chop.edu

## LICENSE INFO
🚧 COMING SOON 🚧


## Translate Phase

The key to the **OnPrem DICOM De-ID** module, as used to de-identify the DICOM metadata of clinical radiology for BGD's research, is:  [dicom-anon](https://github.com/chop-dbhi/dicom-anon).

The following Python code snippet shows its integration:

>                dicom_anon_Popen_args = [
>                    'python3',
>                    './src_3rdParty/dicom_anon.py',
>                    '--spec_file',
>                    DEFAULT_DICOM_ANON_SPEC_FILE,
>                    '--modalities',
>                    DEFAULT_DICOM_ANON_MODALITIES_STR,
>                    '--force_replace',
>                    curr_replacement_patient_info,  # for any 'R' specs (e.g., PatientsName & PatientID) in the dicom_anon_spec_file
>                    '--exclude_series_descs',
>                    DICOM_SERIES_DESCS_TO_EXCLUDE,
>                    '{0}'.format(curr_uuid_id_images_path),
>                    '{0}'.format(deidentified_dirname)
>                ]
>
>                proc = Popen(dicom_anon_Popen_args, stdout=PIPE, stderr=PIPE)
>                (stdoutdata, stderrdata) = proc.communicate()


Further excerpts from Children's Hospital of Philadelphia Research Institute internal repo for Locutus may be found in our reference branch, at: https://github.com/BGDlab/Locutus/tree/reference
