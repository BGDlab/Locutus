<A NAME="top"></A>
# Locutus

<IMG SRC="./docs/images/Locutus_logo.png" WIDTH="400" HEIGHT="100" />

_last update: 06 November 2025_


The CHOP/UPenn Brain-Gene Development Lab ([BGD](https://www.bgdlab.org)), in partnership with CHOP's Translational Research Informatics Group ([TRiG](https://www.research.chop.edu/dbhi-translational-informatics)), is proud to present to you Locutus, our de-identification workflow framework. 

<IMG SRC="./docs/images/Locutus_waterfall_wLogo.png" WIDTH="700" HEIGHT="400" />

From the Latin word *locūtor* (“speaker, talker”), Locutus is a semi-automated processing workflow management system for modules and commands such as the following (as included in this reference repo):

    * OnPrem DICOM De-ID module
    * DICOM Summarizer command



## De-ID Transform Phase

<IMG SRC="./docs/images/phase04transform.png" WIDTH="600" HEIGHT="200" />

The key to the **OnPrem DICOM De-ID** module, as used to de-identify the DICOM metadata of clinical radiology for BGD's research, is  [dicom-anon](https://github.com/chop-dbhi/dicom-anon).

The following Python code snippet shows its integration from the **OnPrem DICOM De-ID** module:


>                dicom_anon_Popen_args = [
>                    'python3',
>                    './src_3rdParty/dicom_anon.py',
>                    '--spec_file',
>                    DEFAULT_DICOM_ANON_SPEC_FILE,
>                    '--modalities',
>                    DEFAULT_DICOM_ANON_MODALITIES_STR,
>                    '--force_replace',
>                    curr_replacement_patient_info,  # for 'R's in dicom_anon_spec_file
>                    '--exclude_series_descs',
>                    DICOM_SERIES_DESCS_TO_EXCLUDE,
>                    '{0}'.format(curr_uuid_id_images_path),
>                    '{0}'.format(deidentified_dirname)
>                ]
>
>                proc = Popen(dicom_anon_Popen_args, stdout=PIPE, stderr=PIPE)
>                (stdoutdata, stderrdata) = proc.communicate()



## **REFERENCE ONLY**

Please note that this is a _**reference snapshot**_ of Locutus, as from an internal repo at the Children's Hospital of Philadelphia Research Institute.  We include for your reference a sample Locutus module (**OnPrem DICOM De-ID**, as used to de-identify clinical radiology for BGD's research), and a sample Locutus command (**the Summarizer**, to assist in preloading and monitoring a batch of accessions for de-identification).

While we would very much like to offer a ready-to-play turnkey solution, there are many internal infrastructure dependencies that will currently require customization to integrate within your own infrastructure.

For example, an internal "TRiG Secrets Manager" package is still referenced by, though not included, in the Locutus code for this reference release. This package provides secure access to [Vault](https://www.hashicorp.com/en/products/vault)-based TRiG unified secrets that contain configurations and connection information for various Locutus components, including databases (within an instance of [Postgres](https://www.postgresql.org)) and our Research PACS (an instance of [Orthanc](https://www.orthanc-server.com)).
Please see the [Deploying Locutus](#deployment) section for further details.

Should you be interested in helping generalize and enhance Locutus to make it more plug-and-playable outside of our internal CHOP infrastructure, please reach out to us, at:
*  DL-locutus-support@chop.edu

## LICENSE INFO

This project is released under a Non-Commercial Research License. For commercial use, please contact us at DL-locutus-support@chop.edu for licensing terms.

Non-Commercial Research License
Copyright ©2025 The Children's Hospital of Philadelphia.

Permission is hereby granted, free of charge, to any person or organization to use, copy, modify, and distribute this software and associated documentation files (the “Software”), for academic, research, or educational purposes only, subject to the following conditions:

1. Attribution
Appropriate credit must be given to the authors in any use, publication, or derivative work of the Software.

2. Non-Commercial Use Only
The Software may not be used, in whole or in part, for commercial purposes, including but not limited to:
use in a product for sale,
use in a for-profit company’s operations,
use in services provided to customers for a fee.

3. Commercial Licensing
For commercial use, a separate license must be obtained from the copyright holder. Please contact:
	* DL-locutus-support@chop.edu

4. Warranty Disclaimer
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


<BR/>

----------------------------------------------------------------
<A NAME="for_more_detailed_dox"></A>
## FOR MORE DETAILS...


Detailed documentation and excerpts from the Children's Hospital of Philadelphia Research Institute internal repo for Locutus may be found in this `reference` branch of this BGD Lab repository, at:
* https://github.com/BGDlab/Locutus/tree/reference#detailed_dox



----------------------------------------------------------------

<A NAME="contact"></A>
## Contact Us

#### From the Brain-Gene Development Lab, the Translational Research Informatics Group, the Department of Biomedical Health Informatics, and all of the Children's Hospital of Philadelphia Research Institute, we would like to sincerely wish you a most productive time with Locutus.

Again, should you be interested in helping generalize and enhance Locutus to make it more plug-and-playable outside of our internal CHOP infrastructure, or just have some questions or feedback, please reach out to us, at: 
* DL-locutus-support@chop.edu

Thank you!
