# Dockerfile for building Locutus processing system
###########################################################
# START OF INTERNAL TRiG base-python-image:legacy
FROM python:3.11.0-slim-buster

RUN apt-get update -qq
RUN apt-get install -y unzip\
    zip\
    python-dev\
    vim\
    unixodbc \
    build-essential \
    unixodbc-dev 

RUN apt-get install -y \
            python-numpy\
            cython

RUN apt-get install -y git

# NOTE: Dockerfile text needs to be updated in order for the python dependencies to rebuild from source (not cache)
# Update the below ENV variable when CHOP internal github dependencies have updates pushed to the production branch
ENV LAST_UPDATE "2025-05-06"

# Cache python dependencies to optimize build times
RUN pip3 install "Cython"
RUN pip3 install "psycopg2-binary"
RUN pip3 install "PyYAML"
RUN pip3 install "pyodbc"
RUN pip3 install -U pip setuptools
# TODO: integrate with YOUR OWN local secrets manager, such as our INTERNAL TRiG secrets manager, as based around Vault:
#RUN pip3 install "hvac>=0.2,<0.3"
#RUN pip3 install "TrigSecretsManager"

# Add drivers source
ADD . /opt/app
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod 777 /usr/local/bin/docker-entrypoint.sh && ln -s /usr/local/bin/docker-entrypoint.sh 

ENTRYPOINT [ "docker-entrypoint.sh" ]
CMD /bin/bash
# END OF INTERNAL TRiG base-python-image:legacy
###########################################################

# installs from trig-dicom-stage:
RUN pip3 install "httplib2"
RUN pip3 install "requests>=2.18.4"

# for dicom-anon.py (now Python 3 compatible!):
########################
RUN pip3 install "pydicom"

# NOTE: include any cloud provider interface packages here
##########
# FOR EXAMPLE, with AWS targets, may want to use:
#RUN pip3 install "boto"
#RUN pip3 install "botocore"
#RUN pip3 install "s3transfer"
#RUN pip3 install "awscli"
##########
# FOR EXAMPLE, any Google components:
#RUN pip3 install "google"
#RUN pip3 install "google-cloud"
#RUN pip3 install "google-cloud-storage"
#RUN pip3 install "google-api-python-client"
##########

# MIME detection packages:
RUN pip3 install "python-magic"

RUN mkdir /opt/app ; exit 0
ADD . /opt/app

# Ensure all python requirements are met
RUN pip3 install -r /opt/app/requirements.txt

ENV APP_NAME=locutus

# running without buffered logs:
RUN export PYTHONUNBUFFERED=1

WORKDIR /opt/app
CMD ["/opt/app/run_main.sh"]
