#!/bin/bash
# run_main.sh
# NOTE: this is expected to be in the top-level of this application repository,
# as used by the eig-jenkins-playbook deployment script general_infa/deploy_etl.sh

cd /opt/app
python3 main_locutus.py
