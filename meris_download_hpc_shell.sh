#!/bin/bash
# This is a shell script to activate a python env in NASA HECC system and then run a python script
# Users should edit this for their respective HPC workflows
source /usr/share/Modules/init/bash
module use -a /swbuild/analytix/tools/modulefiles
module load miniconda3/v4
export CONDA_ENVS_PATH=/nobackup/amulcan/scripts/meris_kl/
source activate meris

cd /nobackup/amulcan/scripts/meris_mml

python meris_download_hpc.py
