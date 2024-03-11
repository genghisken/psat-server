#!/bin/bash
# Backup the ATLAS catalogue data to Kelvin. Use parallel to speed things up.
# Assumes night numbers (MJDs) in push_${CAMERA}_to_kelvin.txt file, which needs
# to be populated.

if [ $# -ne 2 ]
then
   echo "Usage: `basename $0` <camera> <threads>"
   exit 1
fi

export CAMERA=$1
export THREADS=$2

cat push_${CAMERA}_to_kelvin.txt | parallel -P${THREADS} /usr/bin/rsync -auvkKL --exclude="OLD/" --include="*/" --include "*.ddc" --include "*.nnc" --include "*.tph.bz2" --include "*.mom.bz2" --include "*.tpmom" --include "*.tpmom.bz2" --include "*.tpdiff" --include "*.tpdiff.bz" --include "*.difpar" --include "*.dcls.bz2" --exclude="*" /atlas/diff/$CAMERA/{} 3044170@dm.kelvin.alces.network:/users/3044170/atlas_archive/atlas/diff/$CAMERA
