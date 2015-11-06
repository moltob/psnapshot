#!/bin/sh

mkdir -p /volume1/backup/psnapshot
cd /volume1/backup/psnapshot

python3 -m virtualenv psenv
source psenv/bin/activate
pip install --upgrade https://github.com/moltob/psnapshot/archive/master.zip

mkdir flight7
psnapshot -l DEBUG /volume1/NetBackup/flight7 flight7
