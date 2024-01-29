#!/bin/bash
cd /home/casey/Repos/DrivingAbility/
export HSA_OVERRIDE_GFX_VERSION=10.3.0
venv/bin/python3.10 JobScheduler.py >> cron.log 2>&1