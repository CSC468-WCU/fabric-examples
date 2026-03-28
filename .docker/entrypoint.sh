#!/usr/bin/env bash

chmod 744 $HOME/.fabric/
chmod 744 $HOME/.ssh/

chmod 600 $HOME/.fabric/fabric_rc

chmod 600 $HOME/.ssh/slice_key
chmod 644 $HOME/.ssh/slice_key.pub

chmod 600 $HOME/.ssh/fabric-bastion-key
chmod 644 $HOME/.ssh/fabric-bastion-key.pub

source $HOME/.fabric/fabric_rc

cd /app 

jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token='' --ServerApp.password=''