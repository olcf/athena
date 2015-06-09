#!/bin/bash

key="X"
if [[ $# > 0 ]]; then
  key=$1
fi

echo "Task compute-$key running on `/bin/hostname -f`"
