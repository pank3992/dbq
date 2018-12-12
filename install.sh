#!/bin/sh

cur_dir="$(pwd)"
source="${BASH_SOURCE[0]}"
owner=$(ls -l "$source" | awk 'NR==1 {print $3}')
cd "$( dirname "${BASH_SOURCE[0]}" )"

{
    sudo mkdir /var/log/dbq
    sudo chown -R "$owner" /var/log/dbq
    # unzip dbq.zip
    pip install .
} || {
    sudo rm -rf /var/log/dbq
    pip uninstall dbq
    echo "Roll back, Installation Failed"
}

# sudo rm -rf dbq
cd "$cur_dir"
