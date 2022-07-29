#!/bin/bash
# vim:fileencoding=utf-8:ts=4:sw=4:sts=4:expandtab

# switch to current directory
cd $(dirname $0)

if [ "$1" == "" ]; then
    C=/bin/sh
else
    C=$@
fi

docker compose run flask $C



