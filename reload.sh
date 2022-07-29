#!/bin/bash
# vim:fileencoding=utf-8:ts=4:sw=4:sts=4:expandtab

# switch to current directory
cd $(dirname $0)

docker compose restart flask 
docker compose logs -f --tail 0 flask

