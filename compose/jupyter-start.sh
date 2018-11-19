#!/bin/bash

conda env update --file /opt/pygraphistry/environment2.yml
conda env update --file /opt/pygraphistry/environment3.yml

source activate graphistry

pip install -e /opt/pygraphistry

exec /usr/local/bin/start-notebook.sh $*
