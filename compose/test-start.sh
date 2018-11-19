#!/bin/bash

# conda env update --file /opt/pygraphistry/environment2.yml
# conda env update --file /opt/pygraphistry/environment3.yml

source activate graphistry

pytest /opt/pygraphistry
