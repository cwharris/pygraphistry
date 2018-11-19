FROM conda/miniconda3

COPY ./environment2.yml /opt/pygraphistry/environment2.yml
COPY ./environment3.yml /opt/pygraphistry/environment3.yml

RUN conda update -y conda
RUN conda env update --file /opt/pygraphistry/environment2.yml
RUN conda env update --file /opt/pygraphistry/environment3.yml

COPY ./compose/test-start.sh /usr/local/bin/test-graphistry-start.sh

CMD ["test-graphistry-start.sh"]
