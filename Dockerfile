FROM registry.gitlab.com/datadrivendiscovery/images/core:ubuntu-bionic-python36-v2020.1.9

ENV PYTHONPATH=$PYTHONPATH:/app
ENV DEBIAN_FRONTEND=noninteractive

#  install common primitives
RUN pip3 install -e git+https://gitlab.com/datadrivendiscovery/primitives.git@902072b99c00a96771c1cd0b93532639c36942dc#egg=CommonPrimitives

# install distil primitives
RUN pip3 install -e git+https://github.com/uncharted-distil/distil-primitives.git@82698e594a9b4b4cfae86bcab9a98ffc47c3e131#egg=DistilPrimitives

# copy the app
WORKDIR /app
COPY distil-pipeline-executor distil
COPY runner.py

ENTRYPOINT ["/usr/local/bin/dumb-init", "--"]
CMD ["/app/distil"]
