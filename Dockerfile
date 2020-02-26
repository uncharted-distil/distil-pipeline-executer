FROM registry.gitlab.com/datadrivendiscovery/images/core:ubuntu-bionic-python36-v2020.1.9

ENV PYTHONPATH=$PYTHONPATH:/app
ENV DEBIAN_FRONTEND=noninteractive

#  install common primitives
RUN pip3 install -e git+https://gitlab.com/datadrivendiscovery/common-primitives.git@47767f06bdcf2c7e3766b2a14c89cbcddb796a35#egg=CommonPrimitives

# install distil primitives
RUN pip3 install -e git+https://github.com/uncharted-distil/distil-primitives.git@82698e594a9b4b4cfae86bcab9a98ffc47c3e131#egg=DistilPrimitives

# copy the app
WORKDIR /app
COPY distil-pipeline-executer distil
COPY runner.py .

RUN mkdir /data
RUN mkdir /data/pipelines
RUN mkdir /data/datasets
RUN mkdir /data/predictions
RUN mkdir /data/outputs
ENV PIPELINE_DIR=/data/pipelines

COPY 22_handgeometry /data/pipelines/22_handgeometry

CMD ["/app/distil"]
