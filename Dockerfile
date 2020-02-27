FROM registry.gitlab.com/datadrivendiscovery/images/primitives:ubuntu-bionic-python36-v2020.1.9-20200212-063959

ENV PYTHONPATH=$PYTHONPATH:/app
ENV DEBIAN_FRONTEND=noninteractive

#  install common primitives
#RUN pip3 install -e git+https://gitlab.com/datadrivendiscovery/common-primitives.git@d9ee09a8838a222cead2a093d03c623603e175f9#egg=CommonPrimitives

# install distil primitives
RUN pip3 install -e git+https://github.com/uncharted-distil/distil-primitives.git@021eb4063c5982da2fafb666fe3b35b2df48293c#egg=DistilPrimitives

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
ENV DATASET_DIR=/data/datasets
ENV PREDICTION_DIR=/data/predictions

# need the resnet static file
RUN mkdir /data/static_resources
ENV D3MSTATICDIR=/data/static_resources
COPY 5c106cde386e87d4033832f2996f5493238eda96ccf559d1d62760c4de0613f8 /data/static_resources/5c106cde386e87d4033832f2996f5493238eda96ccf559d1d62760c4de0613f8

COPY 22_handgeometry /data/pipelines/22_handgeometry
COPY tattoo /data/pipelines/tattoo

CMD ["/app/distil"]
