from d3m import runtime, cli
from d3m.container import dataset
import logging
import argparse
import pathlib
import pickle
import os
import typing
import sys

def main(argv: typing.Sequence) -> None:

    # Fit and pickle a pipeline
    #
    # runtime
    # -v $D3MSTATICDIR
    # fit
    # -r $D3MINPUTDIR/22_handgeometry_MIN_METADATA/22_handgeometry_MIN_METADATA_problem/problemDoc.json
    # -i $D3MINPUTDIR/22_handgeometry_MIN_METADATA/TRAIN/dataset_TRAIN/datasetDoc.json
    # -p ./image.json
    # -s ./pipeline.d3m

    # Load a pickled pipeline
    #
    # runtime
    # -v $D3MSTATICDIR
    # produce
    # -t $D3MINPUTDIR/22_handgeometry_MIN_METADATA/TEST/dataset_TEST/datasetDoc.json
    # -f ./pipeline.d3m

    logging.basicConfig()

    logging.getLogger().setLevel(10)

    parser = argparse.ArgumentParser(prog='d3m', description="Run a D3M core package command.")
    cli.configure_parser(parser)
    arguments = parser.parse_args(argv[1:])

    if arguments.runtime_command == 'produce':
        fitted_pipeline = pickle.load(arguments.fitted_pipeline)
        dataset_uri = pathlib.Path(os.path.abspath(arguments.test_inputs[0])).as_uri()
        results = produce(fitted_pipeline, dataset_uri)
        output_predictions(pathlib.Path(arguments.output.name).parent.resolve(), results)
    else:
        cli.handler(arguments, parser)

def output_predictions(pred_path: str, results: runtime.Result):
    for output_key in results.values:
        if output_key.startswith('outputs.'):
            path = pathlib.Path(pred_path, '{}.csv'.format(output_key)).resolve()
            results.values[output_key].to_csv(path, index=False)

def produce(fitted_pipeline: runtime.Runtime, dataset_uri: str) -> runtime.Result:
    test_dataset = dataset.Dataset.load(dataset_uri)
    _, result = runtime.produce(
        fitted_pipeline, [test_dataset], expose_produced_outputs=True
    )
    if result.has_error():
        raise result.error
    return result

if __name__ == "__main__":
    main(sys.argv)
