from d3m import runtime, cli
import logging
import argparse
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

    cli.handler(arguments, parser)

if __name__ == "__main__":
    main(sys.argv)
