import argparse
from pipeline.steps import download, preprocess, load, dbt_runner, clean

STEP_FUNCTIONS = {
    "download": download.run,
    "preprocess": preprocess.run,
    "load": load.run,
    "dbt": dbt_runner.run,
    "clean": clean.run,
}

def main():
    parser = argparse.ArgumentParser(description="Run data pipeline steps.")
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=STEP_FUNCTIONS.keys(),
        default=["download", "preprocess", "load", "dbt"],
        help="Steps to run in the pipeline"
    )
    args = parser.parse_args()

    for step in args.steps:
        STEP_FUNCTIONS[step]()

if __name__ == "__main__":
    main()