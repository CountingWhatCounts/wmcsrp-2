import sys
from pipeline.cli import main

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("No steps specified. Running full pipeline...")
        sys.argv.extend(["--steps", "download", "preprocess", "load", "dbt"])
    main()