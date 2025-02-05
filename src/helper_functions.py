import pandas as pd
import os
import yaml
from tempfile import TemporaryDirectory





def get_config(file: str):
    with open(file) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config