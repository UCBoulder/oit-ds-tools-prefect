import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri

import pandas as pd
from prefect import task, get_run_logger

@task
def run_model(data: pd.DataFrame, model_path:str) -> pd.DataFrame:
    """Reads an R model from an RDS file and runs its associated predict method on a dataframe, returning the predictions"""
    importr("tidyverse")
    importr("tidymodels")
    importr("magrittr")
    importr("arrow")
    importr("iml")
    importr("data.table")
    importr("caret")

    with (ro.default_converter + pandas2ri.converter).context():
        r_dataframe = ro.conversion.get_conversion().py2rpy(data)
        model = ro.r.readRDS(model_path)
        preds = ro.r.predict(model, r_dataframe)
        pred_dataframe = ro.conversion.get_conversion().rpy2py(preds)
    return pred_dataframe
