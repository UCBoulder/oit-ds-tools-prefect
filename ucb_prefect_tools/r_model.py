"""This module requires the library rpy2 to be installed to run. This is not listed as a requirement for the package,
to avoid a mandatory R installation to run the remaining tools."""

import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
from prefect import task


@task
def run_model(data: pd.DataFrame, model_path: str) -> pd.DataFrame:
    """Reads an R model from an RDS file and runs its associated predict method on a dataframe, returning the predictions"""

    with (ro.default_converter + pandas2ri.converter).context():
        r_dataframe = ro.conversion.get_conversion().py2rpy(data)

    with (ro.default_converter).context():
        glmnet = importr("glmnet")
        caret = importr("caret")
        stats = importr("stats")
        model = ro.r.readRDS(model_path)
        preds = stats.predict(model, r_dataframe)

    with (ro.default_converter + pandas2ri.converter).context():
        pred_dataframe = ro.conversion.get_conversion().rpy2py(preds)

    return pred_dataframe
