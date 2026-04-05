if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import gc

@transformer
def transform(data, *args, **kwargs):
    # Rename columns in place - no copy created
    data.columns = [col.lower().strip().replace(' ', '_') for col in data.columns]

    # Cast all int columns at once using a dict - single pass
    int_cols = {col: 'int32' for col in ['source_year', 'source_month'] 
                if col in data.columns}
    data = data.astype(int_cols)

    # Downcast numeric columns to save memory
    float_cols = data.select_dtypes(include='float64').columns
    data[float_cols] = data[float_cols].astype('float32')

    gc.collect()
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
