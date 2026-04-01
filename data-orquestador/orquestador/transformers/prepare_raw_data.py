if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    # Standardize column names only (lowercase, no spaces)
    data.columns = [col.lower().strip().replace(' ', '_') for col in data.columns]
    
    # Ensure basic types for loading
    data['source_year'] = data['source_year'].astype(int)
    data['source_month'] = data['source_month'].astype(int)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
