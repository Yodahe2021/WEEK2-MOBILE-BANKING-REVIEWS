import pandas as pd
import pytest
from scripts.preprocessing import *  # Import functions if modularized

def test_remove_duplicates():
    df = pd.DataFrame({'review': ['test', 'test'], 'bank': ['CBE', 'CBE']})
    assert len(df.drop_duplicates()) == 1