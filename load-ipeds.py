import sys
import pandas as pd

filename = sys.argv[1]

ipeds_df = pd.read_csv(filename)