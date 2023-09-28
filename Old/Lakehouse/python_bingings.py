from deltalake import DeltaTable
from deltalake import write_deltalake
import pandas as pd

# write some data into a delta table
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"]})
df