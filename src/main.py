import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from queries.core import create_tables, insert_data

create_tables()
insert_data()