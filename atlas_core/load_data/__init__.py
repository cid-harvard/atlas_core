from .utilities import *
from .load_postgres import *

import logging

# Add new formatted log handler for data_import
logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s %(asctime)s.%(msecs)03d %(message)s",
    datefmt="%Y-%m-%d,%H:%M:%S"
)

logger = logging.getLogger('data_import')
