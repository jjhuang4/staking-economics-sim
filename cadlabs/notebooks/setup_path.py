import sys
import os

"""
The biggest headache with submodules is that 
the code inside ethereum-economic-models expects to be at the "top level." 
When you move it into a subfolder, its internal imports (like from model.types import ...) 
will break because Python now thinks the path is ethereum_economic_models.model.types.

.py file solution: Add the submodule folder to the PYTHONPATH globally.
.ipynb file solution: use this file (setup_path.py) and import at first cell of every notebook
"""

def init_paths():
    root = os.path.abspath(os.path.join(os.getcwd(), ".."))
    submodule = os.path.join(root, "ethereum-economic-models")
    
    paths = [root, submodule]
    for p in paths:
        if p not in sys.path:
            sys.path.insert(0, p)
            
# Call it immediately
init_paths()