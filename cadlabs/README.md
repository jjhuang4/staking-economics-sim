# CADLabs

Dockerfile, entrypoint.sh, and test.sh are currently deprecated.

To setup for experimentation:
```
# With uv package manager
uv venv --python 3.10 .venv
# With base python
python -m venv .venv

# Windows
.venv\Scripts\Activate 
# Mac/Linux
source .venv/bin/activate

# Setup notebook environment to requirements.txt:
# With uv package manager
uv pip install --upgrade pip setuptools wheel
uv pip install -r requirements.txt

# With base python
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

For every notebook made inside of /cadlabs/notebooks include at the top:

```
import setup_path
import setup_templates
```

Which will enable you to import modules from within the ethereum-economic-model git submodule folder. Refer to this(documentation)[https://github.com/CADLabs/ethereum-economic-model/tree/main#Simulation-Experiments] to better understand where to approach.