import sys
from pathlib import Path

# Resolve repository roots relative to this notebook helper.
notebook_path = Path(__file__).resolve()
cadlabs_root = notebook_path.parent.parent
repo_root = cadlabs_root.parent
if repo_root.name != "staking-economics":
    raise AssertionError(f"Bad repo layout: expected parent folder to be staking-economics, got {repo_root}")

possible_names = ["ethereum-economic-model", "ethereum-economic-models"]
submodule_path = None
for name in possible_names:
    candidate = repo_root / name
    if candidate.is_dir():
        submodule_path = candidate
        break

if submodule_path is None:
    raise FileNotFoundError(
        f"Could not locate ethereum economic model submodule under {repo_root}."
        f" Expected one of: {', '.join(possible_names)}"
    )

# Add the submodule and its key package locations to the search path.
def add_to_sys_path(path: Path) -> None:
    path_str = str(path)
    if path.exists() and path_str not in sys.path:
        sys.path.insert(0, path_str)

add_to_sys_path(submodule_path)
add_to_sys_path(submodule_path / "experiments")
add_to_sys_path(submodule_path / "experiments" / "templates")
add_to_sys_path(submodule_path / "model")
add_to_sys_path(submodule_path / "model" / "parts")
    
# 3. Now you can import directly from the submodule's folders
# from model.types import Stage
# import time_domain_analysis as time_domain_analysis
# import monte_carlo_analysis as monte_carlo_analysis
# import eth_price_sweep_analysis as eth_price_sweep_analysis