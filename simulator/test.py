import os
from importlib import import_module

if __name__ == "__main__":
    import_module("pool_tracker.accounting")
    import_module("simulator.behavior")
    import_module("simulator.equivocation_attack")
    import_module("simulator.live_dashboard_data")

    output_dir = os.getenv("SIM_OUTPUT_DIR", "output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "output.txt")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("Simulator container started successfully.\n")
        f.write("Dependencies are installed during image build, not at runtime.\n")
        f.write("Streamlit dashboard modules imported successfully.\n")
    print(f"Wrote smoke-test marker to {output_path}")
