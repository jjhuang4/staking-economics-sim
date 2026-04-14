# CADLabs

This folder contains the Docker setup for running the separate CADLabs Ethereum Economic Model environment.

## Purpose

- Keep the CADLabs model isolated from the custom simulator code and dependencies.
- Clone the upstream CADLabs repository at runtime.
- Create and reuse a dedicated virtual environment for the CADLabs install flow.

## Main Files

- `Dockerfile` defines the standalone CADLabs service image.
- `entrypoint.sh` clones or updates the upstream repo, manages the CADLabs virtualenv, and launches commands inside that environment.

## Notes

- This folder does not contain the CADLabs model source itself; the service clones it into its mounted runtime workspace.
- Use this service when you want the modular validator-economics model rather than the local custom simulator.
- Shared artifacts can be exchanged through `../shared`.
