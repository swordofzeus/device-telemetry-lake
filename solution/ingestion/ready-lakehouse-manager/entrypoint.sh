#!/bin/bash
set -e

exec uv run lakehouse-manager "$@"
