#!/usr/bin/env bash
# Energopoiei to project environment.
# Xrhsh:  source scripts/activate.sh
# Doulevei se bash kai zsh.

# Vriskoume to path tou script
if [ -n "${BASH_SOURCE[0]:-}" ]; then
    _script_src="${BASH_SOURCE[0]}"
elif [ -n "${(%):-%x}" ]; then
    _script_src="${(%):-%x}"
else
    # Fallback: ipothetoume oti to cwd einai to project root
    _script_src="./scripts/activate.sh"
fi

PROJECT_ROOT="$(cd "$(dirname "$_script_src")/.." && pwd)"
unset _script_src

# Java 17 (apaiteitai apo to Spark)
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"

# Python venv
source "$PROJECT_ROOT/venv/bin/activate"

# Spark / PySpark env hints
export PYSPARK_PYTHON="$PROJECT_ROOT/venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJECT_ROOT/venv/bin/python"

# Topiko Docker context
export DOCKER_CONTEXT="desktop-linux"

# To project root sto PYTHONPATH wste to "from arbitrage import ..." na doulevei pantou
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"

echo "Project environment activated"
echo "  Project: $PROJECT_ROOT"
echo "  Python:  $(python --version 2>&1)"
echo "  Java:    $(java -version 2>&1 | head -1)"
echo "  Docker:  context=$DOCKER_CONTEXT"
