#!/bin/bash -x

whoami

echo "Python dependencies: ${printenv python_dependencies}"


pwd
ls

# show dependency information
echo "=== Dependency Tree ==="
python3 -m pipdeptree -d -1 -l

echo "=== Installed Packages ==="
python3 -m pip list


# Execute the main command, passing all script arguments to it
exec "$@"