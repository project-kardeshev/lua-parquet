#!/bin/bash

# Set up Lua paths for LuaRocks modules
eval $(luarocks path)

# Create coverage directory if it doesn't exist
mkdir -p coverage

# Clean previous coverage data
echo "Cleaning previous coverage data..."
rm -f coverage/luacov.stats.out coverage/luacov.report.out

# Run the tests with coverage enabled
echo "Running tests with coverage enabled..."
busted -c

# If tests failed, show warning but continue with report
if [ $? -ne 0 ]; then
  echo "Warning: Some tests failed, but continuing with coverage report..."
fi

# Generate coverage report
echo "Generating coverage report..."
lua -e "require('luacov.runner').run_report()"

# Show coverage summary
echo "Coverage Summary:"
grep -A 10 "^Summary" coverage/luacov.report.out

echo -e "\nCoverage report is available in 'coverage/luacov.report.out'"
echo "To see coverage details for a specific module:"
echo "  grep -A 30 \"src/parquet/utils.lua\" coverage/luacov.report.out" 