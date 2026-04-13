#!/usr/bin/env python3
"""
Script to run fab cloudlab_remote with different parameter combinations
"""

import os
import sys
import subprocess
import itertools
from pathlib import Path

# Parameter values
SIGMA = [1, 2, 5]
KAPPA = [1, 2, 3, 4]
REFERENCE = [1, 4, 7, 10]

# Other parameters (optional, can be customized)
DESIGN_TAG = 'manta_experiment2'
NETWORK_TAG = 'geo631'
LOAD_TAG = 'balanced_50_50'

def run_fab_command(sigma, kappa, reference):
    """
    Run: fab cloudlab_remote --sigma=X --kappa=Y --reference=Z
    """
    cmd = [
        'fab',
        'cloudlab_remote',
        f'--sigma={sigma}',
        f'--kappa={kappa}',
        f'--reference={reference}',
        f'--design_tag={DESIGN_TAG}',
        f'--network_tag={NETWORK_TAG}',
        f'--load_tag={LOAD_TAG}',
    ]
    
    print(f"\n{'='*80}")
    print(f"Running: {' '.join(cmd)}")
    print(f"Parameters: sigma={sigma}, kappa={kappa}, reference={reference}")
    print(f"{'='*80}\n")
    
    try:
        result = subprocess.run(cmd, cwd=os.path.join(os.path.dirname(__file__), '..', 'benchmark'))
        if result.returncode != 0:
            print(f"WARNING: Command failed with return code {result.returncode}")
            return False
        return True
    except Exception as e:
        print(f"ERROR: Failed to run command: {e}")
        return False

def main():
    """
    Generate all parameter combinations and run fab command for each
    """
    # Generate all combinations
    combinations = list(itertools.product(SIGMA, KAPPA, REFERENCE))
    total = len(combinations)
    
    print(f"\nTotal parameter combinations: {total}")
    print(f"sigma values: {SIGMA}")
    print(f"kappa values: {KAPPA}")
    print(f"reference values: {REFERENCE}")
    print(f"\n")
    
    successful = 0
    failed = 0
    
    for idx, (sigma, kappa, reference) in enumerate(combinations, 1):
        print(f"\n[{idx}/{total}] Running parameter set...")
        if run_fab_command(sigma, kappa, reference):
            successful += 1
        else:
            failed += 1
    
    print(f"\n{'='*80}")
    print(f"Summary:")
    print(f"  Total runs: {total}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"{'='*80}\n")
    
    return 0 if failed == 0 else 1

if __name__ == '__main__':
    sys.exit(main())
