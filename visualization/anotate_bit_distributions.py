#!/usr/bin/env python3

import pandas as pd
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Annotate CSV B with bit_distributions from CSV A')
    parser.add_argument('file_a', help='CSV file with bit_distribution column')
    parser.add_argument('file_b', help='CSV file to annotate')
    parser.add_argument('-o', '--output', help='Output file (default: annotated_<file_b>)')
    
    args = parser.parse_args()
    
    # Specification columns to match on
    spec_cols = ['vector_size', 'distinctiveness', 'overlap', 'filter_size', 'k', 'hash_function']
    
    print(f"Loading {args.file_a}...")
    df_a = pd.read_csv(args.file_a)
    
    print(f"Loading {args.file_b}...")
    df_b = pd.read_csv(args.file_b)
    
    # Verify columns exist
    missing_cols_a = [col for col in spec_cols + ['bit_distribution'] if col not in df_a.columns]
    missing_cols_b = [col for col in spec_cols if col not in df_b.columns]
    
    if missing_cols_a:
        print(f"ERROR: File A missing columns: {missing_cols_a}", file=sys.stderr)
        sys.exit(1)
    
    if missing_cols_b:
        print(f"ERROR: File B missing columns: {missing_cols_b}", file=sys.stderr)
        sys.exit(1)
    
    # Create lookup dictionary from file A
    print("Creating bit_distribution lookup...")
    df_a_lookup = df_a[spec_cols + ['bit_distribution']].drop_duplicates(subset=spec_cols)
    
    # Check for duplicates in file A specs
    if len(df_a_lookup) != len(df_a[spec_cols].drop_duplicates()):
        print("ERROR: File A has multiple different bit_distributions for same specs", file=sys.stderr)
        sys.exit(1)
    
    # Create tuple keys for lookup
    a_specs = set(tuple(row) for row in df_a_lookup[spec_cols].values)
    bit_dist_map = {tuple(row[:-1]): row[-1] for row in df_a_lookup.values}
    
    # Get unique specs from file B
    b_specs = set(tuple(row) for row in df_b[spec_cols].drop_duplicates().values)
    
    # Check for missing specs
    missing_specs = b_specs - a_specs
    if missing_specs:
        print(f"ERROR: {len(missing_specs)} specs in file B have no match in file A:", file=sys.stderr)
        for spec in list(missing_specs)[:5]:  # Show first 5
            print(f"  {dict(zip(spec_cols, spec))}", file=sys.stderr)
        if len(missing_specs) > 5:
            print(f"  ... and {len(missing_specs) - 5} more", file=sys.stderr)
        sys.exit(1)
    
    # Add bit_distribution column to file B
    print("Annotating file B with bit_distributions...")
    df_b['bit_distribution'] = df_b[spec_cols].apply(
        lambda row: bit_dist_map[tuple(row)], axis=1
    )
    
    # Determine output filename
    output_file = args.output or f"annotated_{args.file_b.split('/')[-1]}"
    
    print(f"Writing annotated data to {output_file}...")
    df_b.to_csv(output_file, index=False)
    
    print(f"Successfully annotated {len(df_b)} rows with bit_distributions")

if __name__ == "__main__":
    main()

