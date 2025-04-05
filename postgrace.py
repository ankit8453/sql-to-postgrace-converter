import re
import sys
import os
import argparse

def convert_mysql_to_postgresql(mysql_file_path, psql_file_path, verbose=False):
    """
    Convert a MySQL file to PostgreSQL compatible format
    """
    try:
        # Check if input file exists
        if not os.path.exists(mysql_file_path):
            raise FileNotFoundError(f"Input file {mysql_file_path} not found")
        
        # Read MySQL file
        with open(mysql_file_path, 'r') as mysql_file:
            mysql_queries = mysql_file.read()
        
        # Apply PostgreSQL specific transformations
        psql_queries = mysql_queries
        
        # Database object identifiers
        # 1. Convert backticks to double quotes (for identifiers)
        psql_queries = re.sub(r'`(.*?)`', r'"\1"', psql_queries)
        
        # Data types
        # 2. Replace AUTOINCREMENT with SERIAL
        psql_queries = re.sub(r'AUTO_?INCREMENT', 'SERIAL', psql_queries, flags=re.IGNORECASE)
        
        # 3. Replace INT with INTEGER
        psql_queries = re.sub(r'\bINT\b(?!\w)', 'INTEGER', psql_queries, flags=re.IGNORECASE)
        psql_queries = re.sub(r'\bINT\(\d+\)', 'INTEGER', psql_queries, flags=re.IGNORECASE)
        
        # 4. Replace MySQL TEXT types with PostgreSQL TEXT
        psql_queries = re.sub(r'\b(TINY|MEDIUM|LONG)TEXT\b', 'TEXT', psql_queries, flags=re.IGNORECASE)
        
        # 5. Change UNSIGNED (PostgreSQL doesn't support this directly)
        unsigned_pattern = re.compile(r'(\w+\s+)(.*?)(\s+UNSIGNED)', re.IGNORECASE)
        for match in unsigned_pattern.finditer(psql_queries):
            column_type = match.group(1).strip()
            column_name = match.group(2).strip()
            # Replace with CHECK constraint (this is a simplification, would need more context for best solution)
            replacement = f"{column_type} {column_name} CHECK ({column_name} >= 0)"
            psql_queries = psql_queries.replace(match.group(0), replacement)
        
        # 6. Convert BOOL type
        psql_queries = re.sub(r'\bTINYINT\(1\)', 'BOOLEAN', psql_queries, flags=re.IGNORECASE)
        
        # 7. MySQL datetime to PostgreSQL timestamp
        psql_queries = re.sub(r'\bDATETIME\b', 'TIMESTAMP', psql_queries, flags=re.IGNORECASE)
        
        # Functions and other syntax
        # 8. Replace IFNULL() with COALESCE()
        psql_queries = re.sub(r'IFNULL\((.*?),(.*?)\)', r'COALESCE(\1,\2)', psql_queries, flags=re.IGNORECASE)
        
        # 9. Replace LIMIT x,y with LIMIT y OFFSET x 
        psql_queries = re.sub(r'LIMIT\s+(\d+)\s*,\s*(\d+)', r'LIMIT \2 OFFSET \1', psql_queries, flags=re.IGNORECASE)
        
        # 10. Replace NOW() with CURRENT_TIMESTAMP
        psql_queries = re.sub(r'\bNOW\(\)', 'CURRENT_TIMESTAMP', psql_queries, flags=re.IGNORECASE)
        
        # 11. Replace SUBSTRING_INDEX with split_part
        psql_queries = re.sub(r'SUBSTRING_INDEX\(([^,]+),\s*([^,]+),\s*(\d+)\)', 
                            r'split_part(\1, \2, \3)', psql_queries, flags=re.IGNORECASE)
        
        # 12. GROUP_CONCAT to STRING_AGG
        psql_queries = re.sub(r'GROUP_CONCAT\(([^)]+)(?:\s+SEPARATOR\s+\'([^\']+)\')?\)', 
                            lambda m: f"STRING_AGG({m.group(1)}, '{m.group(2) or ','}')", 
                            psql_queries, flags=re.IGNORECASE)
        
        # 13. Convert MySQL boolean expressions
        psql_queries = re.sub(r'\bTRUE\b', 'true', psql_queries, flags=re.IGNORECASE)
        psql_queries = re.sub(r'\bFALSE\b', 'false', psql_queries, flags=re.IGNORECASE)
        
        # 14. REGEXP to ~
        psql_queries = re.sub(r'\sREGEXP\s', ' ~ ', psql_queries, flags=re.IGNORECASE)
        
        # 15. MySQL Engine Declarations (remove these as PostgreSQL doesn't use them)
        psql_queries = re.sub(r'ENGINE\s*=\s*\w+', '', psql_queries, flags=re.IGNORECASE)
        psql_queries = re.sub(r'DEFAULT CHARSET\s*=\s*\w+', '', psql_queries, flags=re.IGNORECASE)
        
        # 16. Handle MySQL comments
        psql_queries = re.sub(r'--\s', '-- ', psql_queries)  # Ensure space after --
        
        # Write to PostgreSQL file
        with open(psql_file_path, 'w') as psql_file:
            psql_file.write(psql_queries)
        
        if verbose:
            print(f"Successfully converted MySQL file {mysql_file_path} to PostgreSQL file {psql_file_path}")
        return True
    except Exception as e:
        print(f"Error during conversion: {e}")
        return False

def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description='Convert MySQL files to PostgreSQL compatible format')
    parser.add_argument('input_file', help='Path to the input MySQL file')
    parser.add_argument('-o', '--output', help='Path to the output PostgreSQL file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set output filename if not provided
    if not args.output:
        base_name = os.path.splitext(args.input_file)[0]
        args.output = f"{base_name}.psql"
    
    # Convert the file
    success = convert_mysql_to_postgresql(args.input_file, args.output, args.verbose)
    
    # Return status code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
