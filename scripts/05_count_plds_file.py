import os
import pandas as pd
from tqdm import tqdm
import gzip

dir_path = '/ceph/alebrink/01_projects/WDC_Extraction_2024/9_c_schema_classspecific'

def count_lines_in_file(filepath):
    """
    Counts the number of lines in a given file.

    Args:
        filepath (str): The path to the file.

    Returns:
        int: The total number of lines in the file.
    """
    line_count = 0
    with gzip.open(filepath, 'rt') as f:
        for _ in f:
            line_count += 1
    return line_count

plds = set()
quads = 0

for schema_org_class in tqdm(os.listdir(dir_path)):
    stats_path = f'/{dir_path}/{schema_org_class}/{schema_org_class}_domain_stats.csv'
    df = pd.read_csv(stats_path, sep='\t')

    #quads += df['#Quads of Subset'].sum()
    plds.update(df['Domain'].unique())
    schema_org_class_path = f'/{dir_path}/{schema_org_class}'
    for file_part in os.listdir(schema_org_class_path):
        if file_part.endswith('.gz'):
            file_path = f'{schema_org_class_path}/{file_part}'
            try:
                quads += count_lines_in_file(file_path)
            except:
                print(f'Unable to unpack: {file_path}')
output_file = '/ceph/alebrink/01_projects/WDC_Extraction_2024/counts_2.txt'
with open(output_file, 'w') as ofile:
    ofile.write(f'Quads: {quads}\n')
    ofile.write(f'PLDs: {len(plds)}')
#print(f'Quads: {quads}')
#print(f'PLDs: {len(plds)}')
