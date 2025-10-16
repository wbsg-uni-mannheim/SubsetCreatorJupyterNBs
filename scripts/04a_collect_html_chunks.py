# Collect html chunks
import os
import pandas as pd
dir_path = '/ceph/alebrink/01_projects/WDC_Extraction_2024/9_c_schema_classspecific'
html_output_path = '/ceph/alebrink/01_projects/WDC_Extraction_2024/page.html'
host_counts_path = '/ceph/alebrink/01_projects/WDC_Extraction_2024/host_counts.csv'

html_contents = []
host_counts = {}
for schema_org_dir in os.listdir(dir_path):
    schema_org_dir_path = f'{dir_path}/{schema_org_dir}'
    for file_name in os.listdir(schema_org_dir_path):
        if file_name.endswith('.html'):
            file_path = f'{schema_org_dir_path}/{file_name}'
            with open(file_path, 'r') as html_file:
                html_contents.append(html_file.read())

        if file_name.endswith('domain_stats.csv'):
            file_path = f'{schema_org_dir_path}/{file_name}'
            df = pd.read_csv(file_path, sep='\t')
            host_counts[schema_org_dir] = len(df)

# Write html
html_contents.sort()
with open(html_output_path, 'w') as output_file:
    output_file.writelines(html_contents)

# Write host counts
hosts = [host for host in host_counts]
hosts.sort()
with open(host_counts_path, 'w') as output_file:
    for host in hosts:
        output_file.write(f'{host}\t')
        output_file.write('{0:,d}\n'.format(host_counts[host]))
