import gzip
import os
import pandas as pd
from multiprocessing import Pool, Process, Manager

import tldextract
from tqdm import tqdm
from os import listdir
from os.path import isfile, join

class_list_file = "/ceph/alebrink/01_projects/WDC_Extraction_2024/scripts/SubsetCreatorJupyterNBs/schemaOrgclasses.txt"
#class_list_file = "scripts/SubsetCreatorJupyterNBs/schemaOrgclasses.txt"
class_list_file_pd = pd.read_csv(class_list_file, sep='\t', names=['type', 'filename'])
all_subsets_names = class_list_file_pd.filename.values

def sortSubsetChunk(input_path, subset, subset_chunk_path, sorted_path):
    # Sorts files by pld+url+nodeid
    def getlinekey(elem):
        line = elem.decode('utf-8')
        url = line.split()[-2]
        domain_extract = tldextract.extract(url[1:-1])
        domain = domain_extract.domain + '.' + domain_extract.suffix
        sort_key = domain + '-' + line.split()[-2] + '-' + line.split()[0]
        return sort_key

    # Read file
    input_file_path = input_path + '/' + subset_chunk_path
    file_ = gzip.open(input_file_path, 'rb')

    lines = sorted(file_.readlines(), key=getlinekey)

    # Write sorted lines to file
    output_file_path = sorted_path + subset + '/' + subset_chunk_path
    with gzip.open(output_file_path, 'a') as sorted_file:
        sorted_file.writelines(lines)

if __name__ == "__main__":

    source_path = "/ceph/alebrink/01_projects/WDC_Extraction_2024/8_c_schema_no_enc_issues/"
    #source_path = "01_Extraction/8_c_schema_no_enc_issues/"
    sorted_path = "/ceph/alebrink/01_projects/WDC_Extraction_2024/8_c_schema_no_enc_issues_sorted/"
    #sorted_path = "01_Extraction/8_c_schema_no_enc_issues_sorted/"

    processed_subsets = []

    if not os.path.exists(sorted_path):
        os.makedirs(sorted_path)

    for subset in all_subsets_names:
        print("Processing subset: ", subset)
        if not os.path.exists(sorted_path + subset + '/'):
            os.makedirs(sorted_path + subset + '/')
        else:
            print("Subset already processed: ", subset)
            continue

        small_file_paths_to_process = []
        large_file_paths_to_process = []
        subset_path = source_path + subset + '/'
        subset_files = [f for f in listdir(subset_path) if isfile(join(subset_path, f))]
        for subset_file in subset_files:
            if subset_file.endswith('.gz'):
                subset_file_path = subset_path + subset_file
                if os.path.getsize(subset_file_path) < (1024 * 1024 * 1024): # 1GB
                    small_file_paths_to_process.append((subset_path, subset, subset_file, sorted_path))
                else:
                    large_file_paths_to_process.append((subset_path, subset, subset_file, sorted_path))

        # Sort large files first
        for large_file in large_file_paths_to_process:
            print("Processing large file: ", large_file)
            sortSubsetChunk(*large_file)

        # Process small files
        # Asynchronous processing
        pool_sorting = Pool(10)
        for _ in tqdm(pool_sorting.starmap(func=sortSubsetChunk, iterable=small_file_paths_to_process), total=len(small_file_paths_to_process)):
            pass

        pool_sorting.close()
        pool_sorting.join()