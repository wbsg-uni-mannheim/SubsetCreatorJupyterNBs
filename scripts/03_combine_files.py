import os
import shutil
import pandas as pd
from multiprocessing import Pool, Process, Manager
from tqdm import tqdm
from os import listdir
from os.path import isfile, join



input_path_json = "/ceph/alebrink/WDC_Extraction_2022/8_c_schema_json_full_no_enc_issues/"
input_path_microdata = "/ceph/alebrink/WDC_Extraction_2022/8_c_schema_microdata_full_no_enc_issues/"
combined_path = "/ceph/alebrink/WDC_Extraction_2022/9_c_schema_no_enc_issues_combined/"
class_list_file = "/ceph/alebrink/WDC_Extraction_2022/schemaOrgclasses.txt"
class_list_file_pd = pd.read_csv(class_list_file, sep='\t', names=['type', 'filename'])
all_subsets_names = class_list_file_pd.filename.values

if not os.path.exists(combined_path):
    os.makedirs(combined_path)

def combineSubset(subset):
    filenames_json = [f for f in listdir(input_path_json+subset) if isfile(join(input_path_json+subset, f))]
    filenames_microdata = [f for f in listdir(input_path_microdata + subset) if isfile(join(input_path_microdata + subset, f))]
    #filenames = filenames_json + filenames_microdata
    with open(combined_path+subset+".gz", 'wb') as wfp:
        for fn in filenames_json:
            with open(input_path_json+subset+"/"+fn, 'rb') as rfp:
                shutil.copyfileobj(rfp, wfp)

        for fn in filenames_microdata:
            with open(input_path_microdata+subset+"/"+fn, 'rb') as rfp:
                shutil.copyfileobj(rfp, wfp)

pool = Pool(40)
for result in tqdm(pool.imap(func=combineSubset, iterable=all_subsets_names), total=len(all_subsets_names)):
    pass