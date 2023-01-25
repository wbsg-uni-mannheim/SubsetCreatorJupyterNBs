import pandas as pd
import gzip
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool, Process, Manager
from tqdm import tqdm
import os

subset_class_path = ""
class_list_file = "/ceph/alebrink/WDC_Extraction_2022/schemaOrgclasses.txt"
class_list_file_pd = pd.read_csv(class_list_file, sep='\t', names=['type', 'filename'])
class_list_file_dict = pd.Series(class_list_file_pd.filename.values, index=class_list_file_pd.type).to_dict()
input_path = "/ceph/alebrink/WDC_Extraction_2022/7_sorted_quads/html-microdata/"
files_json = [f for f in listdir(input_path) if isfile(join(input_path, f))]

output_path = "/ceph/alebrink/WDC_Extraction_2022/8_c_schema_microdata_full_no_enc_issues/"

for key in class_list_file_dict:
    os.makedirs(output_path+class_list_file_dict[key])


def createSubsets(filepath):
    current_url = ""
    types_of_url = set()
    lines_of_url = []

    lines_to_append = dict()

    for schema_class in class_list_file_dict.keys():
        lines_to_append[schema_class] = []

    with  gzip.open(input_path + filepath, 'r') as file_:
        for line_ in file_:
            try:
                line = line_.decode('utf8')
                url = line.split()[-2]

            except:
                print("Bad line")
                continue

            if url != current_url and current_url != "":
                # write lines
                for type_of_url in types_of_url:
                    class_file_name = class_list_file_dict.get(type_of_url)
                    lines_to_append[type_of_url].extend(lines_of_url)

                types_of_url = set()
                lines_of_url = []

            lines_of_url.append(line)

            if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in line):
                for schema_class in class_list_file_dict.keys():
                    for prot in ['http', 'https']:
                        if (
                                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + prot + "://" + schema_class + ">" in line):
                            types_of_url.add(schema_class)

            current_url = url

        # write the last
        for type_of_url in types_of_url:
            lines_to_append[type_of_url].extend(lines_of_url)

    for key in lines_to_append:
        class_file = gzip.open(
            output_path + class_list_file_dict[key] + "/" + class_list_file_dict[key] + "___" + filepath + ".gz", 'a')
        for enc_line in lines_to_append[key]:
            class_file.write(enc_line.encode())
        class_file.close()


pool = Pool(40)
for result in tqdm(pool.imap(func=createSubsets, iterable=files_json), total=len(files_json)):
    pass