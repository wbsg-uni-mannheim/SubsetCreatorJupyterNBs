import logging
import os
import gzip
import time
import urllib
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool
from random import randint
from urllib.error import HTTPError, URLError

from tqdm import tqdm
import tldextract


def collect_plds(filepath):
    plds = set()
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as file_:
            for line in file_:
                # for line in file_.readlines():

                # Check for schema.org vocabulary
                if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in line):

                    schema_type = line.split()[-3][1:-1]

                    if 'http://schema.org/' in schema_type \
                            or 'https://schema.org/' in schema_type:
                        url = line.split()[-2][1:-1]
                        domain_extract = tldextract.extract(url)
                        domain = domain_extract.domain + '.' + domain_extract.suffix
                        plds.add(hash(domain))
    except Exception as e:
        print('Error: {}'.format(e))

    return plds


def retrieve_file_list(directory):
    # Retrieve all files in directory
    file_list = [join(directory, f) for f in listdir(directory) if isfile(join(directory, f))]
    return file_list


if __name__ == "__main__":

    file_list_paths = ['/ceph/alebrink/01_projects/WDC_Extraction_2024/NQperFormat/html-microdata',
                       '/ceph/alebrink/01_projects/WDC_Extraction_2024/NQperFormat/html-embedded-jsonld']

    pool = Pool(50)
    global_plds = set()
    for file_list_path in file_list_paths:
        print('Search in File List Path: {}'.format(file_list_path))
        global_domain_2_urls = {}

        file_list = retrieve_file_list(file_list_path)

        for plds in tqdm(pool.imap_unordered(func=collect_plds, iterable=file_list), total=len(file_list)):
            global_plds.update(plds)

    # Count domains & urls
    no_domains = len(global_plds)
    print("Found {} domains that use schema.org ".format(no_domains))
    print('')
