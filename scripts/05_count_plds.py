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

def collect_urls_per_domain(filepath):
    domain_2_urls = {}
    response = None
    # retrieve file from server
    keep_requesting = True
    while keep_requesting:
        keep_requesting = False
        try:
            response = urllib.request.urlopen(filepath)
        except HTTPError as e:
            if e.code == 429:
                time.sleep(randint(1, 20))
                keep_requesting = True
            else:
                print('Error code: ', e.code)
        except URLError as e:
            # do something
            print('Reason: ', e.reason)

    # read file
    if response is not None:
        with gzip.open(response, 'rt', encoding='utf-8') as file_:
            for line in file_:
            #for line in file_.readlines():

                # Check for schema.org vocabulary
                if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in line):

                    schema_type = line.split()[-3][1:-1]

                    if 'http://schema.org/' in schema_type \
                        or 'https://schema.org/' in schema_type:

                        url = line.split()[-2][1:-1]
                        domain_extract = tldextract.extract(url)
                        domain = domain_extract.domain + '.' + domain_extract.suffix
                        domain_hash = hash(domain)
                        if domain_hash not in domain_2_urls:
                            domain_2_urls[domain_hash] = set()

                        domain_2_urls[domain_hash].add(hash(url))
    else:
        print('Response is none.')

    return domain_2_urls


def collect_plds(filepath):
    plds = set()
    response = None
    # retrieve file from server
    keep_requesting = True
    while keep_requesting:
        keep_requesting = False
        try:
            response = urllib.request.urlopen(filepath)
        except HTTPError as e:
            if e.code == 429:
                time.sleep(randint(1, 20))
                keep_requesting = True
            else:
                print('Error code: ', e.code)
        except URLError as e:
            # do something
            print('Reason: ', e.reason)

    # read file
    if response is not None:
        with gzip.open(response, 'rt', encoding='utf-8') as file_:
            for line in file_:
                # for line in file_.readlines():

                # Check for schema.org vocabulary
                if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in line):

                    schema_type = line.split()[-3][1:-1]

                    if 'http://schema.org/BreadcrumbList' in schema_type \
                            or 'https://schema.org/' in schema_type:

                        url = line.split()[-2][1:-1]
                        domain_extract = tldextract.extract(url)
                        domain = domain_extract.domain + '.' + domain_extract.suffix
                        plds.add(hash(domain))
    else:
        print('Response is none.')

    return plds


def retrieve_file_list(url):
    # Retrieve the file from the URL
    response = urllib.request.urlopen(url)
    file = response.read().decode()
    return file.splitlines()


if __name__ == "__main__":

    file_list_paths = ['https://webdatacommons.org/structureddata/2021-12/files/html-microdata.list',
                      'https://webdatacommons.org/structureddata/2022-12/files/html-embedded-jsonld.list',
                      'https://webdatacommons.org/structureddata/2022-12/files/html-microdata.list'
                      ]

    file_list_paths = ['https://webdatacommons.org/structureddata/2019-12/files/file.list',
                       'https://webdatacommons.org/structureddata/2018-12/files/file.list',
                       'https://webdatacommons.org/structureddata/2017-12/files/file.list',
                       'https://webdatacommons.org/structureddata/2016-10/files/file.list',
                       'https://webdatacommons.org/structureddata/2015-11/files/file.list'
                       ]

    pool = Pool(10)
    for file_list_path in file_list_paths:
        print('Search in File List Path: {}'.format(file_list_path))
        global_domain_2_urls = {}
        global_plds = set()
        file_list = retrieve_file_list(file_list_path)

        # for domain_2_urls in tqdm(pool.imap_unordered(func=collect_urls_per_domain, iterable=file_list), total=len(file_list)):
        # #for file_path in file_list:
        # #    domain_2_urls = collect_urls_per_domain(file_path)
        #     for domain in domain_2_urls:
        #         if domain not in global_domain_2_urls:
        #             global_domain_2_urls[domain] = set()
        #         global_domain_2_urls[domain].update(domain_2_urls[domain])
        #
        # # Count domains & urls
        # no_domains = len(global_domain_2_urls)
        # no_urls = sum([len(global_domain_2_urls[domain]) for domain in global_domain_2_urls])
        # avg_urls_per_domain = no_urls / no_domains
        # print("Found {} domains and {} urls that use schema.org --> avg. {} urls per domain ".format(no_domains, no_urls, avg_urls_per_domain))
        # print('')

        for plds in tqdm(pool.imap_unordered(func=collect_plds, iterable=file_list), total=len(file_list)):
        #for file_path in file_list:
        #    domain_2_urls = collect_urls_per_domain(file_path)
            global_plds.update(plds)

        # Count domains & urls
        no_domains = len(global_plds)
        print("Found {} domains use schema.org ".format(no_domains))
        print('')

