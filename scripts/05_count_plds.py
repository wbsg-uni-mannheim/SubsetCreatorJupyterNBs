import logging
import os
import gzip
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool
from tqdm import tqdm
import tldextract

#domain_url_path = "/ceph/alebrink/WDC_Extraction_2022/10_domain_urls/"
#domain_url_path = "C:/Users/alebrink/Documents/02_Research/WebDataCommons/extractions/2022/11_domain_urls/"


def collect_urls_per_domain(filepath):
    domain_2_urls = {}

    # Read data
    with gzip.open(filepath, 'rt', encoding='utf-8') as file_:
        for line in file_.readlines():

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

    return domain_2_urls

    # # Prepare output path
    # output_file_path = filepath.replace('5_data_per_format', '11_domain_urls')
    #
    # if len(domain_2_urls) > 0:
    #     # Create summary if necessarry
    #     with gzip.open(output_file_path, 'wt', encoding='utf-8') as output_f:
    #         for domain in domain_2_urls:
    #             output_f.write('{}\t{}\n'.format(domain, list(domain_2_urls[domain])))
    # for domain in domain_2_urls:
    #     suffix_path = '{}/{}'.format(domain_url_path, domain.split('.')[-1])
    #     if not os.path.exists(suffix_path):
    #         # Create a new directory if it does not exist
    #         os.makedirs(suffix_path)
    #
    #     file_path = '{}/{}.txt'.format(suffix_path, domain)
    #     with open(file_path, 'a') as f:
    #         for url in domain_2_urls[domain]:
    #             f.write('{}\n'.format(url))


if __name__ == "__main__":
    # Sorts files by url+nodeid
    #input_path = "/ceph/alebrink/WDC_Extraction_2022/2_NQperFormat/{}/".format(format)
    input_path = "/ceph/alebrink/WDC_Extraction_2022/5_data_per_format"
    #input_path = "C:/Users/alebrink/Documents/02_Research/WebDataCommons/extractions/2022/5_data_per_format/"
    global_domain_2_urls = {}

    print(" Start Count")
    for format in os.listdir(input_path):
        input_path_format = '{}/{}/'.format(input_path, format)
        files_to_count_plds = [input_path_format + f for f in listdir(input_path_format) if isfile(join(input_path_format, f))]

        pool = Pool(40)
        for domain_2_urls in tqdm(pool.imap(func=collect_urls_per_domain, iterable=files_to_count_plds), total=len(files_to_count_plds)):
            for domain in domain_2_urls:
                if domain not in global_domain_2_urls:
                    global_domain_2_urls[domain] = set()
                global_domain_2_urls[domain].update(domain_2_urls[domain])

    # Count domains & urls
    no_domains = len(global_domain_2_urls)
    no_urls = sum([len(global_domain_2_urls[domain]) for domain in global_domain_2_urls])
    avg_urls_per_domain = no_urls / no_domains
    print("Found {} domains and {} urls that use schema.org --> avg. {} urls per domain ".format(no_domains, no_urls, avg_urls_per_domain))