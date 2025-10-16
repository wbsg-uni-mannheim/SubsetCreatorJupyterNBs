import random
import re
import gzip
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool, Manager
from tqdm import tqdm
import tldextract
import os
import pandas as pd
import sys


#class_list_file = "/ceph/alebrink/01_projects/WDC_Extraction_2024/scripts/SubsetCreatorJupyterNBs/schemaOrgclasses.txt"
class_list_file = "/ceph/alebrink/01_projects/WDC_Extraction_2024/scripts/SubsetCreatorJupyterNBs/schemaOrgclasses_remaining.txt"
#output_path = "C:/Users/alebrink/Documents/02_Research/WebDataCommons/extractions/2022/9_c_schema_classspecific/"
#class_list_file = "scripts/SubsetCreatorJupyterNBs/schemaOrgclasses.txt"
class_list_file_pd = pd.read_csv(class_list_file, sep='\t', names=['type', 'filename'])
extraction = "2024-12"


def write_to_file_from_queue(queue):
    while True:
        message = queue.get()
        if message == 'STOP':
            break

        file_path, lines = message
        write_to_file(file_path, lines)


def write_to_file(file_path, lines):
    with gzip.open(file_path, 'wb') as f:
        f.writelines(lines)


def append_to_lookup_file(queue):
    # Appends the domain chunk dictionary to the lookup file
    while True:
        message = queue.get()
        if message == 'STOP':
            break

        lookup_file_path, domain_chunk_dict = message

        # Append to lookup file
        with open(lookup_file_path, 'a') as lookup_file:
            for found_pld in domain_chunk_dict:
                found_tld = found_pld.split('.')[-1]
                chunk = 'part_{}.gz'.format(domain_chunk_dict[found_pld])
                lookup_file.write('{},{},{}\n'.format(found_pld, found_tld, chunk))


def create_html_chunk(no_quads, no_urls, no_hosts, schema_dict, size, schema_org_class, current_chunk):
    current_chunk = current_chunk + 1
    top_related_classes = "</td><td>"

    for k in sorted(schema_dict, key=schema_dict.get, reverse=True)[:5]:
        top_related_classes = top_related_classes + k + " (" + str(f"{schema_dict[k]:,}") + ")" + "</br>"

    # print(filename)
    # print(top_related_classes)
    # print("---------")
    rounded_size = round(size, 2)
    if rounded_size > 1024:
        rounded_size = round(rounded_size / 1024, 2)
        txt_size = '{} GB'.format(rounded_size)
    else:
        txt_size = '{} MB'.format(rounded_size)

    html_stats_file = ("<tr><th><a href=\"http://schema.org/" + schema_org_class + "\">" + schema_org_class +
                       "</a></th><td> Quads: " + str(f"{no_quads:,}") + "</br> URLs: " + str(f"{no_urls:,}") +
                       "</br> Hosts: " + str(f"{no_hosts:,}") + "</br>" + top_related_classes + "</td><td>" + txt_size
                       + "<br> ({})".format(current_chunk)
                       + "</td><td><a href=\"https://data.dws.informatik.uni-mannheim.de/structureddata/{}/quads/classspecific/".format(
                extraction) + schema_org_class + "\">" + schema_org_class + "</a> (<a href=\"https://data.dws.informatik.uni-mannheim.de/structureddata/{}/quads/classspecific/{}/".format(
                extraction, schema_org_class) + schema_org_class + "_sample.txt" + "\">sample</a>)</td>" +
                       "<td> <a href=\"https://data.dws.informatik.uni-mannheim.de/structureddata/{}/quads/classspecific/{}/{}_lookup.csv\">lookup_file</a>".format(
                           extraction, schema_org_class, schema_org_class) +
                       "<br> <a href=\"https://data.dws.informatik.uni-mannheim.de/structureddata/{}/quads/classspecific/{}/{}_domain_stats.csv\">pld_stats_file</a>".format(
                           extraction, schema_org_class, schema_org_class) +
                       "</td>"
                       "</tr>\n")

    return html_stats_file

def append_domain_stats(queue):
    while True:
        message = queue.get()
        if message == 'STOP':
            break

        domain_stats_path, domain_stats  = message

        # Create domain stats
        for found_domain in domain_stats:
            domain_stats[found_domain]['schema_dict'] = {k: v / domain_stats[found_domain]['entities'] for k, v in
                                                         domain_stats[found_domain]['schema_dict'].items()}
        # Add domain stats to file
        with open(domain_stats_path, 'a') as domain_stats_file:
            for found_domain in domain_stats:
                domain_stats_file.write('{}\t{}\t{}\t{}\n'.format(found_domain, domain_stats[found_domain]['quads'],
                                                                  domain_stats[found_domain]['entities'],
                                                                  domain_stats[found_domain]['schema_dict']))

def getstatsofsubset(input_path, schema_org_class, output_path, queue, queue_lookup_file, queue_domain_stats):
    quadcounter = 0
    distinct_urls = set()
    last_url = ''

    schema_dict = dict()

    current_chunk = 0
    chunk_lines = []
    domain_chunk_dict = {}
    domain_stats = {}
    count_pld_stats = False # Count stats for entity if applicable

    dir_path = input_path + schema_org_class + '/'
    size = os.path.getsize(dir_path) / (1024 * 1024)

    if not os.path.exists(output_path + schema_org_class + '/'):
        os.makedirs(output_path + schema_org_class + '/')

    # Create look up file --> extract pld+tld+file_lookup
    lookup_file_path = output_path + schema_org_class + '/' + schema_org_class + '_lookup.csv'
    with open(lookup_file_path, 'w') as lookup_file:
       lookup_file.write('pld,tld,file_lookup\n')

    # Create domain stats file
    domain_stats_path = output_path + schema_org_class + '/' + schema_org_class + '_domain_stats.csv'
    with open(domain_stats_path, 'w') as domain_stats_file:
        domain_stats_file.write('Domain\t#Quads of Subset\t#Entities of class\tProperties and Density\n')

    for file in listdir(dir_path):
        file_path = dir_path + file
        if not isfile(file_path) or not file_path.endswith('.gz'):
            continue

        with gzip.open(file_path, 'rt', encoding='utf-8') as file_:
            for line in file_:
                quadcounter += 1

                split_line = line.split()

                url = split_line[-2][1:-1]

                if url != last_url:
                    last_url = url
                    distinct_urls.add(hash(url))
                    domain_extract = tldextract.extract(url)
                    domain = domain_extract.domain + '.' + domain_extract.suffix

                    # Write to chunk if domain changes and chunk size is larger than 100MB
                    if domain not in domain_chunk_dict.keys() and sys.getsizeof(chunk_lines) / (1024 * 1024) > 100:
                        # if sys.getsizeof(chunk_lines) / (1024 * 1024) > 100:

                        # Empty chunk lines and start new chunk
                        chunk_path = output_path + schema_org_class + '/' + 'part_{}.gz'.format(current_chunk)

                        # Add to queue
                        queue.put((chunk_path, chunk_lines))

                        # Start new chunk
                        chunk_lines = []
                        current_chunk += 1

                        # Add to lookup file & reset domain_chunk_dict
                        queue_lookup_file.put((lookup_file_path, domain_chunk_dict))
                        domain_chunk_dict = {}

                        # Append to domain stats file & reset domain_stats
                        queue_domain_stats.put((domain_stats_path, domain_stats))
                        domain_stats = {}

                    if domain not in domain_chunk_dict:
                        domain_chunk_dict[domain] = current_chunk

                    if domain not in domain_stats:
                        domain_stats[domain] = {'quads': 0, 'entities': 0, 'schema_dict': {}}

                if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in line):
                    schema_type = split_line[-3][1:-1]
                    if schema_type not in schema_dict:
                        schema_dict[schema_type] = 0

                    schema_dict[schema_type] = schema_dict[schema_type] + 1

                    if schema_org_class == schema_type.split('/')[-1]:
                        domain_stats[domain]['entities'] += 1
                        count_pld_stats = True
                        current_entity = split_line[0]
                        entity_schema_org_types = []  # Refresh schema_org type count for each entity!
                    else:
                        count_pld_stats = False

                if count_pld_stats:
                    schema_type = split_line[1]
                    entity = split_line[0]

                    if current_entity == entity:

                        if 'schema.org/' in schema_type:
                            schema_type = schema_type.split('schema.org/')[-1].replace('>', '')
                            if schema_type not in entity_schema_org_types:
                                entity_schema_org_types.append(schema_type)

                                if schema_type not in domain_stats[domain]['schema_dict']:
                                    domain_stats[domain]['schema_dict'][schema_type] = 0

                                domain_stats[domain]['schema_dict'][schema_type] += 1

                # collect line for chunk
                chunk_lines.append(line.encode('utf-8'))
                if domain is not None:
                    domain_stats[domain]['quads'] += 1

    chunk_path = output_path + schema_org_class + '/' + 'part_{}.gz'.format(current_chunk)

    # Write remaining chunk lines to file
    queue.put((chunk_path, chunk_lines))

    # Add to lookup file
    queue_lookup_file.put((lookup_file_path, domain_chunk_dict))

    # Append to domain stats file
    queue_domain_stats.put((domain_stats_path, domain_stats))

    # TODO: Fix usage of domain_chunk_dict <- ONLY A SUBSET!
    html_chunk = create_html_chunk(quadcounter, len(distinct_urls), len(domain_chunk_dict), schema_dict, size, schema_org_class, current_chunk)

    html_chunk_path = output_path + schema_org_class + '/' + 'chunk_{}.html'.format(current_chunk)
    with open(html_chunk_path, 'w') as html_file:
        html_file.write(html_chunk)

    return quadcounter, len(distinct_urls), len(domain_chunk_dict), schema_dict, size, schema_org_class, current_chunk


if __name__ == "__main__":
    #source_path = "01_Extraction/8_c_schema_no_enc_issues_sorted/"
    source_path = "/ceph/alebrink/01_projects/WDC_Extraction_2024/8_c_schema_no_enc_issues_sorted/"
    #target_path = "01_Extraction/9_c_schema_classspecific/"
    target_path = "/ceph/alebrink/01_projects/WDC_Extraction_2024/9_c_schema_classspecific/"

    html_stats = ""
    pool_processing = Pool(1)
    pool_writer = Pool(3)
    pool_writer_domain_stats = Pool(1)
    pool_writer_lookup_file = Pool(1)

    m = Manager()
    queue = m.Queue(maxsize=3)
    queue_domain_stats = m.Queue(maxsize=5)
    queue_lookup_file = m.Queue(maxsize=5)

    multiple_runs = [pool_writer.apply_async(write_to_file_from_queue, args=(queue,)) for _ in range(3)]
    multiple_runs_domain_stats = [pool_writer_domain_stats.apply_async(append_domain_stats, args=(queue_domain_stats,)) for _ in range(1)]
    multiple_runs_lookup_file = [pool_writer_lookup_file.apply_async(append_to_lookup_file, args=(queue_lookup_file,)) for _ in range(1)]

    # Filter only relevant schema_org classes
    relevant_schema_org_classes = [(source_path, class_file_name, target_path, queue, queue_lookup_file, queue_domain_stats) for class_file_name in class_list_file_pd['filename']]

    for result in tqdm(pool_processing.starmap(func=getstatsofsubset, iterable=relevant_schema_org_classes), total=len(relevant_schema_org_classes)):
        html_stats_file = create_html_chunk(**result)

        print(html_stats_file)
        html_stats += html_stats_file

    pool_processing.close()
    pool_processing.join()

    queue.put('STOP')
    queue_domain_stats.put('STOP')
    queue_lookup_file.put('STOP')

    while not queue.empty():
        # Wait for all messages to be processed
        pass

    while not queue_domain_stats.empty():
        # Wait for all domain stats messages to be processed
        pass

    while not queue_lookup_file.empty():
        # Wait for all lookup file messages to be processed
        pass

    pool_writer.close()
    pool_writer.join()

    pool_writer_domain_stats.close()
    pool_writer_domain_stats.join()

    pool_writer_lookup_file.close()
    pool_writer_lookup_file.join()

