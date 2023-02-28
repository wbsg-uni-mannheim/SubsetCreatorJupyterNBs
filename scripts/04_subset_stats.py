import random
import re
import gzip
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool, Process, Manager
from tqdm import tqdm
import tldextract
import os
import pandas as pd

input_path = "/ceph/alebrink/WDC_Extraction_2022/9_c_schema_no_enc_issues_combined/"
#input_path = "C:/Users/alebrink/Documents/02_Research/WebDataCommons/extractions/2022/9_c_schema_no_enc_issues_combined/"
output_path = "/ceph/alebrink/WDC_Extraction_2022/9_c_schema_classspecific/"
#output_path = "C:/Users/alebrink/Documents/02_Research/WebDataCommons/extractions/2022/9_c_schema_classspecific/"
class_list_file = "/ceph/alebrink/WDC_Extraction_2022/schemaOrgclasses.txt"
class_list_file_pd = pd.read_csv(class_list_file, sep='\t', names=['type', 'filename'])
extraction = "2022-12"

def getstatsofsubset(subsetfile):
    quadcounter = 0
    distinct_urls = set()
    #distinct_domains = set()
    schema_dict = dict()
    size = os.path.getsize(input_path + subsetfile) / (1024 * 1024)
    current_chunk = 0
    chunk_lines = []
    domain_chunk_dict = {}
    domain_stats = {}
    schema_org_class = subsetfile.replace('.gz', '')
    count_pld_stats = False # Count stats for entity if applicable
    domain = None
    current_entity_id = None

    file_path = input_path + subsetfile
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            quadcounter += 1
            # line = line_.decode('utf8')
            if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in line):

                schema_type = line.split()[-3][1:-1]
                if (schema_type not in schema_dict):
                    schema_dict[schema_type] = 0

                schema_dict[schema_type] = schema_dict[schema_type] + 1

                url = line.split()[-2][1:-1]
                distinct_urls.add(url)
                domain_extract = tldextract.extract(url)
                domain = domain_extract.domain + '.' + domain_extract.suffix

                if domain not in domain_chunk_dict:
                    if len(chunk_lines) > 100000000:
                    #if len(chunk_lines) > 5000:
                        # Empty chunk lines and start new chunk
                        chunk_path = output_path + subsetfile.replace('.gz', '/') + 'part_{}.gz'.format(current_chunk)

                        if not os.path.exists(output_path + subsetfile.replace('.gz', '/')):
                            os.makedirs(output_path + subsetfile.replace('.gz', '/'))

                        # Write to file
                        with gzip.open(chunk_path, 'wt', encoding='utf-8') as f_chunk:
                            for line in chunk_lines:
                                f_chunk.write(line)

                        # Start new chunk
                        chunk_lines = []
                        current_chunk += 1

                    domain_chunk_dict[domain] = current_chunk

                if domain not in domain_stats:
                    domain_stats[domain] = {'quads': 0, 'entities': 0, 'schema_dict': {}}

                if schema_org_class == schema_type.split('/')[-1]:
                    domain_stats[domain]['entities'] += 1
                    count_pld_stats = True
                    current_entity = line.split()[0]
                    entity_schema_org_types = []  # Refresh schema_org type count for each entity!
                else:
                    count_pld_stats = False

            elif count_pld_stats and domain in domain_stats:
                schema_type = line.split()[1]
                entity = line.split()[0]

                if current_entity == entity:

                    if 'schema.org/' in schema_type:
                        schema_type = schema_type.split('schema.org/')[-1].replace('>', '')
                        if schema_type not in entity_schema_org_types:
                            entity_schema_org_types.append(schema_type)

                            if schema_type not in domain_stats[domain]['schema_dict']:
                                domain_stats[domain]['schema_dict'][schema_type] = 0

                            domain_stats[domain]['schema_dict'][schema_type] += 1

            # collect line for chunk
            chunk_lines.append(line)
            if domain is not None:
                domain_stats[domain]['quads'] += 1

    chunk_path = output_path + subsetfile.replace('.gz', '/') + 'part_{}.gz'.format(current_chunk)

    if not os.path.exists(output_path + subsetfile.replace('.gz', '/')):
       os.makedirs(output_path + subsetfile.replace('.gz', '/'))

    # Write remaining chunk lines to file --> Rerun code
    with gzip.open(chunk_path, 'wt', encoding='utf-8') as f_chunk:
        for line in chunk_lines:
            f_chunk.write(line)

    # Create look up file --> extract pld+tld+file_lookup
    lookup_file_path = output_path + subsetfile.replace('.gz', '') + '/' + subsetfile.replace('.gz', '_lookup.csv')
    with open(lookup_file_path, 'w') as lookup_file:
       lookup_file.write('pld,tld,file_lookup\n')
       for pld in domain_chunk_dict:
           tld = pld.split('.')[-1]
           chunk = 'part_{}.gz'.format(domain_chunk_dict[pld])
           lookup_file.write('{},{},{}\n'.format(pld, tld, chunk))

    # Create domain stats
    for domain in domain_stats:
        domain_stats[domain]['schema_dict'] = {k: v / domain_stats[domain]['entities'] for k, v in domain_stats[domain]['schema_dict'].items()}

    if not os.path.exists(output_path + subsetfile.replace('.gz', '')):
        os.makedirs(output_path + subsetfile.replace('.gz', ''))

    domain_stats_path = output_path + subsetfile.replace('.gz', '') + '/' + subsetfile.replace('.gz', '_domain_stats.csv')
    with open(domain_stats_path, 'w') as domain_stats_file:
        domain_stats_file.write('Domain\t#Quads of Subset\t#Entities of class\tProperties and Density\n')
        for domain in domain_stats:
            domain_stats_file.write('{}\t{}\t{}\t{}\n'.format(domain, domain_stats[domain]['quads'], domain_stats[domain]['entities'], domain_stats[domain]['schema_dict']))


    return (quadcounter, len(distinct_urls), len(domain_chunk_dict), schema_dict, size, subsetfile, current_chunk)


# %%

# Filter only relevant schema_org classes
relevant_files = ['{}.gz'.format(class_file_name) for class_file_name in class_list_file_pd['filename']]
files_ = [f for f in listdir(input_path) if isfile(join(input_path, f)) and '.gz' in f and f in relevant_files]
#print(files_)
# Filter files
#existingFiles = ['AdministrativeArea.gz', 'Airport.gz', 'Book.gz', 'City.gz', 'CollegeOrUniversity.gz', 'Continent.gz', 'Dataset.gz', 'Event.gz', 'GeoCoordinates.gz', 'GovernmentOrganization.gz', 'Hospital.gz', 'JobPosting.gz', 'LakeBodyOfWater.gz', 'LandmarksOrHistoricalBuildings.gz', 'Language.gz', 'Library.gz', 'LocalBusiness.gz', 'Mountain.gz', 'Movie.gz', 'Museum.gz', 'MusicAlbum.gz', 'MusicRecording.gz', 'Park.gz', 'Place.gz', 'RadioStation.gz', 'Restaurant.gz', 'School.gz', 'SkiResort.gz', 'SportsEvent.gz', 'SportsTeam.gz', 'StadiumOrArena.gz', 'TelevisionStation.gz']

#files_ = [f for f in files_ if f not in existingFiles]

html_stats = ""
pool = Pool(5)
#for file in files_:
#    result = getstatsofsubset(file)
for result in tqdm(pool.imap(func=getstatsofsubset, iterable=files_), total=len(files_)):
    filename = result[5]
    current_chunk = result[6] + 1
    schema_subset = filename.replace("schema_", "").replace(".gz", "")
    top_related_classes = "</td><td>"

    for k in sorted(result[3], key=result[3].get, reverse=True)[:5]:
        top_related_classes = top_related_classes + k + " (" + str(f"{result[3][k]:,}") + ")" + "</br>"

    # print(filename)
    # print(top_related_classes)
    # print("---------")
    size = round(result[4], 2)
    if size > 1024:
        size = round(size/1024, 2)
        txt_size = '{} GB'.format(size)
    else:
        txt_size = '{} MB'.format(size)

    html_stats_file = ("<tr><th><a href=\"http://schema.org/" + schema_subset + "\">" + schema_subset +
                       "</a></th><td> Quads: " + str(f"{result[0]:,}") + "</br> URLs: " + str(f"{result[1]:,}") +
                       "</br> Hosts: " + str(f"{result[2]:,}") + "</br>" + top_related_classes + "</td><td>" + txt_size
                        + "<br> ({})".format(current_chunk)
                       + "</td><td><a href=\"https://data.dws.informatik.uni-mannheim.de/structureddata/{}/quads/classspecific/".format(extraction) + filename.replace('.gz', '') + "\">" + filename.replace('.gz', '') + "</a> (<a href=\"https://data.dws.informatik.uni-mannheim.de/structureddata/{}/quads/classspecific/{}/".format(extraction, filename.replace('.gz', '')) + filename.replace(
                ".gz", "_sample.txt") + "\">sample</a>)</td>"+
                "<td> <a href=\"https://data.dws.informatik.uni-mannheim.de/structureddata/{}/quads/classspecific/{}/{}_lookup.csv\">lookup_file</a>".format(extraction, filename.replace('.gz', ''), filename.replace('.gz', '')) +
            "<br> <a href=\"https://data.dws.informatik.uni-mannheim.de/structureddata/{}/quads/classspecific/{}/{}_domain_stats.csv\">pld_stats_file</a>".format(extraction, filename.replace('.gz', ''), filename.replace('.gz', '')) +
        "</td>"
                       "</tr>\n")

    print(html_stats_file)
    html_stats += html_stats_file
