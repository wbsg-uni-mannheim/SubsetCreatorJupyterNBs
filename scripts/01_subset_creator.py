from time import sleep

import pandas as pd
import gzip
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool, Process, Manager

import tldextract
from tqdm import tqdm
import os

# Load the class list file
#class_list_file = "/ceph/alebrink/01_projects/WDC_Extraction_2024/scripts/SubsetCreatorJupyterNBs/schemaOrgclasses.txt"
class_list_file = "scripts/SubsetCreatorJupyterNBs/schemaOrgclasses.txt"
class_list_file_pd = pd.read_csv(class_list_file, sep='\t', names=['type', 'filename'])
class_list_file_dict = pd.Series(class_list_file_pd.filename.values, index=class_list_file_pd.type).to_dict()


# Create the subsets
def createSubsets(input_path, file_path, output_path, queues_for_output):
    current_url = ""
    types_of_url = set()
    lines_of_url = []

    lines_to_append = dict()

    for schema_class in class_list_file_dict.keys():
        # Split by schema_class and first 2 letters of the PLD
        lines_to_append[schema_class] = dict()

    with gzip.open(input_path + file_path, 'rb') as file_:
        try:
            for line_ in file_.readlines():
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
                        domain_extract = tldextract.extract(current_url[1:-1])
                        domain_blocking_key = domain_extract.domain[:2]

                        if domain_blocking_key not in lines_to_append[type_of_url]:
                            lines_to_append[type_of_url][domain_blocking_key] = []
                        lines_to_append[type_of_url][domain_blocking_key].extend(lines_of_url)

                    types_of_url = set()
                    lines_of_url = []

                lines_of_url.append(line_)

                if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in line):
                    for schema_class in class_list_file_dict.keys():
                        for prot in ['http', 'https']:
                            if (
                                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + prot + "://" + schema_class + ">" in line):
                                types_of_url.add(schema_class)

                current_url = url

            # write the last
            for type_of_url in types_of_url:
                domain_extract = tldextract.extract(current_url[1:-1])
                domain_blocking_key = domain_extract.domain[:2]
                if domain_blocking_key not in lines_to_append[type_of_url]:
                    lines_to_append[type_of_url][domain_blocking_key] = []
                lines_to_append[type_of_url][domain_blocking_key].extend(lines_of_url)
        except Exception as e:
            print("Bad file")
            print(str(e) + " in file: " + file_path)
            return file_path

    # Write the lines to the correct queue
    for schema_class in class_list_file_dict.keys():
        for domain_blocking_key in lines_to_append[schema_class].keys():
            if class_list_file_dict[schema_class] in ['Organization']:
                # 2 queues for each class
                output_queue = queues_for_output[class_list_file_dict[schema_class] + str(int.from_bytes(domain_blocking_key.encode('utf-8'), 'big') % 25)]
            elif class_list_file_dict[schema_class] in ['Product', 'SearchAction', 'Person']:
                # 2 queues for each class
                output_queue = queues_for_output[class_list_file_dict[schema_class] + str(int.from_bytes(domain_blocking_key.encode('utf-8'), 'big') % 15)]
                #print("Using queue: ", class_list_file_dict[schema_class + str(int(domain_blocking_key) % 4)])
            elif class_list_file_dict[schema_class] in ['Answer', 'CreativeWork', 'Event', 'FAQPage', 'GeoCoordinates', 'LocalBusiness', 'Place', 'Question']:
                output_queue = queues_for_output[class_list_file_dict[schema_class] + str(int.from_bytes(domain_blocking_key.encode('utf-8'), 'big') % 5)]
            else:
                output_queue = queues_for_output[class_list_file_dict[schema_class]]

            output_file_path = output_path + class_list_file_dict[schema_class] + '/' + class_list_file_dict[schema_class] + '__' + domain_blocking_key + '.gz'
            output_queue.put((output_file_path, lines_to_append[schema_class][domain_blocking_key]))

    return file_path


def writeSubsetChunk(queue):
    while True:
        message = queue.get()
        if message == 'STOP':
            break

        file_path, lines = message

        # Write sorted lines to file
        with gzip.open(file_path, 'ab') as sorted_file:
            sorted_file.writelines(lines)
            sorted_file.flush()


if __name__ == "__main__":
    subset_class_path = ""
    #source_path = "/ceph/alebrink/01_projects/WDC_Extraction_2024/NQperFormat/"
    source_path = "01_Extraction/"
    formats = ['html-embedded-jsonld/', 'html-microdata/']
    #output = "/ceph/alebrink/01_projects/WDC_Extraction_2024/8_c_schema_no_enc_issues/"
    output = "01_Extraction/8_c_schema_no_enc_issues/"

    for key in class_list_file_dict:
        if not os.path.exists(output + class_list_file_dict[key]):
            os.makedirs(output + class_list_file_dict[key])

    # Asnychronous processing
    pool = Pool(3)
    manager = Manager()
    processes = []

    # Create different queues for two class
    queues = {}
    for i in range(0, len(class_list_file_dict)):
        schema_class = list(class_list_file_dict.values())[i]
        if schema_class in ['Organization']:
            # Create 25 queues for the schema classes
            for i in range(0, 25):
                queues[schema_class + str(i)] = manager.Queue(maxsize=20000)
                p = Process(target=writeSubsetChunk, args=(queues[schema_class + str(i)],))
                p.start()
                processes.append(p)
        elif schema_class in ['Product', 'SearchAction', 'Person']:
            # Create 15 queues for the schema classes
            for i in range(0, 15):
                queues[schema_class + str(i)] = manager.Queue(maxsize=20000)
                p = Process(target=writeSubsetChunk, args=(queues[schema_class + str(i)],))
                p.start()
                processes.append(p)
        elif schema_class in ['Answer', 'CreativeWork', 'Event', 'FAQPage', 'GeoCoordinates', 'LocalBusiness', 'Place', 'Question']:
            # Create 5 queues for the schema classes
            for i in range(0, 5):
                queues[schema_class + str(i)] = manager.Queue(maxsize=20000)
                p = Process(target=writeSubsetChunk, args=(queues[schema_class + str(i)],))
                p.start()
                processes.append(p)
        else:
            queues[schema_class] = manager.Queue(maxsize=20000)
            p = Process(target=writeSubsetChunk, args=(queues[schema_class],))
            p.start()
            processes.append(p)

    files_to_process = []
    for format in formats:
        files_to_process.extend([(source_path + format, f, output, queues)
                                 for f in listdir(source_path + format) if isfile(join(source_path, format, f))])

    print("Starting to process files!")
    for _ in pool.starmap(func=createSubsets, iterable=files_to_process):
        pass

    for queue in queues.values():
        queue.put('STOP')
    print("Done putting messages into the queues!")

    pool.close()
    pool.join()

    for p in processes:
        p.join()
        p.close()
    print("Done writing to files!")
