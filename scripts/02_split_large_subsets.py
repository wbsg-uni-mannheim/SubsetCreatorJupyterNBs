import random

import gzip
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool, Process, Manager

import tldextract
import os

# Create the subsets
def splitSubsets(input_path, file_path, schema_class, key_length, queue):

    with gzip.open(input_path + file_path, 'rt', encoding='utf-8') as file_:
        try:
            current_domain_key = ""
            lines_of_domain = []
            for line in file_:
                try:
                    url = line.split()[-2]
                except:
                    print("Bad line")
                    continue

                domain_extract = tldextract.extract(url[1:-1])
                domain_blocking_key = domain_extract.domain[:key_length]

                # If the current domain key is empty, set it to the latest domain key.
                if current_domain_key == "":
                    current_domain_key = domain_blocking_key

                if domain_blocking_key != current_domain_key or len(lines_of_domain) > 10000000:

                    # Create the output file path
                    output_file_path = input_path + schema_class + '/' + schema_class + '__' + current_domain_key + '.gz'
                    queue.put((output_file_path, lines_of_domain))

                    lines_of_domain = []
                    current_domain_key = domain_blocking_key

                lines_of_domain.append(line)

            # Write the last lines
            output_file_path = input_path + schema_class + '/' + schema_class + '__' + current_domain_key + '.gz'
            queue.put((output_file_path, lines_of_domain))

        except Exception as e:
            print("Bad file")
            print(str(e) + " in file: " + file_path)
            return file_path

    return file_path


def writeSubsetChunk(queue):
    while True:
        message = queue.get()
        if message == 'STOP':
            break

        file_path, lines = message

        # Write sorted lines to file
        with gzip.open(file_path, 'at', encoding='utf-8') as sorted_file:
            sorted_file.writelines(lines)
            sorted_file.flush()


if __name__ == "__main__":
    # source_path = "01_Extraction/8_c_schema_no_enc_issues/"
    source_path = "/ceph/alebrink/01_projects/WDC_Extraction_2024/8_c_schema_no_enc_issues/"

    for schema_class in listdir(source_path):
        if schema_class == 'Person':
            continue
        print("Processing schema class: ", schema_class)
        key_length = 4
        while key_length <= 10:
            # Asnychronous processing
            pool = Pool(20)
            manager = Manager()
            processes = []

            large_files = []

            queues = {}
            queues_created = False
            queues_2_jobs = {}

            for f in listdir(source_path + schema_class):
                input_file_path = join(source_path, schema_class, f)
                if isfile(join(source_path, schema_class, f)) and f.endswith('.gz'):
                    if os.path.getsize(input_file_path) > (1024 * 1024 * 1024): # 1GB
                    # if os.path.getsize(input_file_path) > 1000000:  # 1MB
                        # Rename file
                        split_file_name = f"SPLIT_{f}"
                        split_file_path = join(source_path, schema_class, split_file_name)
                        os.rename(input_file_path, split_file_path)

                        # Create queues for writing
                        if not queues_created:
                            for i in range(100):
                                queues[schema_class + str(i)] = manager.Queue(maxsize=20000)
                                p = Process(target=writeSubsetChunk, args=(queues[schema_class + str(i)],))
                                p.start()
                                processes.append(p)

                                queues_2_jobs[i] = 0

                            queues_created = True

                        # Find the queue with the least number of jobs
                        min_queue = min(queues_2_jobs, key=queues_2_jobs.get)
                        queues_2_jobs[min_queue] += 1

                        #if os.path.getsize(split_file_path) > (6 * 1024 * 1024 * 1024):  # 6GB
                        # if os.path.getsize(split_file_path) > 2000000:  # 2 MB
                        #     very_large_files.append((source_path, schema_class + "/" + split_file_name, schema_class, key_length,
                        #                     queues[schema_class + str(random_number)]))
                        # else:
                        large_files.append((source_path, schema_class + "/" + split_file_name, schema_class, key_length,
                                        queues[schema_class + str(min_queue)]))

            if len(large_files) == 0:
                pool.close()
                pool.join()
                print("No large files found!")
                break

            # print(f"Starting to process {len(very_large_files)} very large files!")
            # for very_large_file in very_large_files:
            #     processed_file = splitSubsets(*very_large_file)
            #     print("Processed file: ", processed_file)
            #     # Delete the processed file
            #     os.remove(source_path + processed_file)

            print(f"Starting to process {len(large_files)} files!")
            random.shuffle(large_files)

            for processed_file in pool.starmap(func=splitSubsets, iterable=large_files):
                print("Processed file: ", processed_file)
                # Delete the processed file
                os.remove(source_path + processed_file)

            for queue in queues.values():
                queue.put('STOP')
            print("Done putting messages into the queues!")

            pool.close()
            pool.join()

            for p in processes:
                p.join()
                p.close()
            print(f"Done writing to files with key length {key_length}!")

            key_length += 1
