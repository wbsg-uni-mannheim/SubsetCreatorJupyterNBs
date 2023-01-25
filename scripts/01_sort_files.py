import os
import re
import gzip
from os import listdir
from os.path import isfile, join
from multiprocessing import Pool, Process, Manager
from tqdm import tqdm


def filterSort(filepath):
    def getlinekey(elem):
        return elem.split()[-2] + elem.split()[0]

    file_ = gzip.open(input_path + filepath)

    sorted_lines = sorted(file_.readlines(), key=getlinekey)

    sorted_file = gzip.open(sorted_path + filepath, 'wb')

    sorted_file.writelines(sorted_lines)

    sorted_file.close()


if __name__ == "__main__":
    # Sorts files by url+nodeid
    formats = ['html-embedded-jsonld', 'html-microdata']

    for format in formats:
        input_path = "/ceph/alebrink/WDC_Extraction_2022/2_NQperFormat/{}/".format(format)
        files_to_sort = [f for f in listdir(input_path) if isfile(join(input_path, f))]
        sorted_path = "/ceph/alebrink/WDC_Extraction_2022/7_sorted_quads/{}/".format(format)
        if not os.path.exists(sorted_path):
            # Create a new directory because it does not exist
            os.makedirs(sorted_path)

        pool = Pool(50)
        for _ in tqdm(pool.imap(func=filterSort, iterable=files_to_sort), total=len(files_to_sort)):
            pass