from collections import Counter

from flashtext import KeywordProcessor
import json
import os
from itertools import islice
import tqdm

data_path = 'docs_json_objects/data'
keywords_to_search_path = 'keywords_lists'
indexed_docs_objects_path = 'indexed_docs_objects/docs_object'

def index_document_object(docs_json_object_number_to_open, keywords_dict_name = 'keywords.json'):
    '''
    This function searches the dictionary of keywords in the subset of the
    documents and save the results to indexed_docs_objects/docs_objects folder.

    :param
    docs_json_objects_number_to_open: int
    keywords_dict_name: (str) name of the keywords dict json file
    :return:
    '''

    # Open the docs json object
    list_of_docs_json_objects = os.listdir(data_path)
    docs_object_to_open = list_of_docs_json_objects[docs_json_object_number_to_open]
    number_of_indexed_object = docs_object_to_open.split('.')[0].split('_')[-1]
    print('Indexing the docs object', docs_object_to_open)

    if docs_object_to_open.split('.')[-1] == 'json':
        docs_object = json.load(open(os.path.join(data_path, docs_object_to_open), 'r'))
    else:
        return None



    # Open the keywords list
    keywords_dict = json.load(open(os.path.join(keywords_to_search_path, keywords_dict_name), 'r'))

    # Clean the keywords list by removing all the whitespaces
    # Start searching the documents and add the index.
    keywords_to_pmid_sub_list = []
    for keywords_dict_chunk in tqdm.tqdm(chunks(keywords_dict, SIZE=100000), total=int(round(len(list(keywords_dict.keys())))/100000)):
        keywords_dict_reformatted = {}
        for key, values in keywords_dict_chunk.items():
            keywords_dict_reformatted[key.strip()] = []
            for val in values:
                keywords_dict_reformatted[key.strip()].append(val.strip())

        # Make the flash-text object to search the data
        keyword_finder = KeywordProcessor()
        keyword_finder.add_keywords_from_dict(keywords_dict_reformatted)

        # Start searching the documents and add the index.
        keywords_to_pmid_sub = __search_in_documents__(docs_object, keyword_finder, keywords_dict_reformatted)
        keywords_to_pmid_sub_list.append(keywords_to_pmid_sub)

    keywords_to_pmid = {}
    for sub_dict in keywords_to_pmid_sub_list:
        for k, v in sub_dict.items():
            if len(list(v.keys())) > 0:
                keywords_to_pmid[k] = v

    # add the keywords folder if it doesn't exist
    if not os.path.isdir(f"indexed_docs_objects/docs_object/{keywords_dict_name.split('.')[0]}"):
        os.makedirs(f"indexed_docs_objects/docs_object/{keywords_dict_name.split('.')[0]}")

    # Save the keywords to pmid indexed
    json.dump(keywords_to_pmid,
              open(f"indexed_docs_objects/docs_object/{keywords_dict_name.split('.')[0]}/indexed_docs_{number_of_indexed_object}.json", 'w'))


def __search_in_documents__(docs_object, keyword_finder, keywords_dict):
    ## Make a keywords to pmid mentioning the keywords dict
    keywords_to_pmid = {}
    for key, _ in keywords_dict.items():
        keywords_to_pmid[key] = {}
    ## Iterate over docs
    for key, values in docs_object.items():
        pmid = key
        for k, v in values.items():
            if 'text_' in k:
                length_of_text = len(v.split(' '))
                length_of_character = len(v)
                text = v
                occurrence_of_keywords = keyword_finder.extract_keywords(text, span_info=True)
                occurrence_of_keywords_without_span_info = [i[0] for i in occurrence_of_keywords]

                occurrence_of_keywords_with_span_info={}
                for i in occurrence_of_keywords:
                    if i[0] not in occurrence_of_keywords_with_span_info.keys():
                        occurrence_of_keywords_with_span_info[i[0]] = []
                    occurrence_of_keywords_with_span_info[i[0]].append([i[1], i[2]])


                if len(occurrence_of_keywords) != 0:
                    keywords_counter = Counter(occurrence_of_keywords_without_span_info)
                    # For each keyword found put it in the dictionary
                    for keyword in keywords_counter:
                        number_of_elements = keywords_counter[keyword]
                        normalised_occurence_of_keywords = number_of_elements / length_of_text
                        info_per_pmid = [number_of_elements, normalised_occurence_of_keywords, occurrence_of_keywords_with_span_info[keyword], length_of_character]
                        keywords_to_pmid[keyword][pmid] = info_per_pmid
    return keywords_to_pmid


def chunks(data, SIZE=10000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}




