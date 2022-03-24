import pandas as pd
import tqdm
import requests
import json
import os
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger('neobolt').setLevel(logging.INFO)


def download_and_parse_papers():
    '''
    This function downloads the papers from the PUBMED Central BioC API.
    The object returned is json and the data is stored as json object with corresponding PMID and other info.
    :return:
    '''
    PMC_BASE_URL = 'https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json'

    pmids_to_download_csv = pd.read_csv('pmids_to_download/pmc_subset_to_download.csv')
    pmid_with_text = dict()

    for idx, row in pmids_to_download_csv.iterrows():
        pmid = str(int(row['PMID']))
        pmid_with_text[pmid] = {}
        # get the path on the rest api
        path_to_download = os.path.join(PMC_BASE_URL, pmid, 'unicode')
        # Download the paper
        try:
            response = requests.get(path_to_download)
            json_object = json.loads(response.content)
            pmid_with_text[pmid]['raw_json_object'] = json_object
            # Extract the text and put it into database
            documents_in_json_object = json_object['documents']

            for idx1, doc in enumerate(documents_in_json_object):
                passages = doc['passages']
                # iterate over passages
                text_in_article_list = []

                for idx2, passage in enumerate(passages):
                    if idx2 == 0:
                        pmid_with_text[pmid][f'article_info_{idx1}'] = passage['infons']
                    else:
                        text = passage['text']
                        text_in_article_list.append(text)
                text_in_article = ' '.join(text_in_article_list)
                pmid_with_text[pmid][f'text_{idx1}'] = text_in_article
        except:
            log.info(f'Unable to download {pmid} paper ')
            pmid_with_text.pop(pmid, None)
            continue

        if idx%500 == 0 and idx!=0:
            log.info(f'Done with {idx} number of papers bitch!!!!!!!!!')

        # Save the data
        if idx%10000 == 0 and idx != 0:
            log.info(f'Done with {idx} papers, now saving them')
            with open(f'docs_json_objects/full_text_with_pmids_for_{idx}.json', 'w') as f:
                json.dump(pmid_with_text, f)
            pmid_with_text = dict()



def download_and_parse_for_one_paper(pmid_number):
    '''
    This function downloads the papers from the PUBMED Central BioC API.
    The object returned is json and the data is stored as json object with corresponding PMID and other info.
    :param pmid_number: type(int)
    :return: text in article
    '''
    PMC_BASE_URL = 'https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json'
    pmid_with_text = dict()
    pmid = str(pmid_number)
    pmid_with_text[pmid] = {}
    # get the path on the rest api
    path_to_download = os.path.join(PMC_BASE_URL, pmid, 'unicode')
    # Download the paper
    response = requests.get(path_to_download)
    json_object = json.loads(response.content)
    pmid_with_text[pmid]['raw_json_object'] = json_object
    # Extract the text and put it into database
    documents_in_json_object = json_object['documents']
    text_overall = []
    for idx1, doc in enumerate(documents_in_json_object):
        passages = doc['passages']
        # iterate over passages
        text_in_article_list = []

        for idx2, passage in enumerate(passages):
            text = passage['text']
            text_in_article_list.append(text)
        text_in_article = ' '.join(text_in_article_list)
        text_overall.append(text_in_article)

    return ' '.join(text_overall)
