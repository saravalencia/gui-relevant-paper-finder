import pandas as pd
import tqdm
import requests
import json
import os
import asyncio
from random import shuffle
from metapub import PubMedFetcher


def background(f):
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped

class UpdatePaperCorpus:

    def __init__(self,
                 file_path_for_general_keywords,
                 last_list_of_all_pmids_in_database):
        '''
        This pipeline updates the free articles available from the PUBMED Central BioC API in the database.
        :param file_path_for_general_keywords:
        :param list_of_last_pmids_downloaded:
        :return: updated json object to be stored
        '''

        self.file_path_for_general_keywords = file_path_for_general_keywords
        self.last_list_of_all_pmids_in_database = last_list_of_all_pmids_in_database

    def search_for_new_pmids(self):
        '''
        It searches for new papers with the keyword file to add to
        the database.
        '''

        fetch = PubMedFetcher()
        general_keyword_file = pd.read_csv(self.file_path_for_general_keywords)

        list_of_pmids = []
        for idx, row in tqdm.tqdm(general_keyword_file.iterrows(), total=general_keyword_file.shape[0]):
            keyword = row['topic']
            pmids_obtained = None
            retry = 0
            ## now we will call get_ids function to get all the pmids
            while pmids_obtained is None and retry < 5:
                try:
                    pmids_obtained = fetch.pmids_for_query(keyword, retmax=1000000)
                except:
                    retry += 1
                    pass
            list_of_pmids.append(pmids_obtained)

        unique_list = list(set([j for i in list_of_pmids for j in i]))

        last_dict_of_all_pmids_in_database = {}
        for i in self.last_list_of_all_pmids_in_database:
            last_dict_of_all_pmids_in_database[i] = 1

        pmids_to_download = [i for i in unique_list if i not in last_dict_of_all_pmids_in_database.keys()]

        return pmids_to_download


    def download_and_parse_for_one_paper(self, pmid_number):
        '''
        This function downloads the papers from the PUBMED Central BioC API.
        The object returned is json and the data is stored as json object with corresponding PMID and other info.
        :param pmid_number: type(int)
        :return: text in article
        '''
        PMC_BASE_URL = 'https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json'
        pmid_with_text = {}
        # get the path on the rest api
        path_to_download = os.path.join(PMC_BASE_URL, pmid_number, 'unicode')

        try:
            # Download the paper
            response = requests.get(path_to_download)
            json_object = json.loads(response.content)
            pmid_with_text['raw_json_object'] = json_object
            # Extract the text and put it into database
            documents_in_json_object = json_object['documents']
            for idx1, doc in enumerate(documents_in_json_object):
                passages = doc['passages']
                # iterate over passages
                text_in_article_list = []

                for idx2, passage in enumerate(passages):
                    text = passage['text']
                    text_in_article_list.append(text)
                text_in_article = ' '.join(text_in_article_list)
                pmid_with_text[f'text_{idx1}'] = text_in_article
            return pmid_with_text
        except:
            # print("Not found or some error occur")
            return pmid_with_text

    @background
    def download_chunks_of_papers(self, sub_pmid_list, file_index_number):
        sub_pmid_dict = {}
        for i in tqdm.tqdm(sub_pmid_list):
            json_object_downloaded = self.download_and_parse_for_one_paper(i)
            if len(json_object_downloaded.keys())>0:
                sub_pmid_dict[i] = json_object_downloaded
            else:
                continue
        json.dump(sub_pmid_dict, open(f'docs_json_objects/data/full_text_with_pmids_for_{file_index_number}.json', 'w'))



    @staticmethod
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def run(self):
        '''
        This function is used to run the pipeline after the object of the class
        is created.
        '''

        # first lets get the list of pmids to download

        pmids_to_download = self.search_for_new_pmids()
        shuffle(pmids_to_download)
        print('The max number of papers that can be downloaded given many of them would not be free: ', len(pmids_to_download))
        print('Note, that the pipeline will recheck the papers that were not found in last update to download them if they became free.')
        # now we will download the papers in parallel mode

        ## number of json objects already present before the this update
        number_of_json_objects_before_update = len(os.listdir('docs_json_objects/data'))

        ## download the json object and save it in parallel mode
        for idx, sub_list in enumerate(UpdatePaperCorpus.chunks(pmids_to_download, 10000)):
            if number_of_json_objects_before_update == 0:
                self.download_chunks_of_papers(sub_list, idx)
            else:
                self.download_chunks_of_papers(sub_list, idx+number_of_json_objects_before_update+1)


if __name__ == "__main__":
    print('Hello')
    pass




            









