import json
import glob
import multiprocessing

import pandas as pd
from itertools import islice
import tqdm
import os

from joblib import Parallel, delayed
from indra.literature.pubmed_client import get_metadata_for_ids

def get_pmid_for_entered_keywords(keywords,
                                  keywords_file_to_open = 'keywords.json',
                                  pmids_return_limit=100,
                                  return_overall_keywords_to_pmids_file = False,
                                  threshold_mentions_in_pmid = 2):
    '''
    This function returns a csv with the pmid mentioning the keyword entered and the number of occurences of the keywords
    with the parameter to limit the results.
    :param keywords: list(str), enter the keywords to return results
    :param pmids_return_limit: int, the number of pmid to return
    :return: pmids_mentioning_keyword: pandas dataframe
    '''

    # Combine all the dicts to get the super dict
    super_dict = __combine_indexed_objects__(keywords_file_to_open = keywords_file_to_open)

    # Whether to return overall_keywords_to_pmids_file
    if return_overall_keywords_to_pmids_file:
        overall_keywords_to_pmids_file_all = pd.DataFrame()
        for sub_dict in chunks(super_dict, 10000):
            overall_keywords_to_pmids_file = pd.DataFrame()
            counter = 0
            for key, val in tqdm.tqdm(sub_dict.items(), total=len(list(sub_dict.keys()))):
                overall_keywords_to_pmids_file.loc[counter, 'keyword'] = key
                pmids_ = []
                for k,v in val.items():
                    if v[0]>=threshold_mentions_in_pmid:
                        pmid_mentions = f'{k}:{v[0]}'
                        pmids_.append(pmid_mentions)
                overall_keywords_to_pmids_file.loc[counter, 'pmids'] = ';'.join(pmids_)
                counter+=1
            overall_keywords_to_pmids_file_all = pd.concat([overall_keywords_to_pmids_file_all, overall_keywords_to_pmids_file], axis=0)
        overall_keywords_to_pmids_file_all = overall_keywords_to_pmids_file_all.reset_index(drop=True)
        return overall_keywords_to_pmids_file_all

    # Check if multiple keywords supplied or one keyword
    if len(keywords) > 1:
        # Get the dataframes for all the keywords
        # get the overlapping pmids for for different keywords
        pmids_for_specific_keywords_full_list = []
        for keyword in keywords:
            pmids_for_specific_keyword = super_dict[keyword].keys()
            pmids_for_specific_keywords_full_list.append(list(pmids_for_specific_keyword))

        intersection_of_pmids_for_all_keywords_entered = set(pmids_for_specific_keywords_full_list[0])
        for s in pmids_for_specific_keywords_full_list[1:]:
            intersection_of_pmids_for_all_keywords_entered.intersection_update(s)
        intersection_of_pmids_for_all_keywords_entered = list(intersection_of_pmids_for_all_keywords_entered)

        if len(intersection_of_pmids_for_all_keywords_entered)>0:
            num_cores = int(multiprocessing.cpu_count() / 3)
            pmids_mentioning_keyword_dfs_list= Parallel(n_jobs=num_cores)(
                delayed(return_pmids_csv_for_a_keyword)(keyword, pmids_return_limit, super_dict,
                                                        intersection_of_pmids_for_all_keywords_entered)
                for keyword in keywords)


            # for keyword in keywords:
            #     pmids_mentioning_keyword = return_pmids_csv_for_a_keyword(keyword, pmids_return_limit, super_dict,
            #                                                               intersection_of_pmids_for_all_keywords_entered)
            #     if pmids_mentioning_keyword.shape[0] != 0:
            #         pmids_mentioning_keyword_dfs_list.append(pmids_mentioning_keyword)
            #         keywords_for_which_papers_found.append(keyword)
            #     else:
            #         print(f'No paper found with keyword {keyword}; will store intersection csv for rest of the keywords')

            # Get the intersection of the dataframes
            ## Make the pmid column as the index
            pmids_mentioning_keyword_dfs_list = [df.set_index('PMID', inplace=False) for df in pmids_mentioning_keyword_dfs_list if df.shape[0]>0]
            if len(pmids_mentioning_keyword_dfs_list) == 0:
                f"No pmids found for {' '.join(keywords)} keywords"
                return pd.DataFrame()

            intersection_pmids_df = pd.concat(pmids_mentioning_keyword_dfs_list, axis=1, join='inner')
            intersection_pmids_df = intersection_pmids_df.reset_index(drop=False)
            intersection_pmids_df['Importance of Keywords'] = \
                intersection_pmids_df['Normalised no of mentions (No of mentions in doc/Number of words in text)'].apply(
                    lambda x: x.sum(), axis=1)
            intersection_pmids_df = intersection_pmids_df.sort_values(by='Importance of Keywords',ascending=False)

            ## Reformatting of the intersection df

            ### Drop the Normalised no of mentions (No of mentions in doc/Number of words in text) columns as redundant
            intersection_pmids_df = intersection_pmids_df.drop(columns=
                                        ['Normalised no of mentions (No of mentions in doc/Number of words in text)'], axis=1)

            ### fill na
            intersection_pmids_df = intersection_pmids_df.fillna('Not Available')

            ## Limit the results of intersection df
            if intersection_pmids_df.shape[0] < int(pmids_return_limit):
                intersection_pmids_df_mod = __add_article_info__(intersection_pmids_df)
                return intersection_pmids_df_mod
            else:
                intersection_pmids_df_mod= __add_article_info__(intersection_pmids_df.iloc[:int(pmids_return_limit)])
                return intersection_pmids_df_mod
        else:
            return pd.DataFrame()
    else:
        intersection_pmids_df_mod = return_pmids_csv_for_a_keyword(keywords[0], pmids_return_limit, super_dict, multiple_keywords= False)
        return __add_article_info__(intersection_pmids_df_mod)

def return_pmids_csv_for_a_keyword(keyword, pmids_return_limit, super_dict,
                                intersection_of_pmids_for_all_keywords_entered=None,
                                   multiple_keywords = True):

    if multiple_keywords:
        # Get the sub dictionary for keyword mentioned
        try:
            sub_dict_for_keyword = super_dict[keyword]
            ## Fill the csv to return
            pmids_mentioning_keyword = pd.DataFrame()
            idx = 0
            sub_dict_for_keyword_smaller = {key: sub_dict_for_keyword[key] for key in intersection_of_pmids_for_all_keywords_entered}
            for k, v in tqdm.tqdm(sorted(sub_dict_for_keyword_smaller.items(), key=lambda item: item[1][0], reverse=True)):
                pmids_mentioning_keyword.loc[idx, 'PMID'] = k
                pmids_mentioning_keyword.loc[idx, f'No of mentions of {keyword} in doc'] = v[0]
                pmids_mentioning_keyword.loc[idx, 'Normalised no of mentions' \
                                                  ' (No of mentions in doc/Number of words in text)'] \
                    = v[1]
                # add the context info here
                sentences_extracted = __add_context_info(k, v)

                for i, sentence in enumerate(sentences_extracted):
                    pmids_mentioning_keyword.loc[idx, f'{keyword} sentence {i + 1}th mention'] = sentence
                idx += 1

            if pmids_mentioning_keyword.shape[0] != 0:
                if not multiple_keywords:
                    if pmids_mentioning_keyword.shape[0] < int(pmids_return_limit):
                        return pmids_mentioning_keyword
                    else:
                        return pmids_mentioning_keyword.iloc[:int(pmids_return_limit)], keyword
                else:
                    return pmids_mentioning_keyword
            else:
                print(f'No paper found with keyword {keyword}; will store intersection csv for rest of the keywords')
        except:
            print(f'{keyword} not in the list of keywords')
            return pd.DataFrame()
    else:
        # Get the sub dictionary for keyword mentioned
        try:
            sub_dict_for_keyword = super_dict[keyword]
            ## Fill the csv to return
            pmids_mentioning_keyword = pd.DataFrame()
            idx = 0
            for k, v in tqdm.tqdm(sorted(sub_dict_for_keyword.items(), key=lambda item: item[1][0], reverse=True)):
                pmids_mentioning_keyword.loc[idx, 'PMID'] = k
                pmids_mentioning_keyword.loc[idx, f'No of mentions of {keyword} in doc'] = v[0]
                pmids_mentioning_keyword.loc[idx, 'Normalised no of mentions' \
                                                  ' (No of mentions in doc/Number of words in text)'] \
                    = v[1]
                # add the context info here
                sentences_extracted = __add_context_info(k, v)

                for i, sentence in enumerate(sentences_extracted):
                    pmids_mentioning_keyword.loc[idx, f'{keyword} sentence {i + 1}th mention'] = sentence
                idx += 1

            return pmids_mentioning_keyword
        except:
            print(f'{keyword} not in the list of keywords')
            return pd.DataFrame()

def __add_article_info__(dataframe):
    list_of_pmids = dataframe['PMID'].tolist()

    # info gathering from the server
    df_info = pd.DataFrame()
    counter = 0
    for sub_list in chunks_list(list_of_pmids,199):
        dict_obtained = get_metadata_for_ids(pmid_list=sub_list)
        for k, v in dict_obtained.items():
            df_info.loc[counter, 'PMID'] = k
            df_info.loc[counter, 'Title'] = v['title']
            df_info.loc[counter, 'Journal'] = v['journal_title']
            df_info.loc[counter, 'Year'] = int(v['publication_date']['year'])
            df_info.loc[counter, 'Authors'] = '; '.join(v['authors'])
            counter+=1
    df_final = df_info.merge(dataframe, on=['PMID'], how='inner')

    return df_final


def __add_context_info(doc_id, object_different_values):
    '''
    This function returns the context text for the keyword entered.
    :param doc_id:
    :param object_different_values:
    :return:
    '''

    names_of_parsed_file = os.listdir('docs_json_objects/data')

    file_to_open = [i for i in names_of_parsed_file if str(object_different_values[-1]) == i.split('.')[0].split('_')[-1]][0]

    # doc file
    if file_to_open.split('.')[-1] == 'json':
        file_json = json.load(open(os.path.join('docs_json_objects/data', file_to_open), 'r'))
    else:
        file_json = {}

    # doc object in the json
    doc_text = file_json[doc_id]

    ## now divide the length by three

    total_characters = object_different_values[-2]

    starting_of_char_in_first_partition = int(total_characters/3)

    starting_of_char_in_second_partition = int((total_characters / 3)*2)

    # now take 4 examples from each of the three sections available

    list_having_the_coordinates_of_keyword_mentioned = object_different_values[2]

    sub_list1 = [i for i in list_having_the_coordinates_of_keyword_mentioned if i[1]<starting_of_char_in_first_partition][:4]

    sub_list2= [i for i in list_having_the_coordinates_of_keyword_mentioned if
                 i[1] > starting_of_char_in_first_partition and i[1] < starting_of_char_in_second_partition][:4]

    sub_list3 = [i for i in list_having_the_coordinates_of_keyword_mentioned if
                 i[1] > starting_of_char_in_second_partition][:4]

    sentences_extracted_1 = __extract_text_from_doc__(sub_list1, doc_text['text_0'])

    sentences_extracted_2 = __extract_text_from_doc__(sub_list2, doc_text['text_0'])

    sentences_extracted_3 = __extract_text_from_doc__(sub_list3, doc_text['text_0'])

    sentences_extracted = sentences_extracted_1+sentences_extracted_2+sentences_extracted_3

    return sentences_extracted


def __extract_text_from_doc__(list_of_coordinates, doc_text):
    length_of_characters_to_extract_before_or_after_keyword = 150

    sentences_extracted = []
    for coord1 in list_of_coordinates:
        if coord1[0]-length_of_characters_to_extract_before_or_after_keyword < 0:
            sentences_extracted.append(doc_text[0:
                                                coord1[1] + length_of_characters_to_extract_before_or_after_keyword])
        else:
            sentences_extracted.append(doc_text[coord1[0]-length_of_characters_to_extract_before_or_after_keyword:
                                            coord1[1]+length_of_characters_to_extract_before_or_after_keyword])
    return sentences_extracted

def __combine_indexed_objects__(keywords_file_to_open = 'keywords.json' ):
    '''
    This function combines all the indexed docs json files to one dictionary
    :return: super_dict
    '''

    indexed_doc_objects_path = f"indexed_docs_objects/docs_object/{keywords_file_to_open.split('.')[0]}/*"
    files_paths_in_folder = glob.glob(indexed_doc_objects_path)

    # open each file and add to the super dictionary
    super_dict = {}
    for path in files_paths_in_folder:
        if path.split('.')[-1] == 'json':
            indexed_docs = json.load(open(path,'r'))
            for key, values in indexed_docs.items():
                if key not in super_dict:
                    super_dict[key] = {}
                for k, v in values.items():
                    v_new = [int(s) for s in path.split('/')[-1].split('.')[0].split('_') if s.isdigit()]
                    super_dict[key][k] = v + v_new
        else:
            continue
    return super_dict


def chunks(data, SIZE=10000):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}

def chunks_list(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]