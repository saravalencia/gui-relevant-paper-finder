from cProfile import label
import sys
from PyQt5.QtWidgets import QApplication, QLabel, QPushButton, QVBoxLayout, QWidget, QFileDialog, QGridLayout, QDialog, QLineEdit, QInputDialog, QMessageBox
from PyQt5 import QtGui, QtCore
from PyQt5.QtGui import QCursor, QPixmap
import json
import pandas as pd 
import csv
import os
from src.return_papers_from_indexes.get_pmid_for_keywords import get_pmid_for_entered_keywords
from src.indexing_engine.indexing_engine import index_document_object
import glob
import multiprocessing
from joblib import Parallel, delayed


widgets = {
    "logo": [],
    "button": [],
    "label" : [],
    "csv_text" : [],
    "question": []
   
}


app = QApplication(sys.argv)
window = QWidget()
window.setWindowTitle("Relevant Paper Finder")
window.setFixedWidth(1000)
window.setFixedHeight(550)
window.setStyleSheet("background: #ffffff;")


def clear_widgets():
    for widget in widgets:
        if widgets[widget] != []:
            widgets[widget][-1].hide()
        for i in range(0, len(widgets[widget])):
            widgets[widget].pop()

def show_frame1():
    clear_widgets()
    frame1()

def show_frame2():
    clear_widgets()
    frame2()    
          


grid = QGridLayout()

def frame1():

    def browsefiles(): 
        fname = QFileDialog.getOpenFileName(None, 'Open file', '', 'csv(*.csv)')
        data = pd.read_csv(fname[0])
        df = data
        print(df) 

        json_file = {}
        for idx, row in df.iterrows():
            if row[0] not in json_file.keys():
                json_file[row[0]] = []
            for i, col in row[1:].iteritems():
                if not isinstance(col, float):
                    json_file[row[0]].append(col)

        

            
        file_suffix = input_suffix()
        json.dump(json_file, open(f'keywords_lists/keywords_{file_suffix}.json', 'w'))
        file_location()               

        if not os.path.isdir('docs_json_objects/data'):
            os.makedirs('docs_json_objects/data')
        number_of_objects_in_docs_json_folder = os.listdir('docs_json_objects/data')
        keywords_file_chosen = f'keywords_{file_suffix}.json'
        num_cores = multiprocessing.cpu_count()

        # check if the indexing already done or not
        if os.path.isdir("indexed_docs_objects/docs_object/"+keywords_file_chosen.split('.')[0]):
            if len(glob.glob("indexed_docs_objects/docs_object/"+keywords_file_chosen.split('.')[0]+"/*"))>0:
                # print('Do you want to update indexing?')
                
                if update_indexing() == QMessageBox.Yes :
                    Parallel(n_jobs=num_cores)(delayed(index_document_object)(x, keywords_dict_name=keywords_file_chosen)
                                            for x in range(len(number_of_objects_in_docs_json_folder)))
                else:
                    print('Skipping indexing and moving to next step.')
        else:
            Parallel(n_jobs=num_cores)(delayed(index_document_object)(x, keywords_dict_name= keywords_file_chosen)
                        for x in range(len(number_of_objects_in_docs_json_folder)))

        #create output folders
        if not os.path.isdir('output'):
            os.makedirs('output')
        if not os.path.isdir('output/multiple_keywords'):
            os.makedirs('output/multiple_keywords')
        if not os.path.isdir('output/overall_keywords_to_pmids'):
            os.makedirs('output/overall_keywords_to_pmids')
        

        if options_message() == QMessageBox.Yes:
            threshold_value = input_number_of_mentions()
            overall_keywords_to_pmids_file = get_pmid_for_entered_keywords([],
                                            keywords_file_to_open=keywords_file_chosen,return_overall_keywords_to_pmids_file=True
                                                                        , threshold_mentions_in_pmid=int(threshold_value))
            overall_keywords_to_pmids_file.to_csv(f"output/overall_keywords_to_pmids/overall_keywords_to_pmids_for_{keywords_file_chosen.split('.')[0]}.csv", index=False)
            location_Results()

        else:
            
            keywords = json.load(open(f'keywords_lists/{keywords_file_chosen}'))
            list_of_keywords = list(keywords.keys())
            string_keywords = ','.join(list_of_keywords)
            message = f"This is the list of keywords: {string_keywords}."
            options_keywords_message(message)
            print(','.join(list_of_keywords)) 
            keywords = input_get_results()
            keywords_list = keywords.strip().split(',')
            keywords_list = [x.strip() for x in keywords_list]
            limit_results = input_limit_results()
            pmids_mentioning_keyword = get_pmid_for_entered_keywords(keywords_list,
                                            keywords_file_to_open=keywords_file_chosen,pmids_return_limit=limit_results)

            if pmids_mentioning_keyword.shape[0]!=0:
                pmids_mentioning_keyword.to_csv(f"output/multiple_keywords/pmids_mentioning_{keywords.replace('/','_').replace(',','_').replace(' ','_')}_{pmids_mentioning_keyword.shape[0]}.csv", index=False)
                """ print(f"Your results are stored in output/multiple_keywords/pmids_mentioning_{keywords.replace('/','_').replace(',','_').replace(' ','_')}_{pmids_mentioning_keyword.shape[0]}.csv folder") """
                file_location_results()
            else:
               """  print("Something went wrong... There is no output. Try to combine other keywords.") """
               file_error()


    # Display logo
    image = QPixmap("logo.png")
    logo = QLabel()
    logo.setPixmap(image)
    logo.setAlignment(QtCore.Qt.AlignCenter)
    logo.setStyleSheet(
        "margin-top: 30px;"
        "margin-bottom: 60px;"
        )
    widgets["logo"].append(logo)

    #Button Widget
    button = QPushButton("Browse")
    button.setCursor(QCursor(QtCore.Qt.PointingHandCursor) )
    button.setStyleSheet(
        "*{border: 0.2px solid '#293241';" +
        "border-radius: 5px;" +
        "font-size: 25px;" +
        "background-color: '#14213d';" +
        "color: 'white';" +
        "padding: 10px 0;" +
        "margin: 20px 260px;}" +
        "*:hover{background: '#023e8a';}"

    )
    button.clicked.connect(browsefiles)
    button.clicked.connect(show_frame2)

    widgets["button"].append(button)

    #Display Label 
    label = QLabel("Make sure that there is a raw csv file that we will convert to json. Look at the sample.csv file in folder /raw_csv_file_with_keywords ")
    label.setAlignment(QtCore.Qt.AlignCenter)
    label.setWordWrap(True)
    label.setStyleSheet(
        "font-size: 11px;" +
        "color: '#3d5a80';"+
        "margin: 10px 10px;}"
    )
    widgets["label"].append(label)

    grid.addWidget(widgets["logo"][-1], 0, 0)
    grid.addWidget(widgets["button"][-1], 1, 0)
    grid.addWidget(widgets["label"][-1], 2, 0)

    #Display Options
    def options_message():
        dlg = QMessageBox()
        dlg.setText('Now you have two options. The output will be an excel table containing:\n'
            '1. Either every keyword and the PMIDs that mention them + number of occurences.\n'
            '2. Or the occurences  and contexts of single/multiple keywords (chosen by you in the following step) for each PMID.\n')
        dlg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        buttonY = dlg.button(QMessageBox.Yes)
        buttonY.setText('1')
        buttonN = dlg.button(QMessageBox.No)
        buttonN.setText('2')
        return dlg.exec()

    #Display Options
    def options_keywords_message(message):
        QMessageBox.about(None, "Keywords list", message)
    
    #Display update indexing
    def update_indexing():
        dlg = QMessageBox()
        dlg.setWindowTitle("Update indexing")
        dlg.setText("Do you want to update indexing ?")
        dlg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        return dlg.exec()



    # Display input suffix
    def input_suffix():
            inputSuffixName = QInputDialog.getText(None, 'Input Dialog', 'Enter the suffix for your keyword file: ')
            text =  inputSuffixName[0]
            return text
            
    #Display file location
    def file_location():
            QMessageBox.about(None, "File location", "Your file is stored at keywords_lists folder")

    # Display input number of mentions
    def input_number_of_mentions():
            inputNumberOfMentions = QInputDialog.getInt(None, 'Input Dialog', 'Enter the minimum value for the number of mentions of keywords in an article: ')
            text =  inputNumberOfMentions[0]
            return text

    #Display file location results
    def location_Results():
            QMessageBox.about(None, "File location", "Your results are stored in output/overall_keywords_to_pmids folder")

    # Display input keywords to get results
    def input_get_results():
            inputGetResults = QInputDialog.getText(None, 'Input Dialog', 'Enter the keywords to get results(If multiple keywords, remember to add a comma: "," between each keyword. For eg: AAV,ITR, also the keywords are case-sensitive')
            text =  inputGetResults[0]
            return text

    # Display input keywords to get results
    def input_limit_results():
            inputLimitResults = QInputDialog.getInt(None, 'Input Dialog', 'Number of results to save (Default: Top 100 pmids):')
            text =  inputLimitResults[0]
            return text
        
    #Display results file 
    def file_location_results():
            QMessageBox.about(None, "File location", "Your results are stored in output/multiple_keywords")

    #Display results error 
    def file_error():
            QMessageBox.about(None, "Error", "Something went wrong... There is no output. Try to combine other keywords.")

def frame2():

    question = QLineEdit("placeholder")
    question.setAlignment(QtCore.Qt.AlignCenter)
    question.setStyleSheet(
        '''
        font-family: Shanti;
        font-size: 25px;"
        color: 'white';"
        padding: 75px;
        '''
    )
    widgets["question"].append(question)

    grid.addWidget(widgets["question"][-1], 1, 0)




frame1()  

window.setLayout(grid)

window.show()
sys.exit(app.exec())













