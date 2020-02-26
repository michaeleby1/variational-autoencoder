import os
import glob
import re
from gutenberg.cleanup import strip_headers
import wikipedia
PageError = wikipedia.exceptions.PageError
DisambiguationError = wikipedia.DisambiguationError
WikipediaException = wikipedia.exceptions.WikipediaException
import pymongo
import time
import pandas as pd
from sklearn import preprocessing, metrics
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import process

# ----------------------
#  ETL PIPELINE
# ----------------------

client = pymongo.MongoClient('mongodb://localhost/')
db = client['gutenberg_db']
collection = db['gutenberg_collection']


def author_title_year(file):
    with open(file, 'r') as f:
        text = f.read()
    author = re.findall(r'(?:Author: )(.*)(?:\n)', text)[0].strip()
    title = re.findall(r'(?:Title: )(.*)(?:\n)', text)[0].strip()
    
    ## queries wikipedia summaries for year; books without without 
    ## summaries will throw an error later and won't enter the database
    summary = str(wikipedia.summary(title))
    year = re.search(r'(\s\d{4})', summary).group().strip()
    return author, title, year


def clean_text(file):
    with open(file, 'r') as f:
        text = strip_headers(f.read())
        
    ## removes newline characters, underscores, other misc characters/words
    chars = ['\n', '_', '\*+', '\[', '\]', 'Illustration: ', 'ILLUSTRATION: ',
             'Footnote: ', 'FOOTNOTE: ', 'Project Gutenberg', 'PROJECT GUTENBERG']
    text = re.compile('|'.join(chars)).sub(' ', text)
    
    ## removes text from title page
    author, title, year = author_title_year(file)
    titlePage = [title, title.upper(), ' by ', ' By ', ' BY ', author, 
                 author.upper(), year, 'Translated ', 'TRANSLATED ', 
                 'Illustrated ', 'ILLUSTRATED ', 'ILLUSTRATIONS ', 
                 'Edited ', 'EDITED ', 'published '
                 'Table of Contents ', 'TABLE OF CONTENTS', 'CONTENTS'
                 'Author of ', 'Online Distributed Proofreading Team',  
                 'Distributed Proofreading Online Team'
                 'Proofreading ', 'Proofreaders ', 'Distributed Proofreaders', 
                 'Distributed Proofreading ']
    text =  re.compile('|'.join(titlePage)).sub(' ', text[:1000])+text[1000:]  
    
    ## removes chapter headings 
    chapterHeadings = ['CHAPTER \d+', 'Chapter \d+', 'CHAPTER \w+',
                       'Chapter \w+','BOOK \d+', 'Book \d+', 'BOOK \w+', 
                       'Book \w+', 'VOLUME \d+', 'Volume \d+', 'VOLUME \w+', 
                       'Volume \w+', 'ACT \d+', 'Act \d+', 'ACT \w+', 
                       'Act \w+', 'PART \d+', 'Part \d+', 'PART \w+', 
                       'Part \w+','I\.', 'II\.', 'III\.', 'IV\.', 'V\.', 
                       'VI\.', 'VII\.', 'VIII\.', 'IX\.', 'X\.', 'XI\.',
                       'XII', 'XII\.', 'XIII\.', 'XIV\.', 'XV\.', 'XVI\.',
                       'XVII\.', 'XVIII\.', 'XIX\.', 'XX\.', 'XXI\.', 'XXII\.',
                       'XXIII\.', 'XXIV\.', 'XXV\.']
    text = re.compile('|'.join(chapterHeadings)).sub(' ', text)
    
    ## truncates multiple spaces into one; will make analysis at the character level easier
    text = re.compile('\s+').sub(' ', text)               
    return text.strip()