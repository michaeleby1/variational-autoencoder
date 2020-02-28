import re
from gutenberg.cleanup import strip_headers
import pandas as pd
from urllib import request
from urllib.error import HTTPError

# ---------------------------------------------------------------------------------------- #
#   ETL Pipeline                                                                           #
# ---------------------------------------------------------------------------------------- #

def get_file_nums(author, df):

    files = list(df[df['author'] == author].file)

    files = [(re.findall(r'\d+', file)) for file in files]
    files = [x for file in files for x in file]
    files = [file for file in files if not file == '0']

    return files


def get_all_text(files):    
    
    full_text = ''

    for file in files:

        try:
            url = f"http://www.gutenberg.org/files/{file}/{file}.txt"
            response = request.urlopen(url)
            raw = response.read().decode('utf8')
            text = strip_headers(raw)
            text = re.compile('\n').sub('', text)
            full_text += text
            
        except (HTTPError) as e:
            
            try:
                url = f"http://www.gutenberg.org/files/{file}/{file}-0.txt"
                response = request.urlopen(url)
                raw = response.read().decode('utf8')
                text = strip_headers(raw)
                text = re.compile('\n').sub('', text)
                full_text += text
                
            except (HTTPError) as e2:    
                print(f'{file} not found.')
                
        except UnicodeDecodeError as e3:
            print(f'Can\'t decode {file}')
            
    return full_text