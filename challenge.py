# Import your dependencies
# -*- coding: utf-8 -*-
import json
import pandas as pd
import numpy as np
import re # Built-in Python module for regular expressions
from sqlalchemy import create_engine
import time

# Create an automated ETL pipeline
# Create a function that takes in three arguments and performs ETL
def wikipedia_kaggle_pipeline(wiki_data, kaggle_metadata, ratings_data):
    
    #-----------------------------
    # Extract the three data files 
    #-----------------------------
    
    # Assume that formats and file directory are correct. Also assumes the person input the 3 arguments in the correct order.
    
    # Create path to saved files
    file_dir = 'C:/Users/Helen/Movies-ETL/'
    
    # Import kaggle data
    kaggle_metadata = pd.read_csv(f'{file_dir}{kaggle_metadata}', low_memory = False)
    ratings = pd.read_csv(f'{file_dir}{ratings_data}')
    
    # Import wikipedia data
    with open(f'{file_dir}{wiki_data}', mode = 'r') as file:
        wiki_movies_raw = json.load(file)
    
    # If extracted data correctly, print statement saying so
    print('Successfully extracted files.')
    
    #------------------------------
    #Transform the three data files
    #------------------------------
    
    try:
        # Create new list for wikipedia data with only movies and has Director and imdb_link
        wiki_movies = [movie for movie in wiki_movies_raw 
                  if ('Director' in movie or 'Directed by' in movie)
                  and 'imdb_link' in movie
                  and 'No. of episodes' not in movie]
    
    except:
        print('One or more of the following columns do not exist: Director, Directed by, imdb_link, No. of episodes.')
    
    # Create function to clean up each movie entry so it's in a standard format
    def clean_movie(movie):

        # Make a copy of all the info in movie
        movie = dict(movie) # Create a non-destructive copy, movie becomes a local variable, will only change this 'movie'

        # Create alt_titles dict
        alt_titles = {}

        # Loop through a list of all alt title keys
        for key in ['Also known as', 'Arabic', 'Cantonese', 'Chinese', 'French', 
                    'Hangul', 'Hebrew', 'Hepburn', 'Japanese', 'Literally', 
                    'Mandarin', 'McCune-Reischauer', 'Original title', 'Polish', 
                    'Revised Romanization', 'Romanized', 
                    'Russian', 'Simplified', 'Traditional', 'Yiddish']:

            # Check if the current key exists in the movie object
            if key in movie:
                # If so, remove the key-value pair and add to alt titles dict
                alt_titles[key] = movie[key]
                movie.pop(key)

        # After looping through every key, add the alt titles dict as a new column to movie object
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # Create a nested function to change column name
        def change_column_name(old_name, new_name):
            if old_name in movie: 
                movie[new_name] = movie.pop(old_name)

        # Merge column names
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running Time')
        change_column_name('Original release', 'Release Date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production Company(s)')
        change_column_name('Productioncompany ', 'Production Company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie
    
    try:
        
        # Make a list of cleaned movies with a list comprehension
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
    
    except:
        print('One or more columns does not exist to make a list of cleaned movies.')

    # Create wiki_movies_df as the DF created from clean_movies
    wiki_movies_df = pd.DataFrame(clean_movies)
    
    # If successful, print statement saying so
    print('Successfully created DataFrame with cleaned movie entries.')
    
    # Extract the imdb id from the imdb link and create a new column for the ids
    # use str.extract() to extract and put 'r' for python to treat \ as raw str of text
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})') # Assume all imdb_link has imdb_id
        
    try:
        # Drop duplicate imdb ids using drop_duplicates()
        wiki_movies_df.drop_duplicates(subset = 'imdb_id', inplace = True)
    
    except:
        print('There are no duplicate imdb ids.')
    
    # Check null values with list comprehension and keep the following columns with < 90% null values
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep] # Assume that all code ran correctly above to keep the following columns and reassign back to the DF

    # Assume that the format is the same, so columns of box office, running time, release date, and budget are the same and the data structures are also the same
    
    #---Parse through box office---
    
    # dropna() from box office
    box_office = wiki_movies_df['Box office'].dropna()
    
    # Use join() for data points that are lists
    box_office = box_office.apply(lambda x: ''.join(x) if type(x) == list else x)
    
    # Create regex for the first form of the box_office data ($123.4 million or billion)
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    
    # Create regex for the first form of the box_office data ($123,456,789)
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    # Handling values that were given as a range
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex = True)
    
    # Create parse_dollars function to take in a string and return a floating-point number
    def parse_dollars(s):
        # If s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # If input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags = re.IGNORECASE):

            # Remove dollar sign and ' million'
            s = re.sub('\$|\s|[a-zA-Z]', '', s)

            # Convert to float and multiply by a million
            value = float(s) * 10 ** 6

            # Return value
            return value

        # If input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags = re.IGNORECASE):

            # Remove dollar sign and ' billion'
            s = re.sub('\$|\s|[a-zA-Z]', '', s)

            # Convert to float and multiply by a billion
            value = float(s) * 10 ** 9

            # Return value
            return value

        # If input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags = re.IGNORECASE):

            # Remove dollar sign and commas
            s = re.sub('\$|,', '', s)

            # Convert to float
            value = float(s)

            # Return value
            return value

        # Otherwise, return NaN
        else:
            return np.nan
    
    # Extract the values from box_office using str.extract, add new column to DF
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags = re.IGNORECASE)[0].apply(parse_dollars)

    # Drop Box Office column from DF
    wiki_movies_df.drop('Box office', axis = 1, inplace = True)
    
   
    #---Parse through budget---
    
    # Create a budget variable
    budget = wiki_movies_df['Budget'].dropna()
    
    # Convert any lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Remove any values between a dollar sign and a hyphen (for budgets given in ranges)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex = True)
    
    # Remove the citation references
    budget = budget.str.replace(r'\[\d+\]\s*','')
    
    # Extract the values from box_office using str.extract, add new column to DF
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags = re.IGNORECASE)[0].apply(parse_dollars)
    
    # Drop Box Office column from DF
    wiki_movies_df.drop('Budget', axis = 1, inplace = True)
 
    
    #---Parse through release date---
    
    # Create a variable that holds the non-null values of Release date in DF, convert list to strings
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ''.join(x) if type(x) == list else x)
    
    # Create different forms to parse through release dates
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[0123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    # Parse the dates with to_datetime() method in Pandas
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format = True)
    
    
    #---Parse through running time---
    
    # Create a variable that holds the non-null values of running time
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ''.join(x) if type(x) == list else x)
    
    # Extract hours and minutes
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    
    # Use to_numeric() and coerce with fillna()
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors = 'coerce')).fillna(0)
    
    # Convert hours and mins to mins if the pure mins capture group is zero and save in DF
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis = 1)
    
    # Drop Running time from DF
    wiki_movies_df.drop('Running time', axis = 1, inplace = True)
    
    
    #---Clean the kaggle data---
    
    # This follows along the same assumption that data structures are the same
    
    # Keep rows where the adult column is False
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult', axis = 'columns')
    
    # Create boolean column and assign it back to the DF
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    
    # Convert budget, id, and popularity to numeric columns
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors = 'raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors = 'raise')
    
    # Convert release date to datetime
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    # Convert timestamp with the origin is 'unix' and the time unit is seconds
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit = 's')
    
    
    #---Merge Wikipedia and Kaggle Metadata---
    
    # Merge both DF and use suffixes for DF identification
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on = 'imdb_id', suffixes = ['_wiki', '_kaggle'])
    
    # Fillna with zeros for the following columns
    movies_df[['running_time', 'runtime']].fillna(0)
    movies_df[['budget_wiki', 'budget_kaggle']].fillna(0)
    movies_df[['box_office', 'revenue']].fillna(0)
    
    try:
        # Drop the row with an outlier within the following dates
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    
    except:
        print('There are no outliers within these dates.')
    
    # Drop the following Wikipedia columns
    movies_df.drop(columns = ['title_wiki', 'release_date_wiki', 'Language', 'Production Company(s)'], inplace = True)
    
    # Create function that fills in missing data for column pair and drops redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):

        # In DF Kaggle column, we will take the wiki column value if kaggle column is 0
        # If kaggle column has a value, keep the kaggle value
        # axis = 1, move down the desired column (kaggle & wiki column)
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis = 1)

        # After taking the values from wiki_column, delete this column from the DF
        df.drop(columns = wiki_column, inplace = True)
        
    # Run function for runtime, budget and box_office (already filled in with zeros)
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    # As every row is false, we can drop the column
    movies_df.drop(columns = 'video', inplace = True)
    
    # Reorder the columns to make it easier to read
    # Groups (Identifying info, Quantitative facts, Qualitative facts, Business data, People)
    movies_df = movies_df[['imdb_id', 'id', 'title_kaggle', 'original_title', 'tagline', 'belongs_to_collection', 'url', 'imdb_link', 
                           'runtime', 'budget_kaggle', 'revenue', 'release_date_kaggle', 'popularity', 'vote_average', 'vote_count', 
                           'genres', 'original_language', 'overview', 'spoken_languages', 'Country', 
                           'production_companies', 'production_countries', 'Distributor', 
                           'Producer(s)', 'Director', 'Starring', 'Cinematography', 'Editor(s)', 'Writer(s)', 'Composer(s)', 'Based on'
                          ]]
    
    # Rename columns to be consistent
    movies_df.rename({'id': 'kaggle_id', 
                      'title_kaggle': 'title', 
                      'url': 'wikipedia_url', 
                      'budget_kaggle': 'budget', 
                      'release_date_kaggle': 'release_date', 
                      'Country': 'country', 
                      'Distributor': 'distributor', 
                      'Producer(s)': 'producers', 
                      'Director': 'director', 
                      'Starring': 'starring', 
                      'Cinematography': 'cinematograpy', 
                      'Editor(s)': 'editors', 
                      'Writer(s)': 'writers', 
                      'Composer(s)': 'composers', 
                      'Based on': 'based_on'
                     }, axis = 'columns', inplace = True)
    
    
    #---Transform and Merge rating data---
    
    # Count movieId and rating and rename userID to count
    rating_counts = ratings.groupby(['movieId', 'rating'], as_index = False).count().rename({'userId': 'count'}, axis =1).pivot(index = 'movieId', columns = 'rating', values = 'count')
    
    # Rename column to include 'rating_' + column
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    # Use a left merge for the movies_df and rating_counts
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on = 'kaggle_id', right_index = True, how = 'left')
    
    # Fill missing values with zeroes
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    #----------------------------
    # Load the data to sql tables
    #----------------------------
    
    # Create connection string to load data
    db_string = f'postgres://postgres:@127.0.0.1:5432/movie_data'
    
    # Create the database engine
    engine = create_engine(db_string)
    
    # Import the movie data
    movies_df.to_sql(name = 'movies', con = engine, if_exists = 'replace')
    
    # read_csv() for ratings with chunksize= parameter
    # Create variable for the number of rows imported
    rows_imported = 0
    
    for data in pd.read_csv(f'{file_dir}{ratings_data}', chunksize=1000000):
        
        print(data.head())
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='\n')
        
        if rows_imported == 0:
            # Remove existing data from the ratings file and replace new data
            data.to_sql(name='ratings', con=engine, if_exists='replace')
            
        else:
            
            data.to_sql(name='ratings', con=engine, if_exists='append')
            
        rows_imported += len(data)
        
    print(f'ETL process completed.')
    
# Test out function
# 3 data files: movies_metadata.csv, ratings.csv, wikipedia.movies.json
wikipedia_kaggle_pipeline('wikipedia.movies.json', 'movies_metadata.csv', 'ratings.csv')