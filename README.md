# Movies ETL

## Project Overview

Amazing Prime, one of the world's largest online retailer, has a platform for streaming movies and TV shows called Amazing Prime Video. The Amazing Prime Video team would like to know which low budget movies being released will become popular and buy the streaming rights at a bargain.

In order to obtain this information, Amazing Prime sponsors a hackathon for the local coding community and provides a clean dataset of movie data for participants to predict the popular movies.

To provide a clean dataset, we performed the following steps.

  1. ***Extract*** the data from its original file and assign it to a variable.
  2. ***Transform*** the data, such as:
                    
        - Remove duplicate values
        - Remove unwanted columns
        - Fill 'NaN' values with '0'
        - Convert values into the correct datatypes 
        - Fill in missing values from another source that has the values we need
        - Rename columns for consistencies
        
  3. ***Load*** the transformed data to our PostgreSQL database.
  
**Note:** We have created a DataFrame that merges movies with ratings, but for this project, only the movies Dataframe was loaded into the PostgreSQL database.

## Resources

  - Data Source: wikipedia.movies.json, movies_metadata.csv, ratings.csv
  - Software: Python 3.7.6, Jupyter Lab 1.2.6, PostgreSQL 11.7, pgAdmin
  - Library: Pandas, NumPy, json, re, sqlalchemy/create_engine, time
   
**Note:** time has been imported if you choose to check how long the upload takes for the ratings data.

## Challenge

The dataset turned out well and Amazing Prime wants to keep it updated on a daily basis. For this to happen:

1. It will be automated as a repeated, ongoing process.
2. Run without supervision
3. Takes in new data.
4. Performs the appropriate transformations.
5. Loads the data into existing data.

### Assumptions

For our newly created function, we will be reusing the same data as our new data in theory. As such, we have documented a few assumptions. We have also used a few *try-except* blocks to account for any unforeseen problems that may arise with the new data.

**Assumption #1:** We assume that the data formats are the same. So for wikipedia is a JSON file and the kaggle metadata and ratings are CSV files.

**Assumption #2:** We assume that they are using the function to input the file names in the correct order.

**Assumption #3:** We assume that they have the same file directory and connection string to connect to the PostgreSQL database.

**Assumption #4:** We assume that the number of columns are the same and there are not any new categories being added into the dataset.

**Assumption #5:** We assume that the data structures for the values are the same.

**Assumption #6:** We assume that the only column that has one value is the column 'video'. 

### 1. Extract

When the function is called, it takes in 3 arguments to continue the process.

```
def wikipedia_kaggle_pipeline(wiki_data, kaggle_metadata, ratings_data):
```

After that, we pull in our 3 datas with the following code.

```
file_dir = 'C:/Users/Helen/Movies-ETL/'
    
    # Import kaggle data
    kaggle_metadata = pd.read_csv(f'{file_dir}{kaggle_metadata}', low_memory = False)
    ratings = pd.read_csv(f'{file_dir}{ratings_data}')
    
    # Import wikipedia data
    with open(f'{file_dir}{wiki_data}', mode = 'r') as file:
        wiki_movies_raw = json.load(file)
```
This follows **Assumption #1, #2, and #3** that the data formats are the same, we have the same file directory and the datasets are input in the correct order. After this process has finished, we print a statement saying that files have successfully been extracted.

### 2. Transformation

In the second process, we sort through the 3 datasets to clean unwanted, damaged, and duplicate data. We also chose to drop certain columns and keep the ones that are important for the project. Since we are accounting for the process to automate and repeat on its own daily, we included a few adjustments to take that into account.

We see **Assumption #4** when we comb through the columns and choose which ones to keep and which to drop. 

```
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
```

When we hard-code in the column names, it causes the code to be restrictive and not as robust as we would need it to be for the function to automate and repeat without supervision. As such, we make this assumption, along with adding the codes in *try-except* blocks. This helps with debugging the function when it spits out an error and provide us with a clue to its location.


We determined **Assumption #4 and #5** as we introduced regular expressions into the code. This assumes that the data structures for the values are the same.

```
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
```

Above is an example where we used regular expressions. We used similar lines to parse through several columns within the DataFrame. We assume the columns are the same as we are hard-coding it into our code. As well, the regular expressions written is to match a similar pattern to strings of characters within our DataFrame. 

One thing to note, in our original code, we were not able to include every single value, however we were able to narrow down the list to a minimal where it did not affect our analysis. For future new datasets, we will lose some data in the process, however, if the structures of the data changes dramatically, our code will not be able to create a match and extract enough data to add value to our analysis. As a result, we assume the columns and data structures are the same.

As we parsed through and removed unwanted columns, we determined **Assumption #6**.

```
# As every row is false, we can drop the column
movies_df.drop(columns = 'video', inplace = True)
```

We assume that the *video* column is the only one that has one value. If there are new columns that has only one value as well then we will need to edit the code to delete all columns with only one value.

We also felt the need to add this *try-except* block below.

```
try:
    # Drop the row with an outlier within the following dates
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

except:
    print('There are no outliers within these dates.')
```

As we went through our data, we had an outlier that needed to be removed. As the data is updated and the function runs daily, there may be a chance that there will not be any outliers within the specified range of dates. This line could either be removed from the function or it could be changed to reflect a number of days rather than a range of dates.

After we have taken the assumptions into consideration and the *try-except* blocks have taken into account unforeseen problems, the rest of the transform process should run successfully.

### 3. Load

In our last process, we load the clean data to our PostgreSQL database. Since is it not the initial upload, we have made a minor change to the code. 

Firstly, **Assumption #3**, we assume that the connection string to PostgreSQL database is the same. If it is different, Python will not be able to connect to the database and the data will not be uploaded. 

Secondly, since the tables already exist, we do not want to append another 26 million rows below the first 26 million rows. If the function runs daily, the table would be massive and include many duplicate entries.

```
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
```

As we see in the code, we have changed the *if_exist* parameter for the movies_df to *replace*. If the table already exists, it will drop the data inside the table and append the new data.

With our ratings table, since there are a lot of rows, we loop through the dataset and upload the rows in chunksize of 1 million rows. We have also included a condition within the loop that states if the *rows_imported == 0* to *replace* the existing data with the new data. After that iteration, the *rows_imported* count will increase and the rest of the data will *append* below the data that was uploaded in the first iteration.

We have added a couple of print statements for the user to follow along each iteration and ensure that it is iterating accordingly. At the end, we print a statement to confirm that the ETL process has completed.

## Challenge Summary

The function we have created has satisfied the Extract, Transform, and Load aspects of the process. However, the code can be more robust. Many of the assumptions we have determined were a result of hard-coding. If we would like to overcome these assumptions and create a function that is more robust:

1. Include input prompts.
2. Use regular expressions to account for as many matches to the values as possible. 
3. Use conditions that account for changing data, such as release dates and dropping multiple columns.
4. Write code that will read the column names and autofill the names into functions.

## Usage

**Note:** To use these codes, you should already have the softwares listed above downloaded to your computer.

1. Download the provided resources into the folder you will use for this project.
2. Open the *challenge.py* file to input your information for the following lines:
  
    - file_dir = 'your path to the folder that is holding your data sources'
    - db_string = 'postgres://[user]:[password]@[location]:[port]/[database]'
    - for your password, save it into a *config.py* file and call that variable into the string
    
    > --config.py--
    >
    > db_password = 'Your_Password'
    >
    > --challenge.py--
    >
    > Add this line to the top with your other dependencies:
    >
    > from config import db_password
    >
    > Change the password section of your string with curly brackets {} along with your other information:
    >
    > db_string = 'postgres://[user]:{db_password}@[location]:[port]/[database]'

3. Once everything has been filled, call the function and input the 3 arguments. Ensure to check the order of the input.
4. If you want to see how long the upload will run, change the upload for ratings to reflect the following:

```
# Create variable for the number of rows imported
rows_imported = 0

# Get the start_time from time.time()
start_time = time.time()

for data in pd.read_csv(f'{file_dir}{ratings_data}', chunksize=1000000):

    print(data.head())
    print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='\n')

    if rows_imported == 0:
        # Remove existing data from the ratings file and replace new data
        data.to_sql(name='ratings', con=engine, if_exists='replace')

    else:

        data.to_sql(name='ratings', con=engine, if_exists='append')

    rows_imported += len(data)
    
    # Print that the rows have finished importing and add elasped time
    print(f'Done. {time.time() - start_time} total seconds elapsed.')
    
print(f'ETL process completed.')
```
