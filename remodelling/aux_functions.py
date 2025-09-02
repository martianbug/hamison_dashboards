# %%

import pandas as pd
from tqdm import tqdm 
import os

missing_columns = {'sentiment', 
                   'user_id_count', 
                   'text_length', 
                   'text_preprocessed_length',
                   'text_original_length', 
                   'text_length_ratio', 
                   'pyemotion', 
                   'pysentimiento'}

def load_or_create_pickle(csv_path, pickle_path=None, force_update=False):
    """
    Load DataFrame from pickle if available; otherwise from CSV and save as pickle.
    
    Args:
        csv_path (str): Path to the original CSV file.
        pickle_path (str): Path to the pickle file (defaults to same name as CSV with .pkl).
        force_update (bool): If True, always reload from CSV and overwrite pickle.

    Returns:
        pd.DataFrame: Loaded DataFrame
    """
    if pickle_path is None:
        pickle_path = os.path.splitext(csv_path)[0] + ".pkl"

    # Determine if pickle exists and is up-to-date
    pickle_exists = os.path.exists(pickle_path)
    csv_newer = False

    if pickle_exists:
        csv_mtime = os.path.getmtime(csv_path)
        pickle_mtime = os.path.getmtime(pickle_path)
        csv_newer = csv_mtime > pickle_mtime  # CSV modified after pickle

    # Decide loading strategy
    if force_update or not pickle_exists or csv_newer:
        print(f"Loading from CSV: {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"Saving pickle: {pickle_path}")
        df.to_pickle(pickle_path)
    else:
        print(f"Loading from Pickle: {pickle_path}")
        df = pd.read_pickle(pickle_path)

    return df

def import_files():
    #df = pd.read_csv('big_files/dataset_26_05_sb2.csv')
    #df2 = pd.read_csv('big_files/dataset_06_05_sb2.csv')
    #users = pd.read_csv('big_files/usuarios.csv')
    df = load_or_create_pickle('big_files/dataset_26_05_sb2.csv')
    df2 = load_or_create_pickle('big_files/dataset_06_05_sb2.csv')
    users = load_or_create_pickle('big_files/usuarios.csv')
    return(df, df2, users)
'''
def find_original_tweet(df2, rt_user_id, text):
    extracted_text = text.split(': ')[1][0:100]
    original_tweet = df2[(df2['user_id'] ==  rt_user_id) & (df2['text'].str.contains(extracted_text, regex=False))]
    return(original_tweet)
    '''
def find_original_tweet(df2_indexed, rt_user_id, text):
    # Extract text after first ": " (safer version you requested earlier)
    colon_pos = text.find(': ')
    extracted_text = text[colon_pos + 2: colon_pos + 102] if colon_pos != -1 else text[:100]

    try:
        # Get all tweets by this user (fast O(1))
        candidates = df2_indexed.loc[rt_user_id]

        # Filter by extracted text
        if isinstance(candidates, pd.Series):  # Single row result
            candidates = candidates.to_frame().T

        original_tweet = candidates[candidates['text'].str.contains(extracted_text, regex=False)]

        return original_tweet

    except KeyError:
        return pd.DataFrame()  # No tweets by that user

def get_user_id_count(users, user_id):
    """
    Returns the num_tweets for a given user_id.
    If user_id is not found, returns None (instead of printing or raising).
    """
    try:
        return users[users['user_id']==user_id].iloc[0]['num_tweets'].item()
    except:
        return None

def craft_destination_row(original_tweet, retweet, users):
    #make a copy of the row that is the retweet, add the missing columns in destination and recalculate
    #the value that change, like user_id_count. The rest remain the same
    aux = retweet.copy()
    for col in missing_columns:
        if isinstance(original_tweet, pd.DataFrame):
            aux[col] = original_tweet[col].iloc[0]
        else:
            aux[col] = original_tweet[col]
    result = get_user_id_count(users, retweet['user_id'])
    aux['user_id_count'] = -1 if result is None else result
    #debuging lines
    #if result != None:
        #print(aux)
    return(aux, result)

def transfer_RTed_tweets(df, df2, users, df2_indexed):
    #debug/logging vars
    # missing user ids are those users who haven't been found at the users df
    # original_tweet_not_found contains the RTs ids whose original tweet haven't been found
    successful_rts = 0
    missing_user_ids = []
    original_tweet_not_found = []
    #first identify the RTs in origin (df)
    retweets = df[(df['rt_user_id'] != -1) & (df['lang'].isin(['en', 'es']))]
    #list of new rows to insert
    new_rows = []
    #iter through tweets and insert in destination (df2)
    with tqdm(total=len(retweets)) as pbar:
        for row in retweets.iterrows():
            pbar.set_description(f"Success: {successful_rts} | Missing users: {len(missing_user_ids)} | Original tweet 404: {len(original_tweet_not_found)}")
            pbar.update(1)
            row = row[1]
            #find original tweet if that RT
            original_tweet = find_original_tweet(df2_indexed, row['rt_user_id'], row['text'])
            if original_tweet.empty:
                original_tweet_not_found.append(row['id'])
                continue
            new_row, result = craft_destination_row(original_tweet, row, users) 
            #pbar.write(f'Result: {result}\nNew row: \n----------------\n{new_row}')
            if result is not None:
                #insert the new row in destination
                successful_rts += 1
                new_rows.append(new_row)
            else:
                missing_user_ids.append(row['user_id'])
            #print(original_tweet)
    df2 = pd.concat([df2, pd.DataFrame(new_rows)], ignore_index=True)

def main():
    df, df2, users = import_files()
    df2_indexed = df2.set_index('user_id', drop=False)
    transfer_RTed_tweets(df, df2, users, df2_indexed)
    df2.to_csv('new_converted.csv', index=False)
    
main()
