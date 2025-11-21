# %%

import pandas as pd
from tqdm import tqdm 
import os

missing_columns = {'user_id_tweets_count', 
                   'pyemotion', 
                   'pysentimiento'}

_global_df2 = None
_global_users = None
_global_indexed_df2 = None

def find_original_tweet(df2, rt_user_id, text):
    extracted_text = text.split(': ')[1][0:100]
    original_tweet = df2[(df2['user_id'] ==  rt_user_id) & (df2['text'].str.contains(extracted_text, regex=False))]
    return(original_tweet)

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
    try:
        for col in missing_columns:
            if isinstance(original_tweet, pd.DataFrame):
                aux[col] = original_tweet[col].iloc[0]
            else:
                aux[col] = original_tweet[col]
        result = get_user_id_count(users, retweet['user_id'])
        aux['user_id_tweets_count'] = -1 if result is None else result
    except Exception as e:
        print(f"Error crafting destination row for retweet ID {retweet['id']}: {e}")
        print(f"Missed con column {col}")
        aux['user_id_tweets_count'] = -1
        result = None
    #debuging lines
    #if result != None:
        #print(aux)
    return(aux, result)

def transfer_RTed_tweets(df_withrts, df_without_rts, users):
    #debug/logging vars
    # missing user ids are those users who haven't been found at the users df
    # original_tweet_not_found contains the RTs ids whose original tweet haven't been found
    successful_rts = 0
    missing_user_ids = []
    original_tweet_not_found = []
    #first identify the RTs in origin (df)
    retweets = df_withrts[(df_withrts['rt_user_id'] != -1) & (df_withrts['lang'].isin(['en', 'es']))]
    retweets = retweets.head(100)
    #list of new rows to insert
    new_rows = []
    #iter through tweets and insert in destination (df2)
    with tqdm(total=len(retweets)) as pbar:
        for row in retweets.iterrows():
            pbar.set_description(f"Success: {successful_rts} | Missing users: {len(missing_user_ids)} | Original tweet 404: {len(original_tweet_not_found)}")
            pbar.update(1)
            row = row[1]
            #find original tweet if that RT
            original_tweet = find_original_tweet(_global_indexed_df2, row['rt_user_id'], row['text'])
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
    df_without_rts = pd.concat([df_without_rts, pd.DataFrame(new_rows)], ignore_index=True)
    return(df_without_rts, missing_user_ids, original_tweet_not_found)
def main():
    prefix = 'data/'
    file_rts, files_norts, users_file = (
    prefix+'cop27_en_filledtext_stance.csv', 
    prefix+'dataset_23_10_en.csv', 
    prefix+'usuarios_en_complete.csv')
    
    df_withrts = pd.read_csv(file_rts, index_col = 0)
    df_without_rts = pd.read_csv(files_norts)
    users = pd.read_csv(users_file)
    
    # df, df2, users = import_files()
    # df2_indexed = df_without_rts.set_index('user_id', drop=False)
    # transfer_RTed_tweets(df, df2, users, df2_indexed)
    # df2.to_csv('new_converted.csv', index=False)
    
    
    global _global_df2, _global_users, _global_indexed_df2
    _global_df2 = df_without_rts
    _global_indexed_df2 = _global_df2.set_index('user_id', drop=False)
    _global_users = users
    
    # df = pd.read_csv(csv_path, index_col = 0)
    # df = pd.read_csv(csv_path, index_col = 0)
    # print(df_without_rts['created_at'])
    # print(df_withrts['created_at'])
    # print(len(df_without_rts['created_at'].isna()))
    # print(len(df_withrts['created_at'].isna()))
    
    df_without_rts, missing_users, orig_404 = transfer_RTed_tweets(df_withrts, df_without_rts, users)
    df_without_rts.to_csv(prefix+'dataset_23_10_en_extended3.csv', index=False)
    
    # try:
    #     write_logs(missing_users, orig_404)
    # except:
    #     print('Error writing log files')
        
main()
