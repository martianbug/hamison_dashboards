import pandas as pd
from tqdm import tqdm
from multiprocessing import Process, Queue, cpu_count

from utilities import is_rt

# Globals
_global_df2 = None
_global_users = None
_global_indexed_df2 = None
_global_missing_columns = {
                        'user_id_tweets_count',
                        #    'text_length',
                        #    'text_preprocessed_length',
                        #    'text_original_length',
                        #    'text_length_ratio',
                           'pyemotion',
                           'pysentimiento'}
# --- worker initializer to set globals ---
def _init_worker(df_without_rts, users):
    global _global_df2, _global_users, _global_indexed_df2
    _global_df2 = df_without_rts
    _global_indexed_df2 = _global_df2.set_index('user_id', drop=False)
    _global_users = users

# --- worker function that pulls from queue ---
def worker(task_queue: Queue, result_queue: Queue, df_without_rts, users):
    _init_worker(df_without_rts, users)  # initialize globals for this worker

    while True:
        row = task_queue.get()
        if row is None:  # poison pill = stop
            break
        try:
            result = _process_retweet(row)
            result_queue.put(result)
        except Exception as e:
            # Return error as tuple for debugging
            result_queue.put((False, None, None, None))

def load_or_create_pickle(csv_path, pickle_path=None, force_update=False):
    # if pickle_path is None:
    #     pickle_path = os.path.splitext(csv_path)[0] + ".pkl"

    # pickle_exists = os.path.exists(pickle_path)
    # csv_newer = False

    # if pickle_exists:
    #     csv_mtime = os.path.getmtime(csv_path)
    #     pickle_mtime = os.path.getmtime(pickle_path)
    #     csv_newer = csv_mtime > pickle_mtime

    # if force_update or not pickle_exists or csv_newer:
    print(f"Loading from CSV: {csv_path}")
    df = pd.read_csv(csv_path, index_col = 0)
    # print(f"Saving pickle: {pickle_path}")
    # df.to_pickle(pickle_path)
    # else:
        # print(f"Loading from Pickle: {pickle_path}")
        # df = pd.read_pickle(pickle_path)
# 
    return df

def import_files(file_rts, files_norts=None):
    df = load_or_create_pickle(file_rts)
    df2 = load_or_create_pickle(files_norts)
    
    # users = load_or_create_pickle(users_file)
    return df, df2 #,users 

def extract_original_text(rt_text: str, max_len: int = 100) -> str:
    idx = rt_text.find(': ')
    if idx == -1:
        return rt_text[:max_len]
    start = idx + 2
    return rt_text[start:start + max_len]

def find_original_tweet(rt_user_id, text):
    colon_pos = text.find(': ')
    extracted_text = text[colon_pos + 2: colon_pos + 102] if colon_pos != -1 else text[:100]
    try:
        candidates = _global_indexed_df2.loc[rt_user_id]
        if isinstance(candidates, pd.Series):
            candidates = candidates.to_frame().T
        original_tweet = candidates[candidates['text'].str.contains(extracted_text, regex=False)]
        return original_tweet
    except KeyError:
        # print(f"Original tweet not found for RT user {rt_user_id}")
        return pd.DataFrame()

def get_user_id_count(users, user_id):
    try:
        return users[users['user_id'] == user_id].iloc[0]['num_tweets'].item()
    except:
        print(f"User ID {user_id} not found in users dataframe")
        return None

def craft_destination_row(original_tweet, retweet, users):
    aux = retweet.copy()
    try:
        for col in _global_missing_columns:
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
    return aux

def _process_retweet(row):
    global _global_df2, _global_users, _global_missing_columns
    row = pd.Series(row)
    try:
        original_tweet = find_original_tweet(row['rt_user_id'], row['text'])
        if original_tweet.empty:
            return (False, None, row['id'], None)
        
        new_row = craft_destination_row(original_tweet, row, _global_users)
        if new_row['user_id_tweets_count'] > -1:
            return (True, None, None, new_row)
        else:
            return (False, row['user_id'], None, new_row)
    except Exception:
        return (False, None, row['id'], None)

def transfer_RTed_tweets_parallel(df_withhrts, df_without_rts, users, workers=None, chunk_size=1000):
    if workers is None:
        workers = cpu_count()

    retweets = df_withhrts[(df_withhrts['rt_user_id'].notna()) & (df_withhrts['lang'].isin(['en', 'es']))]
    # retweets = retweets.head(100)
    # retweets = df_withhrts[df_withhrts['is_rt'] & (df_withhrts['lang'].isin(['en', 'es']))]
    # df_withhrts['is_rt'] = df_withhrts.apply(is_rt, axis=1)
    # retweets = df_withhrts[df_withhrts['is_rt'] == True]
    
    total_tasks = len(retweets)
    task_queue = Queue(maxsize=workers * 2)
    result_queue = Queue()

    processes = []
    for _ in range(workers):
        p = Process(target=worker, args=(task_queue, result_queue, df_without_rts, users))
        p.start()
        processes.append(p)

    # Feed tasks
    successful_rts = 0
    missing_user_ids = []
    original_tweet_not_found = []
    new_rows = []

    # Convert to dict to reduce serialization cost
    retweet_dicts = (row.to_dict() for _, row in retweets.iterrows())

    with tqdm(total=total_tasks) as pbar:
        for row_dict in retweet_dicts:
            task_queue.put(row_dict)

            # Collect results as soon as they are available
            while not result_queue.empty():
                success, missing_user, orig_404, new_row = result_queue.get()
                if success and new_row is not None:
                    successful_rts += 1
                    new_rows.append(new_row)
                if missing_user is not None:
                    missing_user_ids.append(missing_user)
                    new_rows.append(new_row)
                if orig_404 is not None:
                    original_tweet_not_found.append(orig_404)
                pbar.update(1)
                pbar.set_description(f"Success: {successful_rts} | Missing users: {len(missing_user_ids)} | 404: {len(original_tweet_not_found)}")

    # Send poison pills
    for _ in range(workers):
        task_queue.put(None)

    # Drain remaining results
    processed = pbar.n
    while processed < total_tasks:
        success, missing_user, orig_404, new_row = result_queue.get()
        if success and new_row is not None:
            successful_rts += 1
            new_rows.append(new_row)
        if missing_user is not None:
            missing_user_ids.append(missing_user)
            new_rows.append(new_row)
        if orig_404 is not None:
            original_tweet_not_found.append(orig_404)
        processed += 1
        pbar.set_description(f"Success: {successful_rts} | Missing users: {len(missing_user_ids)} | 404: {len(original_tweet_not_found)}")
        pbar.update(1)

    for p in processes:
        p.join()

    # Combine results
    if new_rows:
        df_without_rts = pd.concat([df_without_rts, pd.DataFrame(new_rows)], ignore_index=True)

    return df_without_rts, missing_user_ids, original_tweet_not_found

def write_logs(missing_users, tweet_404):
    with open('log/missing_user_ids.txt', 'w') as file_:
        file_.write(str(missing_users))
    
    with open('log/original_tweet_404.txt', 'w') as file_:
        file_.write(str(tweet_404))

def main():
    prefix = '../data/'
    # file_rts, files_norts, users_file = (
    # prefix+'cop27_en_filledtext_stance.csv', 
    # prefix+'dataset_23_10_en.csv', 
    # prefix+'usuarios_en_complete.csv')
    
    file_rts, files_norts, users_file = (
    prefix+'cop27_en_filledtext_stance.csv', 
    prefix+'dataset_3_11_en.csv',   
    prefix+'usuarios_en_complete.csv')
    
    # df_withrts, df_without_rts= import_files(file_rts,  files_norts)
    df_withrts = pd.read_csv(file_rts, index_col = 0)
    df_without_rts = pd.read_csv(files_norts)
    
    # df = pd.read_csv(csv_path, index_col = 0)
    # df = pd.read_csv(csv_path, index_col = 0)
    users = pd.read_csv(users_file)
    # print(df_without_rts['created_at'])
    # print(df_withrts['created_at'])
    # print(len(df_without_rts['created_at'].isna()))
    # print(len(df_withrts['created_at'].isna()))
    
    df_without_rts, missing_users, orig_404 = transfer_RTed_tweets_parallel(df_withrts, df_without_rts, users, workers=8)
    try:
        write_logs(missing_users, orig_404)
    except:
        print('Error writing log files')
    
    df_without_rts['rt_user_id'] = df_without_rts['rt_user_id'].fillna(value=-1)
    df_without_rts.to_csv(prefix+'dataset_3_11_extended_en.csv', index=False)

if __name__ == "__main__":
    main()
