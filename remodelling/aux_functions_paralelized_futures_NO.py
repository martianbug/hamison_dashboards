import pandas as pd
from tqdm import tqdm 
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

# Variables globales que estarán disponibles en cada proceso
_global_df2 = None
_global_users = None
_global_indexed_df2 = None
_global_missing_columns = {'sentiment', 
                   'user_id_count', 
                   'text_length', 
                   'text_preprocessed_length',
                   'text_original_length', 
                   'text_length_ratio', 
                   'pyemotion', 
                   'pysentimiento'}

def _init_worker(df2, users):
    """
    Inicializador: se ejecuta una vez por proceso del pool.
    Carga los DataFrames en variables globales para evitar overhead.
    """
    global _global_df2, _global_users, _global_indexed_df2
    _global_df2 = df2
    _global_indexed_df2 = _global_df2.set_index('user_id', drop=False)
    _global_users = users

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

def extract_original_text(rt_text: str, max_len: int = 100) -> str:
    """
    Given a retweet text like 'RT @user: blah: blah more blah',
    returns the substring after the FIRST colon+space, truncated to `max_len` chars.
    """
    idx = rt_text.find(': ')
    if idx == -1:
        # No colon+space found — fallback: return full text truncated
        return rt_text[:max_len]
    start = idx + 2  # skip ': '
    return rt_text[start:start + max_len]

def find_original_tweet(rt_user_id, text):
    # Extract text after first ": " (safer version you requested earlier)
    colon_pos = text.find(': ')
    extracted_text = text[colon_pos + 2: colon_pos + 102] if colon_pos != -1 else text[:100]

    try:
        # Get all tweets by this user (fast O(1))
        candidates = _global_indexed_df2.loc[rt_user_id]

        # Filter by extracted text
        if isinstance(candidates, pd.Series):  # Single row result
            candidates = candidates.to_frame().T

        original_tweet = candidates[candidates['text'].str.contains(extracted_text, regex=False)]

        return original_tweet

    except KeyError:
        return pd.DataFrame()  # No tweets by that user
'''
def find_original_tweet(df2, rt_user_id, text):
    extracted_text = extract_original_text(text)
    original_tweet = df2[(df2['user_id'] ==  rt_user_id) & (df2['text'].str.contains(extracted_text, regex=False))]
    return(original_tweet)
'''
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
    for col in _global_missing_columns:
        if isinstance(original_tweet, pd.DataFrame):
            aux[col] = original_tweet[col].iloc[0]
        else:
            aux[col] = original_tweet[col]
    result = get_user_id_count(users, retweet['user_id'])
    # Si el usuario no existe, se inserta -1
    aux['user_id_count'] = -1 if result is None else result
    return aux

def _process_retweet(row):
    """
    Procesa una sola fila de retweet usando las variables globales.
    Return values: success, missing_user, orig_404, new_row
    """
    global _global_df2, _global_users, _global_missing_columns
    row = pd.Series(row)

    try:
        # Buscar el tweet original
        original_tweet = find_original_tweet(row['rt_user_id'], row['text'])
        # Si esta vacio, no se ha encontrado, y por tanto se devuelve success=False y no se inserta
        if original_tweet.empty:
            return (False, None, row['id'], None)
        
        # Sino, se crea nueva fila
        new_row = craft_destination_row(original_tweet, row, _global_users)

        # Si new_row['user_id_count'] == -1 el usuario no se ha encontrado, pero se inserta igualmente
        if new_row['user_id_count'] > -1:
            print('exito')
            return (True, None, None, new_row)  # éxito
        else:
            return (False, row['user_id'], None, new_row)  # usuario faltante, pero se inserta

    except Exception:
        return (False, None, row['id'], None)

def transfer_RTed_tweets_parallel(df, df2, users, max_workers=4):
    successful_rts = 0
    missing_user_ids = []
    original_tweet_not_found = []
    new_rows = []

    # Filtrar RTs válidos
    retweets = df[(df['rt_user_id'] != -1) & (df['lang'].isin(['en', 'es']))]

    # Ejecutar en paralelo con inicialización
    with ProcessPoolExecutor(
        max_workers=max_workers,
        initializer=_init_worker,
        initargs=(df2, users)
    ) as executor:
        # print(f'Preparing futures: {len(retweets)}')
        # futures = {
        #     executor.submit(_process_retweet, row.to_dict()): idx
        #     for idx, row in retweets.iterrows()
        # }
        
        # print('Futures ready')

        with tqdm(total=len(retweets)) as pbar:
            for result in executor.map(_process_retweet, (row for _, row in retweets.iterrows())):
            #for future in as_completed(futures):
                pbar.update(1)
                if pbar.n % 100 == 0:
                    pbar.set_description(
                    f"Success: {successful_rts} | Missing users: {len(missing_user_ids)} | Original 404: {len(original_tweet_not_found)}"
                )
                try:
                    #success, missing_user, orig_404, new_row = future.result()
                    success, missing_user, orig_404, new_row = result
                    
                    # Si todo ha ido bien, se inserta nueva row
                    if success and new_row is not None:
                        successful_rts += 1
                        new_rows.append(new_row)
                    
                    # Si no se ha encontrado el usuario, se inserta la nueva row
                    # Y se actualiza la lista de ids de usuarios que faltan
                    if missing_user is not None:
                        missing_user_ids.append(missing_user)
                        new_rows.append(new_row)

                    # Si no se ha encontrado el tweet original, no se inserta row
                    # Y se actualiza la lista de tweets no encontrados
                    if orig_404 is not None:
                        original_tweet_not_found.append(orig_404)

                except Exception:
                    tqdm.write('Que haces aqui?')
                    original_tweet_not_found.append(None)

    # Combinar nuevos rows con df2
    if new_rows:
        df2 = pd.concat([df2, pd.DataFrame(new_rows)], ignore_index=True)

    return df2, missing_user_ids, original_tweet_not_found

def write_logs(missing_users, tweet_404):
    with open('missing_user_ids.txt', 'w') as file_:
        file_.write(missing_users)
    
    with open('original_tweet_404.txt', 'w') as file_:
        file_.write(tweet_404)
        
    
def main():
    df, df2, users = import_files()
    df2, missing_users, orig_404 = transfer_RTed_tweets_parallel(
        df,
        df2,
        users,
        max_workers = 4  # Ajusta según tu CPU (8 cores por ejemplo)
        )
    write_logs(missing_users, orig_404)
    df2.to_csv('new_converted.csv', index=False)
    
if __name__ == "__main__":
    main()