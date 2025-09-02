import pandas as pd
from tqdm import tqdm
from multiprocessing import Process, Queue, cpu_count
from threading import Thread, Lock
import os
import pickle
import lz4.frame

# --- CONFIG ---
CHECKPOINT_FILE = "checkpoint.pkl.lz4"
CHECKPOINT_EVERY = 100000  # rows between checkpoints

# Globals
_global_df2 = None
_global_users = None
_global_indexed_df2 = None
_global_missing_columns = {
    'sentiment', 'user_id_count', 'text_length', 'text_preprocessed_length',
    'text_original_length', 'text_length_ratio', 'pyemotion', 'pysentimiento'
}

# Lock for checkpoint data
_checkpoint_lock = Lock()
_pending_checkpoint = None
_checkpoint_thread = None


# --- Checkpoint helpers (compressed + threaded) ---
def save_checkpoint_async(processed_ids, new_rows):
    """Trigger async save using a separate thread with compression."""
    global _pending_checkpoint, _checkpoint_thread

    # Copy data snapshot safely
    with _checkpoint_lock:
        _pending_checkpoint = {
            "processed_ids": processed_ids.copy(),
            "new_rows": new_rows.copy()
        }

    # Start thread if not already running
    if _checkpoint_thread is None or not _checkpoint_thread.is_alive():
        _checkpoint_thread = Thread(target=_save_checkpoint_worker)
        _checkpoint_thread.daemon = True
        _checkpoint_thread.start()


def _save_checkpoint_worker():
    """Worker thread to write checkpoint compressed."""
    global _pending_checkpoint

    with _checkpoint_lock:
        data_to_save = _pending_checkpoint
        _pending_checkpoint = None

    if data_to_save is None:
        return

    try:
        with lz4.frame.open(CHECKPOINT_FILE, mode="wb") as f:
            pickle.dump(data_to_save, f)
    except Exception as e:
        print(f"Error writing checkpoint: {e}")


def load_checkpoint():
    """Load compressed checkpoint file if exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with lz4.frame.open(CHECKPOINT_FILE, mode="rb") as f:
            return pickle.load(f)
    return {"processed_ids": set(), "new_rows": []}


# --- init worker ---
def _init_worker(df2, users):
    global _global_df2, _global_users, _global_indexed_df2
    _global_df2 = df2
    _global_indexed_df2 = _global_df2.set_index('user_id', drop=False)
    _global_users = users


# --- worker function ---
def worker(task_queue: Queue, result_queue: Queue, df2, users):
    _init_worker(df2, users)  # initialize globals for this worker

    while True:
        row = task_queue.get()
        if row is None:  # poison pill
            break
        try:
            result = _process_retweet(row)
            result_queue.put((result, row['id']))  # attach row id for checkpointing
        except Exception:
            result_queue.put(((False, None, None, None), row['id']))


# --- utility functions ---
def load_or_create_pickle(csv_path, pickle_path=None, force_update=False):
    if pickle_path is None:
        pickle_path = os.path.splitext(csv_path)[0] + ".pkl"

    pickle_exists = os.path.exists(pickle_path)
    csv_newer = False

    if pickle_exists:
        csv_mtime = os.path.getmtime(csv_path)
        pickle_mtime = os.path.getmtime(pickle_path)
        csv_newer = csv_mtime > pickle_mtime

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
    df = load_or_create_pickle('big_files/dataset_26_05_sb2.csv')
    df2 = load_or_create_pickle('big_files/dataset_06_05_sb2.csv')
    users = load_or_create_pickle('big_files/usuarios.csv')
    return df, df2, users


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
        return pd.DataFrame()


def get_user_id_count(users, user_id):
    try:
        return users[users['user_id'] == user_id].iloc[0]['num_tweets'].item()
    except:
        return None


def craft_destination_row(original_tweet, retweet, users):
    aux = retweet.copy()
    for col in _global_missing_columns:
        if isinstance(original_tweet, pd.DataFrame):
            aux[col] = original_tweet[col].iloc[0]
        else:
            aux[col] = original_tweet[col]
    result = get_user_id_count(users, retweet['user_id'])
    aux['user_id_count'] = -1 if result is None else result
    return aux


def _process_retweet(row):
    global _global_df2, _global_users, _global_missing_columns
    row = pd.Series(row)
    try:
        original_tweet = find_original_tweet(row['rt_user_id'], row['text'])
        if original_tweet.empty:
            return (False, None, row['id'], None)
        
        new_row = craft_destination_row(original_tweet, row, _global_users)
        if new_row['user_id_count'] > -1:
            return (True, None, None, new_row)
        else:
            return (False, row['user_id'], None, new_row)
    except Exception:
        return (False, None, row['id'], None)


# --- main parallel logic with threaded + compressed checkpointing ---
def transfer_RTed_tweets_parallel(df, df2, users, workers=None):
    if workers is None:
        workers = cpu_count()

    # Load checkpoint
    checkpoint = load_checkpoint()
    processed_ids = checkpoint["processed_ids"]
    new_rows = checkpoint["new_rows"]

    # Filter retweets (skip processed ones)
    retweets = df[(df['rt_user_id'] != -1) & (df['lang'].isin(['en', 'es']))]
    retweets = retweets[~retweets['id'].isin(processed_ids)]

    total_tasks = len(retweets)

    if total_tasks == 0:
        print("No new tasks to process (all rows processed).")
        return df2, [], []

    # Queues
    task_queue = Queue(maxsize=workers * 2)
    result_queue = Queue()

    # Start workers
    processes = []
    for _ in range(workers):
        p = Process(target=worker, args=(task_queue, result_queue, df2, users))
        p.start()
        processes.append(p)

    # Feed tasks
    successful_rts = 0
    missing_user_ids = []
    original_tweet_not_found = []

    # Convert to dict to reduce serialization cost
    retweet_dicts = (row.to_dict() for _, row in retweets.iterrows())

    processed_count = 0

    with tqdm(total=total_tasks) as pbar:
        for row_dict in retweet_dicts:
            task_queue.put(row_dict)

            # Collect results
            while not result_queue.empty():
                (success, missing_user, orig_404, new_row), row_id = result_queue.get()

                processed_ids.add(row_id)
                processed_count += 1

                if success and new_row is not None:
                    successful_rts += 1
                    new_rows.append(new_row)
                if missing_user is not None:
                    missing_user_ids.append(missing_user)
                    new_rows.append(new_row)
                if orig_404 is not None:
                    original_tweet_not_found.append(orig_404)

                # Progress bar
                pbar.update(1)
                pbar.set_description(
                    f"Success: {successful_rts} | Missing users: {len(missing_user_ids)} | 404: {len(original_tweet_not_found)}"
                )

                # Save checkpoint asynchronously
                if processed_count % CHECKPOINT_EVERY == 0:
                    save_checkpoint_async(processed_ids, new_rows)

    # Poison pills
    for _ in range(workers):
        task_queue.put(None)

    # Drain remaining results
    processed = processed_count
    while processed < total_tasks:
        (success, missing_user, orig_404, new_row), row_id = result_queue.get()

        processed_ids.add(row_id)
        processed += 1

        if success and new_row is not None:
            successful_rts += 1
            new_rows.append(new_row)
        if missing_user is not None:
            missing_user_ids.append(missing_user)
            new_rows.append(new_row)
        if orig_404 is not None:
            original_tweet_not_found.append(orig_404)

        pbar.set_description(
            f"Success: {successful_rts} | Missing users: {len(missing_user_ids)} | 404: {len(original_tweet_not_found)}"
        )
        pbar.update(1)

        if processed % CHECKPOINT_EVERY == 0:
            save_checkpoint_async(processed_ids, new_rows)

    # Join workers
    for p in processes:
        p.join()

    # Final save
    save_checkpoint_async(processed_ids, new_rows)

    # Ensure last checkpoint thread completes
    if _checkpoint_thread:
        _checkpoint_thread.join()

    # Merge with df2
    if new_rows:
        df2 = pd.concat([df2, pd.DataFrame(new_rows)], ignore_index=True)

    return df2, missing_user_ids, original_tweet_not_found


# --- log writing ---
def write_logs(missing_users, tweet_404):
    with open('log/missing_user_ids.txt', 'w') as file_:
        file_.write(str(missing_users))
    
    with open('log/original_tweet_404.txt', 'w') as file_:
        file_.write(str(tweet_404))


def main():
    df, df2, users = import_files()
    df2, missing_users, orig_404 = transfer_RTed_tweets_parallel(df, df2, users, workers=8)
    try:
        write_logs(missing_users, orig_404)
    except:
        print('Error writing log files')
    df2.to_csv('big_files/new_converted.csv', index=False)


if __name__ == "__main__":
    main()
