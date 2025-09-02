# divide_bulk.py
import os

input_file = "tweets_and_retweets.json"
lines_per_file = 10000  # 5000 documentos

with open(input_file, "r", encoding="utf-8") as f:
    part = 1
    buffer = []

    output_dir = "retweets_parts"
    os.makedirs(output_dir, exist_ok=True)
    
    for i, line in enumerate(f, 1):
        buffer.append(line)
        if i % lines_per_file == 0:
            outname = os.path.join(output_dir, f"tweets_and_retweets_part_{part}.json")
            # outname = f"tweets_and_retweets_part_{part}.json"
            with open(outname, "w", encoding="utf-8") as out:
                out.writelines(buffer)
            print(f"Escribiendo {outname}")
            buffer = []
            part += 1


            if buffer:
                outname = os.path.join(output_dir, f"tweets_and_retweets_part_{part}.json")
                with open(outname, "w", encoding="utf-8") as out:
                    out.writelines(buffer)
                print(f"Escribiendo {outname}")
