# Tweet Misinformation Analysis Using Sentiment Models and OpenSearch Visualization
This repository contains scripts used for the [Hamison Project](https://nlp.uned.es/hamison-project/) in the search for misinformation patterns on social media. Specifically, this repository includes:

* add_column_with_model.py uses language sentiment and emotion models to classify tweets and add a column to the dataset.

* Scripts that manipulate, pre and post-process the data (inside data_process folder):
    * dataset_preprocess.py normalizes and pre-process datyaframes for the analysis.
    * create_users_df creates a df with active users and their metadata and saves it to a .csv file, later used by re-retweeting 
    * re-retweeting copy certaing column values from a df without retweets and processed by the model to the df with retweets (see doc)

* uploader_csv_to_json_opensearch.py prepares and uploads the data to OpenSearch Dashboards in order to prepare interactive visualizations

## OpenSearch Dashboards
File Hamison OpenSearch Dashboards.ndjson its to be loaded on the [OpenSearch Dashboards](https://docs.opensearch.org/latest/dashboards/) interactive tool. It is necessary to download the docker and run the 'docker-compose.yml' configuration file by executing `docker compose up`

## Step-by-step tutorial
If you desire to use these scripts to obtain the final json bulk files to upload to Opensearch, this is what yoiu should do:
1. Run add_column_with_model.py, by changing de DATASET variable with your filename. Also specify which model you want to apply and the language.
    IMPORTANT: Only original tweets are processed in order to accelerate the process, to be later re-retweeted with re-retweeting script. You can disable this if you wan to process all tweets.

    This script will save a new csv file with the TASK on its name.

2. Once you have run the model/s to your csv, run data_preprocess.py to
homogeneize text and columns.

3. Then run create_users_df.py to have a csv file containing users and some info about them. This is optional, but mandatory if you want to run re-retweeting script.

4. Re-tweeting script (In case you processed only original tweets). You need to have the csv with original tweets csv and new created columns, the csv with all tweets and the users csv created in create_usres_df.
This will apply these new columns values to the RTs, by finding the original tweets.

5. Once you have your final information to upload to OpenSearch, finally uploader_csv_to_json_to_opensearch.py can be run to upload it.


## Data
The data used for this project are the tweets deployed during the 'COP27' Climate convention. They all include hashtags like "#COP27" to filter by this particular event in search for misinformation patterns. This data was extracted using the [Twitter API](https://docs.x.com/fundamentals/developer-apps#overview)

## Dependencies
Use `pip install -r requirements.txt' to automatically install all dependencies used by the different parts of this repository. Apart from them, [data_explorer](https://github.com/alpgarcia/data_explorer/tree/main) repository by @alpgarcia was used for some functions. 

## Acknowledgements
This work was supported by the HAMiSoN project grant **CHIST-ERA-21-OSNEM-002**, **AEI PCI2022-135026-2** (MCIN/AEI/10.13039/501100011033 and EU “NextGenerationEU”/PRTR).
