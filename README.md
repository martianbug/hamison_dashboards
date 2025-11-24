# Tweet Misinformation Analysis Using Sentiment Models and OpenSearch Visualization
This repository contains scripts used for the Hamison Project (https://nlp.uned.es/hamison-project/) in the search for misinformation patterns on social media. Specifically, this repository includes:

* add_column_with_model.py uses language sentiment and emotion models to classify tweets and add a column to the dataset.

* Scripts that manipulate, pre and post-process the data:
    * dataset_preprocess.py normalizes and pre-process datyaframes for the analysis.
    * create_users_df creates a df with active users and their metadata and saves it to a .csv file, later used by re-retweeting 
    * re-retweeting copy certaing column values from a df without retweets and processed by the model to the df with retweets (see doc)

* uploader_csv_to_json_opensearch.py prepares and uploads the data to OpenSearch Dashboards in order to prepare interactive visualizations

## OpenSearch Dashboards
File Hamison OpenSearch Dashboards.ndjson its to be loaded on the [OpenSearch Dashboards](https://docs.opensearch.org/latest/dashboards/) interactive tool. It is necessary to download the docker and run the 'docker-compose.yml' configuration file by executing `docker compose up`

## Data
The data used for this project are the tweets deployed during the 'COP27' Climate convention. They all include hashtags like "#COP27" to filter by this particular event in search for misinformation patterns. This data was extracted using the [Twitter API](https://docs.x.com/fundamentals/developer-apps#overview)

## Dependencies
Use `pip install -r requirements.txt' to automatically install all dependencies used by the different parts of this repository. Apart from them, data_explorer repository by @alpgarcia (https://github.com/alpgarcia/data_explorer/tree/main) is used for some functions. 

## Acknowledgements
This work was supported by the HAMiSoN project grant **CHIST-ERA-21-OSNEM-002**, **AEI PCI2022-135026-2** (MCIN/AEI/10.13039/501100011033 and EU “NextGenerationEU”/PRTR).
