# Tweet Misinformation Analysis Using Sentiment Models and OpenSearch Visualization
This repository contains scripts used for the Hamison Project (https://nlp.uned.es/hamison-project/) in the search for misinformation patterns on social media. Specifically, this repository includes:
· Scripts that use language sentiment and emotion models to process tweets.
· Scripts that manipulate, pre and post-process the data.
· Scripts to prepare and upload the data to OpenSearch Dashboards in order to prepare interactive visualizations

## Data
The data used for this project are the tweets deployed during the 'COP27' Climate convention. They all include hashtags like "#COP27" to filter by this particular event in search for misinformation patterns. This data was extracted using the Twitter API(https://docs.x.com/fundamentals/developer-apps#overview)

# Dependencies
Use `pip install -r requirements.txt' to automatically install all dependencies used by the different parts of this repository. Apart from them, data_explorer repository by @alpgarcia (https://github.com/alpgarcia/data_explorer/tree/main) is used for some functions. 

## Acknowledgements
This work was supported by the HAMiSoN project grant **CHIST-ERA-21-OSNEM-002**, **AEI PCI2022-135026-2** (MCIN/AEI/10.13039/501100011033 and EU “NextGenerationEU”/PRTR).
