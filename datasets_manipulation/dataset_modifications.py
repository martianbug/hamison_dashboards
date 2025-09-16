from pandasgui import show
import pandas as pd

NAME = 'Top Retweet Users per Emotion And Sentiment'
df = pd.read_csv(NAME + '.csv')

df['Count'] = df['Count'].str.replace(',','').astype(int)
df_filtered = df.query("Count > 500")
df_filtered.drop('Count.1', axis=1, inplace=True)



df.head(500).to_csv('Top_RT_users_emotion_and_sentiment.csv', index=False)
# df_filtered.to_csv('Most_RT_users_emotion_and_sentiment.csv', index=False)
show(df)


