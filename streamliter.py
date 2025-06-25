import streamlit as st
import pandas as pd
import plotly.express as px

@st.cache_data
def load_data():
    df = pd.read_csv("usuarios.csv", parse_dates=['user_created_at'])
    return df

df = load_data()

st.title("📊 Explorador de Usuarios de Twitter")

st.sidebar.header("Filtros")

min_tweets = st.sidebar.slider("Número mínimo de tweets", 0, int(df['num_tweets'].max()), 1)
min_retweets = st.sidebar.slider("Número mínimo de retweets recibidos", 0, int(df['num_retweets'].max()), 0)
edad_max = st.sidebar.slider("Edad máxima de la cuenta (días)", 0, int(df['user_age_days'].max()), int(df['user_age_days'].max()))

sentimientos = st.sidebar.multiselect(
    "Filtrar por sentimiento mayoritario",
    options=df['majority_sentiment'].unique(),
    default=df['majority_sentiment'].unique()
)

df_filtrado = df[
    (df['num_tweets'] >= min_tweets) &
    (df['num_retweets'] >= min_retweets) &
    (df['user_age_days'] <= edad_max) &
    (df['majority_sentiment'].isin(sentimientos))
]

st.subheader("Usuarios filtrados")
st.write(f"{len(df_filtrado)} usuarios encontrados.")
st.dataframe(df_filtrado[['user_name', 'num_tweets', 'num_retweets', 'user_age_days', 'majority_sentiment']])

st.subheader("Ranking de usuarios con más retweets")
ranking = df_filtrado.sort_values(by='num_retweets', ascending=False).head(10)
fig_ranking = px.bar(ranking, x='user_name', y='num_retweets', color='majority_sentiment', title="Top 10 usuarios más retuiteados")
st.plotly_chart(fig_ranking)

st.subheader("📈 Distribución de sentimientos")
fig_sent = px.histogram(df_filtrado, x='majority_sentiment', color='majority_sentiment', title="Sentimiento mayoritario por usuario")
st.plotly_chart(fig_sent)

st.subheader("🔍 Inspección de un usuario")
usuario_seleccionado = st.selectbox("Selecciona un usuario", df_filtrado['user_name'].unique())
usuario_info = df_filtrado[df_filtrado['user_name'] == usuario_seleccionado].iloc[0]

st.markdown(f"""
**ID:** {usuario_info['user_id']}  
**Tweets:** {usuario_info['num_tweets']}  
**Retweets realizados:** {usuario_info['num_retweets']}
**Retweets recibidos:** {usuario_info['num_retweeters']}
**Edad de la cuenta:** {usuario_info['user_age_days']} días  
**Sentimiento predominante:** {usuario_info['majority_sentiment']}  
""")
