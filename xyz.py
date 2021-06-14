import plotly.express as px
import plotly.offline as pyo

df = px.data.iris()
fig = px.scatter(df, x="sepal_width", y="sepal_length", color="species")

fig.update_traces(
    marker=dict(
    symbol='diamond-open',
    size=12,
    line=dict(width=2, color='DarkSlateGrey')),
    selector=dict(mode='markers')
)
pyo.plot(fig, filename='xyz.html')