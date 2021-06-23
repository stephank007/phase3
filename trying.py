import os
import codecs
import time
import yaml
import pandas as pd
import plotly.express as px
import plotly.offline as pyo
import plotly.graph_objects as go

import numpy as np
np.random.seed(1)

# Generate data
x0 = np.random.normal(2, 0.45, 300)
y0 = np.random.normal(2, 0.45, 300)

x1 = np.random.normal(6, 0.4, 200)
y1 = np.random.normal(6, 0.4, 200)

# Create figure
fig = go.Figure()

# Add scatter traces
fig.add_trace(go.Scatter(x=x0, y=y0, mode="markers"))
fig.add_trace(go.Scatter(x=x1, y=y1, mode="markers"))

# Add shapes
fig.add_shape(type="circle",
    xref="x", yref="y",
    x0=min(x0), y0=min(y0),
    x1=max(x0), y1=max(y0),
    opacity=0.2,
    fillcolor="blue",
    line_color="blue",
)

fig.add_shape(type="circle",
    xref="x", yref="y",
    x0=min(x1), y0=min(y1),
    x1=max(x1), y1=max(y1),
    opacity=0.2,
    fillcolor="orange",
    line_color="orange",
)


# Hide legend
fig.update_layout(showlegend=True)

pyo.plot(fig, filename='xyz.html')
