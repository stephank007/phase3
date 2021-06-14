import os
import codecs
import pandas as pd
import yaml
import dash
import dash_core_components as dcc
import dash_html_components as html

with codecs.open('./config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
fn = os.path.join(path, config['config'].get('name'))

df = pd.read_excel(fn, sheet_name='process_ref')
df = df[['domain_id', 'process_id']]

d_dict = {'Select' : None}
keys = df['domain_id'].unique()
for key in keys:
    p_ids = df[df['domain_id'] == key]['process_id'].to_list()
    d_dict.update({key: p_ids})

app = dash.Dash()

fnameDict = {'chriddy': ['opt1_c', 'opt2_c', 'opt3_c'], 'jackp': ['opt1_j', 'opt2_j']}
fnameDict = d_dict

names = list(fnameDict.keys())
nestedOptions = fnameDict[names[0]]

app.layout = html.Div(
    [
        html.Div([
            dcc.Dropdown(
                id='name-dropdown',
                options=[{'label':name, 'value':name} for name in names],
                # value = list(fnameDict.keys())[0]
            ),
        ],style={'width': '20%', 'display': 'inline-block'}),
        html.Div([
            dcc.Dropdown(
                id='opt-dropdown',
            ),
        ],style={'width': '20%', 'display': 'inline-block'}
        ),
        html.Hr(),
        html.Div(id='display-selected-values')
    ]
)

@app.callback(
    dash.dependencies.Output('opt-dropdown', 'options'),
    [dash.dependencies.Input('name-dropdown', 'value')]
)
def update_date_dropdown(name):
    print(update_date_dropdown.__name__, name)
    return [{'label': i, 'value': i} for i in fnameDict[name]]

@app.callback(
    dash.dependencies.Output('display-selected-values', 'children'),
    [dash.dependencies.Input('opt-dropdown', 'value')])
def set_display_children(selected_value):
    print(selected_value)
    return 'you have selected {} option'.format(selected_value)

if __name__ == '__main__':
    app.run_server(debug=True)
