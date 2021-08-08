import os
import codecs
import yaml
import warnings as warning
import pandas as pd
import dash
import flask
from flask import Flask
from datetime import date
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_bootstrap_components as dbc
import app_body as ap

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125 * '=')

today = date.today()
with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

server = Flask(__name__)
app = dash.Dash(
    __name__,
    title='MRS-Caesar:Phase3-App',
    server=server,
    assets_folder=config['config'].get('assets'),
    suppress_callback_exceptions=True
)

app.layout = html.Div(
    [
        ap.app_navbar(app),
        dbc.Container(id="page-content", fluid=True),
    ], lang='he', dir='rtl'
)
""" domain dropdown """
@app.callback(
    [
        Output('process-dropdown', 'options'),
        Output('domain-id', 'data')
    ],
    [Input('domain-dropdown', 'value')]
)
def update_dropdown(name):
    return ap.update_dropdown(name)

""" process dropdown """
@app.callback(
    [
        Output('selected-row-text'   , 'children'),
        Output('marimekko-figure'    , 'figure'),
        Output('df-requirements-data', 'data'),
        Output('rule-id', 'data')
    ],
    [
        Input('process-dropdown', 'value'),
        Input('rules-dropdown', 'value'),
        Input('domain-id', 'data'),
        Input('clear', 'n_clicks')
    ]
)
def update_dropdown(selected_process, selected_rule, stored_domain, n_clicks):
    if stored_domain and not selected_process:
        selected_process = None
    return ap.home_page_filter_data(stored_domain, selected_process, selected_rule)

@app.callback(
    Output('requirements-table', 'children'),
    [
        Input('process-dropdown'    , 'value'),
        Input('df-requirements-data', 'data'),
        Input('marimekko-figure'    , 'clickData'),
        Input('clear', 'n_clicks')
    ]
)
def requirements_status(selection, df_requirements, click, n_clicks):
    change_id = [p['prop_id'] for p in dash.callback_context.triggered]
    t1 = None
    if click is not None:
        t1 = click.get('points')[0].get('customdata')[0]

    if 'n_clicks' in change_id[0]:
        t1 = None

    df_requirements = pd.DataFrame(df_requirements)
    requirements_table = ap.display_requirements_table(dff=df_requirements, selected_month=t1)
    return requirements_table

@app.callback(
    Output('gantt-chart', 'figure'),
    [Input('rule-radio-items', 'value')],
)
def gantt_rendition(rule):
    return ap.gantt_page_filter_data(rule)

@ app.callback(
    Output("page-content", "children"),
    [
        Input("url", "pathname"),
    ]
)
def render_page_content(pathname):
    if pathname == "/":
        return ap.home_page()
    elif pathname == "/page-1":
        return html.Div(ap.page_1(), lang='he', dir='ltr')
    elif pathname == "/page-2":
        return ap.page_2()
    # If the user tries to reach a different page, return a 404 message
    return dbc.Jumbotron(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ]
    )

def toggle_navbar_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

app.callback(
    Output('navbar-collapse', 'is_open'),
    [Input('navbar-toggle', 'n_clicks')],
    [State('navbar-collapse', 'is_open')],
)(toggle_navbar_collapse)

@server.route('/favicon.ico')
def favicon():
    return flask.send_from_directory(os.path.join(server.root_path, 'assets'), 'favicon.ico')

if __name__ == '__main__':
    app.run_server(port=8050, debug=True)