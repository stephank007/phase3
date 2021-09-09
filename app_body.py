import os
import codecs
import pandas as pd
import numpy as np
import warnings as warning
import yaml
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table as dt

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125 * '=')

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

d_gantt = config['dates'].get('gantt')
d_int_1 = config['dates'].get('int_1')
d_int_2 = config['dates'].get('int_2')

pc       = '% Complete'
holidays = []
for d in config['bdays'].get('holidays'):
    holidays.append(pd.to_datetime(d))
weekmask   = config['bdays'].get('bdays')
custombday = pd.offsets.CustomBusinessDay(weekmask=weekmask, holidays=holidays)

x = px.colors.qualitative.Pastel
c_sequence = [
    'rgb(102, 197, 204, 0.1)',
    'rgb(246, 207, 113, 0.1)',
    'rgb(248, 156, 116, 0.1)',
    'rgb(220, 176, 242, 0.1)',
    'rgb(135, 197, 95, 0.1)'
]

holidays = pd.to_datetime(config['holidays'])
(MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
weekends = (FRI, SAT)

path = config['config'].get('path')
fn = os.path.join(path, config['config'].get('gantt'))
f1 = os.path.join(path, config['config'].get('pmo'))
f2 = os.path.join(path, config['config'].get('name'))
f3 = os.path.join(path, config['config'].get('p3BI'))
f4 = os.path.join(path, '_in/motd.xlsx')
f5 = os.path.join(path, config['config'].get('risk-out'))

today    = pd.to_datetime('today')
if today.day < 16:
    today_mm = ( today - pd.DateOffset(months=1) ).strftime('%Y-%m')
else:
    today_mm = today.strftime('%Y-%m')

print('today_mm: ', today_mm)

deadline = pd.to_datetime('2020/10/31', format='%Y/%m/%d')
x_start  = pd.to_datetime('2020/08/01', format='%Y/%m/%d')

style_cell   = config['style_cell']
style_header = config['style_header']

df    = pd.read_excel(fn, sheet_name='gantt-data')
df_int= pd.read_excel(fn, sheet_name='int')
df_ct = pd.read_excel(fn, sheet_name='completed')
df_rd = pd.read_excel(fn, sheet_name='raw-dev')
df_bm = pd.read_excel(fn, sheet_name='bl-month')
df_gt = pd.read_excel(fn, sheet_name='g-totals')
df_evm= pd.read_excel(fn, sheet_name='EVM')
df_pd = pd.read_excel(fn, sheet_name='process_dates')
df_pr = pd.read_excel(f2, sheet_name='process_ref')
df_bi = pd.read_excel(f3, sheet_name='Task_Table1')
motd  = pd.read_excel(f4, sheet_name='Sheet2')
df_rsk= pd.read_excel(f5, sheet_name='risk_table')
df_rss= pd.read_excel(f5, sheet_name='risk_summary')

df_rd = df_rd.rename(columns={'Finish_Date': 'Finish'})
pc_level = df_bm['pc_level'].values[0]
print('% Complete Level: {}'.format(pc_level))

# df_dl.sort_values(['RN'], ascending=False, inplace=True)
df_mj = pd.read_excel(f1, sheet_name='major')
df_pr = df_pr[['domain_id', 'process_id']]
domains = df_pr['domain_id'].unique()

""" Closed Requirements """
df_closed = df_rd[( df_rd['month'] < today_mm ) & ( df_rd[pc] >= pc_level )]
df_closed = df_closed.copy()
df_closed = df_closed[[pc, 'value', 'Name', 'process_id', 'xRN']]
""" Finish Closed Requirements """

""" Open Requirements """
df_open = df_rd[( df_rd['month'] < today_mm ) & ( df_rd[pc] < pc_level )]
df_open = df_open.copy()
df_open = df_open[[pc, 'old_budget', 'Name', 'process_id', 'xRN']]
""" Finish Open Requirements """
YTD_closed = df_gt['YTD_Closed'].values[0] / (df_gt['YTD_Open'].values[0] + df_gt['YTD_Closed'].values[0])
YTD_closed = f'{YTD_closed * 100:.1f}%'
closed_pc_text = 'אחוז מימוש מצטבר {}'.format(YTD_closed)

closed_text1 = '{} דרישות סגורות'.format(df_gt['YTD_Closed'].values[0])
closed_text  ='במשקל {:,} נקודות'.format(int(df_closed['value'].sum()))

open_text1    = '{} דרישות פתוחות'.format(df_gt['YTD_Open'].values[0])
open_text     ='במשקל {:,} נקודות. '.format(int(df_open['old_budget'].sum()))
open_69       = '{} דרישות מתחת ל-69%'.format(df_gt['open_lt_69'].values[0])
pc_level_text = 'סף לחישוב התקדמות: {}%'.format(int(pc_level * 100))
# open_text += open_69

df_closed['value'] = df_closed['value'].apply(lambda x: f'{x:.2f}')
df_open['value']   = df_open['old_budget'].apply(lambda x: f'{x:.2f}')
df_open = df_open[[pc, 'value', 'Name', 'process_id', 'xRN']]

process_dict = {'select all...': ['select all...']}
for x_key in domains:
    process_ids = df_pr[df_pr['domain_id'] == x_key]['process_id'].to_list()
    process_dict.update({x_key: process_ids})

rules = df_rd['rule'].unique()
rules_dict = [{'label': i, 'value': i} for i in rules]

line_1 = motd.loc[0, 'text']
line_2 = motd.loc[1, 'text']

def annotation_position(*args, **kwargs):
    figure = args[0]
    figure.add_annotation(
        yref        =  'paper',
        text        =  kwargs.get('text'),
        x           =  kwargs.get('x'),
        y           =  kwargs.get('y'),
        ax          =  kwargs.get('ax'),
        ay          =  kwargs.get('ay'),
        showarrow   =  True,
        align       =  'center',
        arrowhead   =  2,
        arrowsize   =  3,
        arrowcolor  =  '#636363',
        bordercolor =  '#c7c7c7',
        borderwidth =  1,
        borderpad   =  4,
        # bgcolor     =  '#ff7f0e',
        bgcolor     = kwargs.get('bgcolor'),
        opacity     =  0.8,
        font        =  dict(size=18, color='black')
    )
    return figure

def annotation_style_position(figure=go.Figure(), text=str, x_position=float):
    figure.add_annotation(
        yref        =  'paper',
        text        =  text,
        x           =  x_position,
        y           =  0.95,
        showarrow   =  True,
        align       =  'center',
        arrowhead   =  2,
        arrowsize   =  1,
        arrowcolor  =  '#636363',
        ax          =  70,
        ay          =  -30,
        bordercolor =  '#c7c7c7',
        borderwidth =  2,
        borderpad   =  4,
        bgcolor     =  '#ff7f0e',
        opacity     =  0.8
    )
    return figure

def home_page():
    layout = dbc.Container([
        dbc.Row([
            dbc.Col(
                dbc.Jumbotron([
                    html.H1('מסר השבוע', className='display-3'),
                    dbc.Alert(line_1, color='#325d88', style={'font-size': 28}),
                    html.Hr(),
                    dbc.Alert(line_2, color='secondary', style={'font-size': 18}),
                ]),
                style={
                    'textAlign' : 'center',
                }, width=12
            ),
        ]),
        dbc.Row([
            dbc.Col(
               dcc.Graph(id='bullet_chart', figure=f_bullet), width=12
            ),
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='evm-figure', figure=f_evm), width=8
            ),
            dbc.Col(
                dcc.Graph(id='evm-figure', figure=f_spi), width=4
            )
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H5(closed_text1, className="card-title",
                                        style={'textAlign': 'center', 'font-size': 48}),
                        html.P(closed_text,
                               style={'textAlign': 'center', 'font-size': 24, 'color': 'Yellow'}),
                        html.P('עד לסוף חודש: ' + today_mm, style={'textAlign': 'center', 'font-size': 18, 'color': 'Yellow'}),
                        dbc.Button('פירוט דרישות', id="open-closed-table", size='lg',
                                   color='secondary', outline=False,
                                   block=False),
                    ]), color='#325d88', inverse=True
                ), width={'size': 3, 'offset': 0}
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H5(open_text1, className="card-title",
                                style={'textAlign': 'center', 'font-size': 48}),
                        html.P(open_text,
                               style={'textAlign': 'center', 'font-size': 24, 'color': 'Yellow'}),
                        html.P('עד לסוף חודש: ' + today_mm, style={'textAlign': 'center', 'font-size': 18, 'color': 'Yellow'}),
                        dbc.Button('פירוט דרישות', id="open-open-table", size='lg',
                                   color='secondary', outline=False,
                                   block=False),
                    ]), color='#325d88', inverse=True
                ), width={'size': 3, 'offset': 0}
            ),
            dbc.Col(
                html.Div([
                    dbc.Alert(
                        '',
                        color='warning',
                        style={'textAlign': 'center', 'font-size': 24, 'color': '#8c564b'},
                    ),
                    dbc.Alert(
                        pc_level_text,
                        color='success',
                        style={'textAlign': 'center', 'font-size': 24, 'color': '#8c564b'},
                    ),
                ]), width={'size': 2, 'offset': 2}
            )
        ]),
        dbc.Modal([
            dbc.ModalBody(
                dt.DataTable(
                    data=df_closed.to_dict('records'),
                    columns=[{'name': i, 'id': i, 'deletable': False} for i in df_closed.columns if i != 'id'],
                    style_cell=style_cell,
                    style_header=style_header,
                    filter_action='native',
                    sort_action='native',
                    sort_mode='single',
                    page_action='native',
                    page_current=0,
                    page_size=20,
                    export_format='xlsx',
                    export_headers='display'
                )
            ),
            dbc.ModalFooter(
                dbc.Button('סגור', id='close-closed-xl', className='ml-auto')
            )
        ], id='modal-closed-table', size='xl', fade=True),
        dbc.Modal([
            dbc.ModalBody(
                dt.DataTable(
                    data=df_open.to_dict('records'),
                    columns=[{'name': i, 'id': i, 'deletable': False} for i in df_open.columns if i != 'id'],
                    style_cell=style_cell,
                    style_header=style_header,
                    filter_action='native',
                    sort_action='native',
                    sort_mode='single',
                    page_action='native',
                    page_current=0,
                    page_size=20,
                    export_format = 'xlsx',
                    export_headers = 'display'
    )
            ),
            dbc.ModalFooter(
                dbc.Button('סגור', id='close-open-xl', className='ml-auto')
            )
        ], id='modal-open-table', size='xl', fade=True),
        html.Hr(),
        dbc.Row([
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='domain-dropdown',
                        options=[{'label': i, 'value': i} for i in domains],
                        placeholder='select all...',
                        value='select all...',
                        searchable=True,
                        optionHeight=26,
                    )],
                    style={
                        'textAlign': 'right',
                        'align-items': 'right',
                        'justify-content': 'right',
                        'font-size': 14,
                    }, lang='he', dir='rtl'
                ), width={'size': 2, 'offset': 0}
            ),
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='process-dropdown',
                        searchable=True,
                        optionHeight=26,
                    )],
                    style={
                        'textAlign'      : 'right',
                        'align-items'    : 'right',
                        'justify-content': 'right',
                        'font-size'      : 14,
                    }, lang='he', dir='rtl'
                ), width={'size': 2, 'offset': 0}
            ),
            dbc.Col(
                html.Div([
                    dcc.Dropdown(
                        id='rules-dropdown',
                        options=rules_dict,
                        searchable=True,
                        optionHeight=26,
                    )],
                    style={
                        'textAlign'      : 'right',
                        'align-items'    : 'right',
                        'justify-content': 'right',
                        'font-size'      : 14,
                    }, lang='he', dir='rtl'
                ), width={'size': 2, 'offset': 0}
            ),
            dbc.Col(id='selected-row-text', width=2),
            dbc.Col(
                dbc.Button('clear marimekko selections', id='clear')
            )
        ]),
        # html.Hr(),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='marimekko-figure')
            ),
        ]),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col(html.Div(id='requirements-table', lang='he', dir='ltr'), width=10),
            dbc.Col(width=1)
        ]),
        html.Hr(),
        dcc.Store(id='domain-id'),
        dcc.Store(id='df-requirements-data'),
        dcc.Store(id='rule-id'),
        dcc.Store(id='clear-out'),
    ], fluid=True)
    return layout

def page_1():
    layout = dbc.Container([
        html.Hr(),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col(
                dcc.Graph(id='mta-figure', figure=f_mta), width=10
            ),
            dbc.Col(width=1),
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col(
                dcc.Graph(id='risk-stacked', figure=f_stacked), width=4
            ),
            dbc.Col(
                dcc.Graph(id='risk-chart', figure=f_risk), width=6
            ),
            dbc.Col(width=1)
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col(
                dbc.Col(html.Div(id='risk-table', lang='he', dir='ltr'), width=10),
            ),
            dbc.Col(width=1)
        ]),
        html.Hr(),
    ], fluid=True )
    return layout

def page_2():
    layout = dbc.Container([
        dbc.Row(
            gantt_status_row()
        ),
        html.Hr(),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='gantt-contract', figure=f_major), width=12
            ),
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(
                html.Div([
                    dcc.RadioItems(
                        id='rule-radio-items',
                        options=[
                            {'label': 'ממשקים',   'value': 'INT'},
                            {'label': 'גאנט שלם', 'value': 'ALL'}
                        ], labelStyle={'display': 'inline-block'}, inputStyle={'margin-right': '20px', 'margin-left': '5px'}
                    )],
                    style={
                        'textAlign'      : 'right',
                        'align-items'    : 'right',
                        'justify-content': 'right',
                        'font-size': 24,
                        'padding'  : '2px 2px',
                    }, lang='he', dir='rtl'
                ),  width={'size': 3, 'offset': 1}
            ),
        ]),
        dbc.Row([
            dbc.Col(width=0),
            dbc.Col(
                dcc.Graph(id='gantt-chart'), width = 12
            ),
            dbc.Col(width=0),
        ]),

        html.Hr()
    ], fluid=True )
    return layout

def update_dropdown(name):
    x = {'select all...': ['select all...']}
    p_list = [{'label': i, 'value': i} for i in x.keys()]
    if name:
        p_list = [{'label': i, 'value': i} for i in process_dict[name]]
    return p_list, name

def home_page_filter_data(selected_domain=str, selected_process=str, selected_rule=str):
    def is_0():
        """ all in None """
        # print(is_0.__name__)
        df_r = df_rd.copy()
        s_str = html.P(str(''))
        return s_str,  df_r, None

    def is_1():
        """ None None rule """
        # print(is_1.__name__)
        df_r = df_rd[df_rd['rule'] == selected_rule]
        s_str = html.P(' -- '.join([selected_rule]))
        return s_str, df_r, selected_rule

    def is_2():
        """ None process None """
        # print(is_2.__name__)
        return is_0()

    def is_3():
        """ None process rule """
        # print(is_3.__name__)
        return is_0()

    def is_4():
        """ domain None None """
        # print(is_4.__name__)
        df_r = df_rd[df_rd['domain_id'] == selected_domain]
        s_str = html.P(selected_domain)
        return s_str, df_r, None

    def is_5():
        """ domain None rule """
        # print(is_5.__name__)
        df_r = df_rd[(df_rd['domain_id'] == selected_domain) & ( df_rd['rule'] == selected_rule )]
        s_str = html.P(' -- '.join([selected_domain, selected_process, selected_rule]))
        return s_str, df_r, selected_rule

    def is_6():
        """ domain process None """
        # print(is_6.__name__)
        df_r = df_rd[(df_rd['domain_id'] == selected_domain) & (df_rd['process_id'] == selected_process)]
        s_str = html.P(' -- '.join([selected_domain, selected_process]))
        return s_str, df_r, None

    def is_7():
        """ domain process rule """
        # print(is_7.__name__)
        df_r = df_rd[(df_rd['domain_id'] == selected_domain) & (df_rd['process_id'] == selected_process) & ( df_rd['rule'] == selected_rule )]
        s_str = html.P(' -- '.join([selected_domain, selected_process, selected_rule]))
        return s_str, df_r, selected_rule

    function_list = [is_0, is_1, is_2, is_3, is_4, is_5, is_6, is_7]
    d = 0 if selected_domain in ['select all...', None] else 1
    p = 0 if selected_process is None else 1
    r = 0 if selected_rule is None else 1
    filter_mask = '{}{}{}'.format(d, p, r)
    print(filter_mask)
    f_index = int(filter_mask, 2)

    is_0()
    selected_str, dfr, rule = function_list[f_index]()
    fig = display_marimekko_chart(df_dev=dfr.copy())
    return selected_str, fig, dfr.to_dict('records'), selected_rule

def display_bullet_chart():
    target = df_gt['target'].values[0] * 100
    actual = df_gt['actual'].values[0] * 100
    fig = go.Figure(
        go.Indicator(
            # mode='number+gauge+delta',
            mode='gauge',
            value=actual,
            delta={
                'reference': target,
                'position': 'top',
            },
            gauge={
                'shape': 'bullet',
                'axis': {'range': [0, 100]},
                'threshold': {
                    'line': {
                        'color': 'crimson',
                        'width': 8
                    },
                    'thickness': 1,
                    'value': target
                },
                'bgcolor': 'white',
                'steps': [
                    {'range': [0, target-12],      'color': '#d9534f'},
                    {'range': [target-12, target], 'color': 'lightsalmon'},
                    {'range': [target, 100],       'color': '#93c54b'}
                ],
                'bar': {'color': 'MidNightBlue'}
            }
        ),
    )

    # fig = annotation_style_position(figure=fig, text='נקודת המטרה', x_position=target/100 - 0.10)
    # t_text = ' {:.1f}% <= {:.1f}%  מול יעד של {:.1f}% :ההתקדמות בפועל'.format(( actual/target ) * 100, actual, target)
    target_text = '{:.1f}% יעד'.format(target)
    actual_text = '{:.1f}% בפועל'.format(actual)
    fig = annotation_position(fig, x=target/100, y=1.00, ax=0, ay=-50, text=target_text, bgcolor='#f47c3c')
    fig = annotation_position(fig, x=actual/100, y=0.41, ax=0, ay= 70, text=actual_text, bgcolor='#f47c3c')
    t_text = 'התקדמות בפועל אל מול יעד ההתקדמות'
    fig.update_layout(
        height=250,
        title={
            'text'    : t_text,
            'y'       : 0.95,
            'x'       : 0.5,
            'xanchor' : 'center',
            'yanchor' : 'top'
        },
        title_font_family='Times New Roman',
        title_font_color='#0047ab',
        title_font_size=36,
        paper_bgcolor='#f8f5f0',   # f8f5f0',
        plot_bgcolor='#c7d6cb',
    )
    fig.update_xaxes(
        tickfont=dict(
            family='Arial',
            color='crimson',
            size=14
        )
    )
    return fig

def display_evm():
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_evm['month'],
            y=df_evm['budget-cumsum'],
            mode='lines+markers',
            name='תכנון תפוקות',
            hovertemplate='<i>plan</i>: %{y}'
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_evm['month'],
            y=df_evm['ytd-value'],
            mode='lines+markers',
            name='תפוקה בפועל',
            hovertemplate='<i>actual</i>: %{y}'
        )
    )
    fig.update_layout(
        title={
            'text'    : 'גרף תפוקות',
            'y'       : 0.95,
            'x'       : 0.5,
            'xanchor' : 'center',
            'yanchor' : 'top'
        },
        title_font_size=36,
        # xaxis_title='תאריך דיווח',
        # yaxis_title='תאריכי אבני הדרך',
        font=dict(
            family='Times Roman',
            size=14,
            color="RebeccaPurple"
        ),
        height = 400,
        paper_bgcolor = '#f8f5f0',
        plot_bgcolor = '#c7d6cb'
    )
    fig.update_xaxes(rangemode='tozero')
    fig.update_yaxes(rangemode='tozero')
    return fig

def display_spi():
    fig = go.Figure(
        go.Indicator(
            # mode = 'gauge+number+delta',
            mode = 'gauge+number',
            # value = df_gt['SPI'].values[0],
            value = float('{:.2f}'.format(df_gt['SPI'].values[0])),
            domain = {'x': [0, 1], 'y': [0, 1]},
            # title = {'text': 'מדד תפוקות', 'font': {'size': 24}},
            delta = {'reference': 1.0, 'increasing': {'color': 'RebeccaPurple'}},
            gauge = {
                'axis': {'range': [None, 1], 'tickwidth': 1, 'tickcolor': 'darkblue'},
                'bar': {'color': 'darkblue'},
                'bgcolor': 'white',
                'borderwidth': 2,
                'bordercolor': 'gray',
                'steps': [
                    {'range': [0.0, 0.6], 'color': '#d9534f'},
                    {'range': [0.6, 0.8], 'color': '#ffc107'},
                    {'range': [0.8, 1.0], 'color': '#93c54b'}
                ],
            }
        )
    )

    fig.update_layout(
        title={
            'text'    : 'מדד תפוקות',
            'y'       : 0.95,
            'x'       : 0.5,
            'xanchor' : 'center',
            'yanchor' : 'top'
        },
        title_font_size=36,
        # xaxis_title='תאריך דיווח',
        # yaxis_title='תאריכי אבני הדרך',
        font=dict(
            family='Times Roman',
            size=14,
            color="RebeccaPurple"
        ),
        height = 400,
        # paper_bgcolor = '#9ab3a0',
        paper_bgcolor = '#f8f5f0',
        plot_bgcolor = '#c7d6cb'
    )
    return fig

def display_marimekko_chart(df_dev=pd.DataFrame):
    df_dev['month'] = pd.to_datetime(df_dev['Finish']).dt.strftime('%Y-%m')
    df_dev.sort_values('month', inplace=True)
    df_g = df_dev.groupby(['month']).agg({
        'RN'       : 'size',
        'completed': 'sum'
    })
    df_g.reset_index(inplace=True)
    df_g['total']  = df_g['RN']
    df_g['closed'] = df_g['completed']
    df_g['open']   = df_g['total'] - df_g['closed']

    gt                  = df_g['total'].sum()
    df_g['month-label'] = pd.to_datetime(df_g['month'], format='%Y-%m').dt.strftime('%b-%Y')
    df_g['% open']      = (df_g['open']   / df_g['total']) * 100
    df_g['% closed']    = (df_g['closed'] / df_g['total']) * 100
    df_g['width']       = (df_g['total']  / gt) * 100

    df_g['x_open']   = df_g['open'  ].apply(lambda x: f'{x:.1f}')
    df_g['x_closed'] = df_g['closed'].apply(lambda x: f'{x:.1f}')

    labels = df_g['month-label']
    widths = np.array(df_g['width'])

    data = {
        'closed'  : df_g['% closed'].apply(np.ceil),
        'open'    : df_g['% open'].apply(np.floor),
    }

    colors = {
        'closed'  : 'lightslategray',
        'open'    : 'crimson'
    }

    # c_map = df_sf[['C_MS', 'month', 'y']]
    # c_map.set_index('month', inplace=True)
    # c_map = c_map.to_dict()
    # df_g['c-ms'] = df_g.month.map(c_map.get('C_MS'))
    # df_g['c-y'] = df_g.month.map(c_map.get('y'))
    df_g['cumsum'] = df_g['width'].cumsum()
    df_g['cumsum'] = df_g['cumsum'].shift(1)
    df_g.loc[0, 'cumsum'] = 0

#     t_yr = today.year
#     t_mn = 1 if today.month + 1 > 12 else today.month + 1
#     x0_rect = '{}-{}'.format(t_yr, f'{t_mn:0>2}')
    x0_rect = (pd.to_datetime(today_mm, format='%Y-%m') + pd.DateOffset(months=1)).strftime('%Y-%m')
    print('marimekko x0_rect: ', x0_rect)

    x1_rect = df_g['month'].max()
    rect = True

#    df_ms = df_g[df_g['c-ms'].notnull()]
    try:
        x0_vrect = df_g[df_g['month'] == x0_rect]['cumsum'].values[0]
        x1_vrect = df_g[df_g['month'] == x1_rect]['cumsum'].values[0] + df_g[df_g['month'] == x1_rect]['width'].values[0]
#        x_vline = df_ms[df_ms['c-ms'] == 'INTEG']['cumsum'].values[0]
    except IndexError:
        rect = False

    bl_start = 0.0
    m = df_bm.loc[0, 'key']

    bl_baseline = True
    try:
        bl_end = df_g[df_g['month'] == m]['cumsum'].values[0]
        baseline_x = [bl_start, bl_end]
    except IndexError:
        bl_baseline = False

    baseline_y = [10, 10]
    annotations = []

    marimekko_data_map = df_g.copy()
    marimekko_data_map.set_index('month', inplace=True)
    marimekko_data_map = marimekko_data_map.to_dict()

    fig = go.Figure()
    for key in data:
        fig.add_trace(go.Bar(
            name=key,
            y=data[key],
            # x=np.cumsum(widths) - widths,
            x=df_g['cumsum'],
            width=widths,
            marker_color=colors[key],
            offset=0,
            customdata=np.transpose([labels, data[key]]),
            text='from total of ' + df_g['total'].astype(str) + ' requirements' +
                 '<br>open/closed: ' + df_g['x_open'] + '/' + df_g['x_closed'],
            textfont_color='lightsalmon',
            texttemplate='%{y}%',
            textposition='inside',
            textangle=0,
            hovertemplate='<br>'.join([
                '%{customdata[0]}',
                key + ': %{y}%',
                '%{text}'
            ]),
            marker=dict(
                line=dict(
                    width=1,
                    color='white'
                )
            )
        ))

    fig.update_xaxes(
        tickvals=np.cumsum(widths)-widths/2,
        ticktext=['%s' % l for l, w in zip(labels, widths)]
    )

    if rect:
        fig.add_vrect(
            x0=x0_vrect,
            x1=x1_vrect,
            fillcolor='darksalmon',
            opacity=0.6,
            layer='above',
            line_width=0
        )
        fig.add_vline(
            x=x0_vrect,
            line_width=3,
            line_dash='dash',
            line_color='green'
        )
        fig = annotation_position(fig, x=x0_vrect, y=1.00, ax=-80, ay=-20, text='עבר', bgcolor='#f47c3c')
        fig = annotation_position(fig, x=x0_vrect, y=1.00, ax= 80, ay=-20, text='עתיד', bgcolor='#f47c3c')
        fig = annotation_position(fig, x=x0_vrect, y=0.80, ax= -180, ay=-20, text=closed_pc_text, bgcolor='#f8f5f0')

    annotations.append(dict(
        xref='paper',
        x=0.05,
        y=baseline_y[0],
        xanchor='left',
        yanchor='bottom',
        text='{}'.format('baseline'),
        font=dict(
            family='Arial',
            size=16,
            color='yellow'
        ),
        showarrow=False
    ))

    fig.update_xaxes(range = [0, 100])
    fig.update_yaxes(range = [0, 100])

    fig.update_layout(
        hoverlabel=dict(
            bgcolor='white',
            font_size=16,
            font_family='Courier New, monospace'
        ),
        bargap=0.9
    )
    fig.update_layout(
        title_text='%open vs. %closed - Marimekko Chart',
        barmode='stack',
        uniformtext=dict(mode='hide', minsize=10),
        xaxis={'categoryorder': 'category ascending'},
        xaxis_tickangle=-45,
        height=600,
        title=config['style_title']
        # annotations=annotations
    )
    return fig

def gantt_page_filter_data(selected_rule=str):
    df_gantt        = df.copy()
    df_completed    = df_ct.copy()
    df_deliverables = pd.DataFrame()
    if selected_rule  == 'INT':
        fig = display_gantt_int()
    else:
        fig = display_gantt_all(df1=df_gantt.copy(), df2=df_completed.copy(), df3=df_deliverables.copy())
    return fig

def display_gantt_int():
    fig = px.timeline(
        df_int,
        x_start='Start_Date',
        x_end='Finish_Date',
        y='process_id',
        color='m_task',
        hover_name='process_id',
        hover_data={
            'process_id' : False,
            'Start_Date' : True,
            'Finish_Date': True
        },
        color_discrete_map={'INT': 'rgb(246, 207, 113)'},
        title='Gantt Report: ' + d_gantt,
        range_x=[pd.to_datetime('20201130'), pd.to_datetime('20221231')]
    )

    for ser in fig['data']:
        ser['hovertemplate'] = \
            '<b>%{hovertext}</b><br>start =  %{customdata[1]|%d/%m/%Y}<br>finish= %{x|%d/%m/%Y}<extra></extra>'

    """ DEADLINE """
    dead_line = pd.to_datetime(d_int_1,   format='%d/%m/%Y')
    second_line = pd.to_datetime(d_int_2, format='%d/%m/%Y')
    fig.add_vline(
        x=today,
        line_width=2,
        line_dash='dot',
        line_color='green',
    )
    fig.add_vline(
        x=dead_line,
        line_width=2,
        line_dash='dot',
        line_color='blue'
    )
    fig.add_vline(
        x=second_line,
        line_width=2,
        line_dash='dot',
        line_color='blue'
    )

    fig = annotation_style_position(figure=fig, text='היום',                 x_position=today)
    fig = annotation_style_position(figure=fig, text='נקודת אינטגרציה',      x_position=dead_line)
    fig = annotation_style_position(figure=fig, text='נקודת אינטגרציה שניה', x_position=second_line)

    fig.add_scatter(                   # progress marker
        y=df_int['process_id'],
        x=df_int['c_date'],
        name='progress',
        mode='markers',
        marker={'size': 8, 'symbol': 'square', 'color': 'black'},
        hovertemplate='<b>%{text}</b>',
        text=['completed: {}'.format(df_int.c_date[i].strftime('%d/%m/%Y')) for i in range(len(df_int))],
        showlegend=True
    )

    progress_line = config['progress_line']
    history_line  = config['history_line']
    y_size = len(df_int['process_id']) - 1
    for i, x in enumerate(df_int['process_id']):
        progress_line['x0'] = df_int.Start_Date[i]
        progress_line['x1'] = df_int.c_date[i]
        progress_line['y0'] = i
        progress_line['y1'] = i
        fig.add_shape(progress_line, name='progress')

    fig.update_layout(
        xaxis_title='תאריך',
        yaxis_title='תהליכים',
        font=dict(
            family='Courier New, monospace',
            size=14,
            color="RebeccaPurple"
        ),
        paper_bgcolor='#f8f5f0',
        plot_bgcolor='#c7d6cb',
        height=950,
        xaxis = dict(autorange='reversed'),
        yaxis = dict(side='right'),
        legend=dict(
            yanchor='top',
            y=0.99,
            xanchor='left',
            x=0.01
        ),
        title=config['style_title']
    )
    return fig

def display_gantt_all(df1=pd.DataFrame, df2=pd.DataFrame, df3=pd.DataFrame):
    df1['start']  = df1['Start_Date']
    df1['finish'] = df1['Finish_Date']
    fig = px.timeline(
        df1,
        x_start='start',
        x_end='finish',
        y='process_id',
        color='m_task',
        color_discrete_sequence=px.colors.qualitative.Pastel,
        hover_name='process_id',
        hover_data=['m_task', 'start', 'finish'],
        title='Gantt Report: ' + d_gantt,
        category_orders={'m_task': ['BP', 'SAP', 'INT', 'Test', 'Prod']},
        range_x=[pd.to_datetime('20201130'), pd.to_datetime('20221231')],
    )
    for ser in fig['data']:
        # c = [xz.strftime('%d/%m/%Y') for xz in ser['customdata'][:, 1]]
        # ser['customdata'][:, 1] = c
        ser['hovertemplate'] = '<b>%{hovertext}</b><br><br>step  = %{customdata[0]}<br>start  = %{customdata[1]}<br>finish = %{x|%d/%m/%Y}<extra></extra>'

    dead_line = pd.to_datetime(d_int_1,   format='%d/%m/%Y')
    second_line = pd.to_datetime(d_int_2, format='%d/%m/%Y')
    fig.add_vline(
        x=today,
        line_width=2,
        line_dash='dot',
        line_color='green',
    )
    fig.add_vline(
        x=dead_line,
        line_width=2,
        line_dash='dot',
        line_color='blue'
    )
    fig.add_vline(
        x=second_line,
        line_width=2,
        line_dash='dot',
        line_color='blue'
    )

    fig = annotation_position(fig, x=today, y=1.00, ax=-100, ay=-40, text='אנחנו כאן', bgcolor='#f47c3c')
    # fig = annotation_style_position(figure=fig, text='היום',                 x_position=today)
    fig = annotation_style_position(figure=fig, text='נקודת אינטגרציה',      x_position=dead_line)
    fig = annotation_style_position(figure=fig, text='נקודת אינטגרציה שניה', x_position=second_line)

    df2.reset_index(inplace=True)
    fig.add_scatter(                   # progress marker
        y=df2['process_id'],
        x=df2['c_date'],
        name='SAP progress',
        mode='markers+text',
        marker={'size': 6, 'symbol': 'circle', 'color': 'black'},
        text=['{:.0%}'.format(df2.sap_pc[i]) for i in range(len(df2))],
        hovertemplate='<b>%{text}</b>',
        textposition='top center',
        showlegend=True
    )
    """
    fig.add_scatter(                   # Dev Start Date
        y=df2['process_id'],
        x=df2['dev_sd'],
        name='תחילת סאפ',
        mode='markers',
        marker={'size': 8, 'symbol': 'diamond', 'color': 'rgb(246, 207, 113)', 'line': {'color': 'MediumPurple', 'width': 1}},
        text=[df2.dev_fd[i].strftime('%d/%m/%Y') for i in range(len(df2))],
        hovertemplate='%{text}',
        textposition='bottom center',
        textfont=dict(
            # color='rgb(246, 207, 113)',
            color='rgb(138, 109, 39)',
        ),
        showlegend=True
    )
    fig.add_scatter(                   # Dev Finish Date
        y=df2['process_id'],
        x=df2['dev_fd'],
        name='סיום סאפ',
        mode='markers',
        marker={'size': 8, 'symbol': 'diamond', 'color': 'rgb(246, 207, 113)', 'line': {'color': 'MediumPurple', 'width': 1}},
        text=[df2.dev_fd[i].strftime('%d/%m/%Y') for i in range(len(df2))],
        hovertemplate='%{text}',
        textposition='bottom center',
        textfont=dict(
            # color='rgb(246, 207, 113)',
            color='rgb(138, 109, 39)',
        ),
        showlegend=True
    )
    """

#     progress_line = config['progress_line']
#     history_line  = config['history_line']
#     p_line = []
#     h_line = []
#     y_size = len(df2['process_id']) - 1
#     for i, x in enumerate(df2['process_id']):
#         progress_line['x0'] = df2.Start_Date[i]
#         progress_line['x1'] = df2.c_date[i]
#         progress_line['y0'] = y_size - i      #   df2.process_id[i]
#         progress_line['y1'] = y_size - i      #   df2.process_id[i]
#         fig.add_shape(progress_line, name='progress')
#
#         # history_line['x0'] = progress_line['x0']   # df2.Start_Date[i]
#         # history_line['x1'] = df2['bl-finish'][i]
#         # history_line['y0'] = y_size - i - 0.35
#         # history_line['y1'] = y_size - i - 0.35
#         # fig.add_shape(history_line, yref='y')
#         df2['y_size'] = y_size - i

    for process_id in df_pd['process_id'].unique():
        dff = df_pd[df_pd['process_id'] == process_id]
        fig.add_trace(
            go.Scatter(
                x=dff['process_date'],
                y=dff['process_id'],
                mode='lines+markers',
                showlegend=False,
                line=dict(
                    color='royalblue',
                    width=2
                ),
                text=[df2.dev_fd[i].strftime('%d/%m/%Y') for i in range(len(df2))],
                hoverinfo='none'
            ),
        )
    fig.update_xaxes(title_font_family='Arial')

    """ Major and M4N Milestones """
    """
    fig.add_trace(go.Scatter(
        name='milestones',
        x=df3[df3['row_type'] == 'm4n-ms']['Finish_Date'],
        y=[-0.001] * len(df2['process_id'].unique()),
        mode='markers',
        marker={'size': 20, 'symbol': 'triangle-up-open', 'color': 'crimson'},
        text=df3['Name'],
        textposition='top center',
        hovertemplate='%{text}',
        textfont=dict(
            family='sans serif',
            size=18,
            color='Lightsalmon'
        )
    ))

    fig.add_trace(go.Scatter(
        name='m4n',
        x=df2['m4n-ms'],
        y=df2['process_id'],
        mode='markers',
        marker={'size': 20, 'symbol': 'triangle-up', 'color': 'darksalmon'},
        text=df2['m4n-nm'],
        textposition='top center',
        hovertemplate='%{text}',
        textfont=dict(
            family='sans serif',
            size=18,
            color='Lightsalmon'
        )
    ))
    """
    fig.update_layout(
        xaxis_title='תאריך',
        yaxis_title='תהליכים',
        font=dict(
            family='Courier New, bold',
            size=14,
            color="RebeccaPurple"
        ),
        paper_bgcolor='#f8f5f0',
        plot_bgcolor='#c7d6cb',
        height=1200,
        xaxis=dict(autorange='reversed'),
        yaxis=dict(side='right'),
        legend=dict(
            yanchor='top',
            y=1.00,
            xanchor='left',
            x=-0.07
        ),
        title=config['style_title'],
        legend_title_text='מקרא'
    )
    fig.update_xaxes(
        dtick='M1',
        ticklabelmode='period'
    )
    fig.data[1]['marker']['opacity'] = 1.0
    fig.data[2]['marker']['opacity'] = 1.0

    fig.data[1]['width'] = 0.9
    fig.data[2]['width'] = 0.5
    return fig

def display_gantt_contract():
    dff = df_mj[['line', 'task', 'start', 'finish', 'months']]
    dff = dff.copy()
    dff['months'] = dff['months'].apply(lambda x: f'{x:.1f}')
    fig = px.timeline(
        dff,
        x_start='start',
        x_end='finish',
        y='line',
        color='task',
        color_discrete_sequence=px.colors.qualitative.Pastel,
        text='<b>' + dff['task'] + ': ' + dff['finish'].dt.strftime('%d/%m/%Y') + '</b>' + '<br>' + dff['months'] + ' :משך',
        title='Gantt Report: ' + d_gantt,
        category_orders={
            'task': ['SRR', 'PDR', 'CDR', 'INTEG']
        },
        hover_data={
            'line' : False,
            'task' : False,
        },
        height=500,
        range_x=[pd.to_datetime('20221231', format='%Y%m%d'), pd.to_datetime('20201130', format='%Y%m%d')]
    )

    v_df = df_mj[['vline', 'vline_text', 'ax']]
    v_df = v_df[v_df['vline'].notnull()]
    for index, row in v_df.iterrows():
        fig.add_annotation(
            # text='<b>' + row.vline_text + '</b>',
            text=row.vline_text,
            x=row.vline,
            yref='paper',
            y=0,
            showarrow=True,
            align='center',
            arrowhead=3,
            arrowsize=1,
            arrowcolor='#636363',
            ax=row.ax,
            ay=50,
            bordercolor='#c7c7c7',
            borderwidth=2,
            borderpad=4,
            bgcolor='#ff7f0e',
            opacity=0.8,
            font= dict(
                family='Times Roman',
                size=18,
                color='midnightblue'
            )
        )

    fig.add_annotation(
        xref='paper',
        yref='paper',
        text='צמצום של 4.5 חדשים בשלב האינטגריצה',
        x=0.312,
        y=0.93,
        # x=pd.to_datetime('15/05/2023', format='%d/%m/%Y'),
        showarrow=True,
        align='center',
        arrowhead=2,
        arrowsize=1,
        arrowcolor='#636363',
        ax=0,
        ay=0,
        bordercolor='#c7c7c7',
        borderwidth=2,
        borderpad=4,
        bgcolor='crimson',
        opacity=0.8,
        font=dict(
            family='Times Roman',
            size=18,
            color='white'
        )
    )

    fig.add_annotation(
        xref='paper',
        yref='paper',
        text='מבצוע 15/07/2024',
        x=0.01,
        y=0.95,
        # x=pd.to_datetime('15/05/2023', format='%d/%m/%Y'),
        showarrow=True,
        align='center',
        arrowhead=2,
        arrowsize=1,
        arrowcolor='#636363',
        ax=0,
        ay=0,
        bordercolor='#c7c7c7',
        borderwidth=2,
        borderpad=4,
        bgcolor='midnightblue',
        opacity=0.8,
        font=dict(
            family='Times Roman',
            size=18,
            color='white'
        )
    )

    fig.update_layout(
        showlegend=False,
        yaxis_title='',
        font = dict(
            family='Times Roman',
            size=18,
            color="RebeccaPurple",
        ),
        paper_bgcolor = '#f8f5f0',
        plot_bgcolor = '#c7d6cb',
        bargap=0.01,
        xaxis=dict(autorange='reversed'),
        yaxis=dict(side='right'),
        legend=dict(
            yanchor='top',
            y=0.99,
            xanchor='left',
            x=-0.15
        ),
        title=config['style_title']
    )
    return fig

def gantt_status_row():
    dff = df_gt.copy()
    bps = dff['BP'].unique()
    Int = dff['int'].unique()
    sap = dff['sap'].unique()
    req = dff['requirements'].unique()
    sl  = dff['shortlist'].unique()
    ot  = sl - (sap+Int)

    status_row = [
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6(
                        'בלו-פרינטים',
                        className='card-title',
                        style={
                            'textAlign': 'center',
                            'text-decoration': 'underline'
                        }
                    ),
                    html.P(
                        bps,
                        className='card_text',
                        style={
                            'textAlign': 'center',
                            'color'    : '#EADB5A',
                            'font-size': 48,
                        })
                ]), color="#325d88", inverse=True
            ), width={'size': 2, 'offset': 0}
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6(
                        'סך דרישות',
                        className='card-title',
                        style={
                            'textAlign': 'center',
                            'text-decoration': 'underline'
                        }
                    ),
                    html.P(
                        req,
                        className='card_text',
                        style={
                            'textAlign': 'center',
                            'color'    : '#EADB5A',
                            'font-size': 48
                        }
                    )
                ]), color="#325d88", inverse=True
            ), width={'size': 2, 'offset': 0}
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6(
                        'דרישות ממשקים',
                        className='card-title',
                        style={
                            'textAlign': 'center',
                            'text-decoration': 'underline'
                        }
                    ),
                    html.P(
                        Int,
                        className='card_text',
                        style={
                            'textAlign': 'center',
                            'color'    : '#EADB5A',
                            'font-size': 48
                        }
                    )
                ]), color="#325d88", inverse=True
            ), width={'size': 2, 'offset': 0}
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6(
                        'דרישות סאפ',
                        className='card-title',
                        style={
                            'textAlign': 'center',
                            'text-decoration': 'underline'
                        }
                    ),
                    html.P(
                        sap,
                        className='card_text',
                        style={
                            'textAlign': 'center',
                            'color'    : '#EADB5A',
                            'font-size': 48
                        }
                    )
                ]), color="#325d88", inverse=True
            ), width={'size': 2, 'offset': 0}
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6(
                        'דרישות אחרות',
                        className='card-title',
                        style={
                            'textAlign': 'center',
                            'text-decoration': 'underline'
                        }
                    ),
                    html.P(
                        ot,
                        className='card_text',
                        style={
                            'textAlign': 'center',
                            'color'    : '#EADB5A',
                            'font-size': 48
                        }
                    )
                ]), color="#325d88", inverse=True
            ), width={'size': 2, 'offset': 0}
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6(
                        'סך דרישות לפיתוח',
                        className='card-title',
                        style={
                            'textAlign': 'center',
                            'text-decoration': 'underline'
                        }
                    ),
                    html.P(
                        sl,
                        className='card_text',
                        style={
                            'textAlign': 'center',
                            'color'    : '#EADB5A',
                            'font-size': 48
                        }
                    )
                ]), color="#325d88", inverse=True
            ), width={'size': 2, 'offset': 0}
        ),
    ]
    return status_row

def mta_fig():
    df_mta = pd.read_excel(f1, sheet_name='mta')

    df_mta['yyyy-mm'] = df_mta['x'].map(lambda x: pd.to_datetime(x).strftime('%Y-%m'))
    df_mta.set_index('yyyy-mm', inplace=True)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_mta['line'],
            y=df_mta['y'],
            mode='lines',
            showlegend=False
        )
    )

    milestones = ['ARO', 'SRR', 'PDR', 'CDR', 'M4N', 'Army']
    colors = [px.colors.qualitative.G10[i] for i in [3, 6, 4, 5, 8]]
    colors.append('midnightblue')
    size_list = [10, 10, 10, 10, 12, 10]
    i = 0
    for ms in milestones:
        fig.add_trace(
            go.Scatter(
                x=df_mta['x'],
                y=df_mta[ms],
                name=ms,
                mode='lines+markers',
                marker={
                    'size': size_list[i],
                    'symbol': 'square'
                },
                hovertemplate=ms + ': %{y|%d/%m/%Y}',
                marker_color=colors[i],
            ),
        )
        i += 1

    fig.update_layout(
        height=600,
        paper_bgcolor='#f8f5f0',
        plot_bgcolor='#c7d6cb'
    )

    fig.update_xaxes(range = [df_mta['x'].min(), df_mta['x'].max()])
    fig.update_yaxes(range = [df_mta['x'].min(), df_mta['x'].max()])

    fig.update_layout(
        title='MTA',
        xaxis_title='תאריך דיווח',
        yaxis_title='תאריכי אבני הדרך',
        font=dict(
            family='Courier New, monospace',
            size=18,
            color="RebeccaPurple"
        )
    )
    return fig

def display_requirements_table(dff=pd.DataFrame(), selected_month=str):
    if len(dff) == 0:
        dff = df_rd.copy()
    if selected_month is not None:
        selected_month = pd.to_datetime(selected_month, format='%b-%Y')
        mn = selected_month.month
        yr = selected_month.year
        dff['Finish_Date'] = pd.to_datetime(dff['Finish'])

        mask_1 = dff['Finish_Date'].map(lambda x: x.month) == mn
        mask_2 = dff['Finish_Date'].map(lambda x: x.year)  == yr

        dff = dff[mask_1 & mask_2]

    df_tmp = dff.copy()
    df_tmp = df_tmp[[pc, 'Finish', 'Name', 'rule', 'xRN', 'process_id', 'domain_id']]
    df_tmp[pc] = df_tmp[pc].apply(lambda x: f'{x:.2f}')
    df_tmp['Finish'] = pd.to_datetime(df_tmp['Finish']).dt.strftime('%d/%m/%Y')
    dff_table = dt.DataTable(
        data=df_tmp.to_dict('records'),
        columns=[{'name': i, 'id': i, 'deletable': False} for i in df_tmp.columns if i != 'id'],
        style_cell=style_cell,
        style_header=style_header,
        filter_action='native',
        sort_action='native',
        sort_mode='single',
        page_action='native',
        page_current=0,
        page_size=20,
        export_format='xlsx',
        export_headers='display'
    )
    return dff_table

def display_risktable(month=str, text=str):
    if month is not None:
        color = df_rss[( df_rss['indexDT'] == month ) & ( df_rss['size'] == int(text) )]['color'].values[0]
        dff   = df_rsk[( df_rsk['indexDT'] == month ) & ( df_rsk['color'] == color )]
    else:
        dff = df_rsk.copy()

    dff = dff[['comments', 'reduction actions', 'risk source', 'risk', 'severity', 'probability', 'Name']]
    dff.columns = ('comments', 'actions', 'source', 'level', 'severity', 'probability', 'description')
    dff_table = dt.DataTable(
        data=dff.to_dict('records'),
        columns=[{'name': i, 'id': i, 'deletable': False} for i in dff.columns if i != 'id'],
        style_cell=style_cell,
        style_header=style_header,
        filter_action='native',
        sort_action='native',
        sort_mode='single',
        page_action='native',
        page_current=0,
        page_size=20,
        export_format='xlsx',
        export_headers='display'
    )
    return dff_table


def display_risk_stacked():
    fig = px.bar(
        df_rss,
        x='indexDT',
        y='size',
        color='color',
        color_discrete_sequence=['#d9534f', '#ffc107', '#93c54b'],
        text='size'
    )

    fig.update_traces(
        textposition='inside',
        textfont_size=32,
    )

    fig.update_layout(
        title='Risk Management',
        xaxis_title='Dates',
        yaxis_title='# of Risks',
        font=dict(
            family='Courier New, monospace',
            size=18,
            color="RebeccaPurple"
        ),
        paper_bgcolor = '#f8f5f0',
        plot_bgcolor  = '#c7d6cb',
        height = 600,
        uniformtext=dict(
            minsize=10,
            mode='hide'
        ),
    )
    return fig

def display_risk_heatmap():
    z_text = np.empty((5, 5), dtype=int)
    z_text[:] = 0
    z_text = [
        ['', '', '', '', ''],
        ['', '', '', '', ''],
        ['', '', '', '', ''],
        ['', '', '', '', ''],
        ['', '', '', '', '']
    ]

    z = [
        [5, 10, 15, 20, 25],
        [4, 8, 12, 16, 20],
        [3, 6, 9, 12, 15],
        [2, 4, 6, 8, 10],
        [1, 2, 3, 4, 5]
    ]
    df_risk = df_rsk.copy()
    max_index_date = df_risk['indexDT'].max()
    print('max risk date: ', max_index_date)
    df_risk = df_risk[( df_risk['risk'] > 1 ) & ( df_risk['indexDT'] == max_index_date )]
    df_risk = df_risk[['risk', 'Name', 'severity', 'probability']]
    df_risk['severity'] = df_risk['severity'] - 1
    df_risk['probability'] = df_risk['probability'] - 1

    df_risk = df_risk.groupby(['severity', 'probability']).size()
    df_risk = df_risk.to_frame()
    df_risk = df_risk.rename(columns={0: 'count_x'})
    df_risk.reset_index(inplace=True)

    for index, row in df_risk.iterrows():
        x = row.severity
        y = row.probability
        z_text[x][y] = str(row.count_x)

    z_text = np.flip(z_text, 0)

    x = [1, 2, 3, 4, 5]
    y = [5, 4, 3, 2, 1]
    font_colors = ['black']

    fig = ff.create_annotated_heatmap(
        z,
        x=x,
        y=y,
        annotation_text=z_text,
        colorscale=[
            [0.00, 'rgb(56, 158, 39)'],
            [0.32, 'rgb(247, 247, 5)'],
            [0.60, 'rgb(244, 109, 67)'],
            [1.00, 'rgb(165, 0, 38)'],
        ],
        colorbar=dict(
            title='Risk Level'
        ),
        font_colors=font_colors,
        showscale=True,
        hoverinfo='none'
    )

    for i in range(len(fig.layout.annotations)):
        fig.layout.annotations[i].font.size = 32

    fig.update_layout(
        title='Risk Management',
        xaxis_title='Severity',
        yaxis_title='Probability',
        font=dict(
            family='Courier New, monospace',
            size=18,
            color="RebeccaPurple"
        ),
        paper_bgcolor='#f8f5f0',
        plot_bgcolor='#c7d6cb',
        height=600,
    )
    return fig

def app_navbar(app):
    navbar = dbc.Navbar(
        dbc.Container(
            [
                dcc.Location(id="url", refresh=True),
                html.A(
                    dbc.Row([
                        html.Img(src=app.get_asset_url('logo.png'), height='30px'),
                        dbc.NavbarBrand(
                            'מערכת לניהול מרה"ס שלב ג',
                            className="ml-2",
                            style={
                                'font-size': 14,
                                'text-decoration': 'none'
                            },
                            href='/page-2'
                        ),
                    ]), href='/page-2'
                ),
                dbc.NavbarToggler(id="navbar-toggler"),
                dbc.Collapse(
                    dbc.Nav([
                        dbc.NavLink(
                            'בית',
                            href="/",
                            active="exact",
                            style={
                                'font-size': 14,
                                'text-decoration': 'none'
                            }
                        ),
                        dbc.NavLink(
                            "MTA...",
                            href="/page-1",
                            active="exact",
                            style={
                                'font-size': 14,
                                'text-decoration': 'none'
                            }
                        ),
                        dbc.NavLink(
                            'דשבורד',
                            href="/page-2",
                            active="exact",
                            style={
                                'font-size': 14,
                                'text-decoration': 'none'
                            }
                        ),
                    ], className="ml-auto", navbar=True),
                    id="navbar-collapse",
                    navbar=True,
                ),
            ], fluid=True
        ),
        color="dark",
        dark=True,
        className="mb-5",
    )
    return navbar

""" prepare figures """
f_mta     = mta_fig()
f_major   = display_gantt_contract()
f_risk    = display_risk_heatmap()
f_stacked = display_risk_stacked()
f_evm     = display_evm()
f_spi     = display_spi()
f_bullet  = display_bullet_chart()

# f_dl      = display_deliverable()

#     if shafir:
#         fig.add_trace(go.Scatter(
#             name='shafir',
#             x=df_sf['x'],
#             y=df_sf['y'],
#             mode='markers+text',
#             marker={'size': 18, 'symbol': 'diamond', 'color': 'darksalmon'},
#             text=df_sf['C_MS'],
#             textposition='top center',
#             hovertemplate='%{text}',
#             textfont=dict(
#                 family='sans serif',
#                 size=14,
#                 color='Lightsalmon'
#             )
#         ))

#     if bl_baseline:
#         moshe = True
#     fig.add_trace(go.Scatter(
#         x=baseline_x,
#         y=baseline_y,
#         mode='lines',
#         name='baseline',
#         line=dict(
#             color='yellow',
#             width=4
#         ),
#         opacity=0.7
#     ))
"""
def display_deliverable():
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        name='delivery',
        x=df_dl['Finish_Date'],
        y=df_dl['Name'],
        mode='markers',
        marker={'size': 18, 'symbol': 'triangle-up', 'color': 'darksalmon'},
        text=df_dl['Finish_Date'],
        textposition='top center',
        hovertemplate='%{text}',
        textfont=dict(
            family='sans serif',
            size=18,
            color='Lightsalmon'
        )
    ))

    fig.add_trace(go.Scatter(
        name='delivery-baseline',
        x=df_dl['bl-date'],
        y=df_dl['Name'],
        mode='markers',
        marker={'size': 18, 'symbol': 'triangle-up-open-dot', 'color': 'lightsalmon'},
        text=df_dl['bl-date'],
        textposition='top center',
        hovertemplate='%{text}',
        textfont=dict(
            family='sans serif',
            size=18,
            color='Lightsalmon'
        )
    ))
    for index, row in df_dl.iterrows():
        fig.add_shape(
            # name='line',
            type='line',
            x0=df_dl.loc[index, 'bl-date'],
            y0=df_dl.loc[index, 'Name'],
            x1=df_dl.loc[index, 'Finish_Date'],
            y1=df_dl.loc[index, 'Name'],
            line=dict(
                width=0.5,
                color='RoyalBlue',
                dash='dashdot'
            )
        )
    fig.update_layout(
        title_text='deliverables',
        paper_bgcolor='#f8f5f0',
        plot_bgcolor='#f5faf6',
        height=1000,
        font = dict(
            family='Courier New, monospace',
            size=14,
            color="RebeccaPurple"
        ),
    )
    return fig
"""
