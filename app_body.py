import os
import codecs
import pandas as pd
import numpy as np
import warnings as warning
import yaml
import plotly.express as px
import plotly.graph_objects as go
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table as dt
import r_heatmap
import r_stacked

from plotly.subplots import make_subplots

from dash.exceptions import PreventUpdate
from datetime import date

pd.options.mode.chained_assignment = 'raise'
warning.filterwarnings('ignore')
print(125 * '=')

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

path = config['config'].get('path')
fn = os.path.join(path, config['config'].get('gantt'))
f1 = os.path.join(path, config['config'].get('pmo'))
f2 = os.path.join(path, config['config'].get('name'))

today    = pd.to_datetime('today')
deadline = pd.to_datetime('2020/10/31', format='%Y/%m/%d')
x_start  = pd.to_datetime('2020/08/01', format='%Y/%m/%d')

style_cell   = config['style_cell']
style_header = config['style_header']

df    = pd.read_excel(fn, sheet_name='gantt-data')
df_ct = pd.read_excel(fn, sheet_name='completed')
df_dv = pd.read_excel(fn, sheet_name='dev')
df_rd = pd.read_excel(fn, sheet_name='raw-dev')
df_sf = pd.read_excel(fn, sheet_name='shafir')
df_dl = pd.read_excel(fn, sheet_name='deliverables')
df_bm = pd.read_excel(fn, sheet_name='bl-month')
df_pr = pd.read_excel(f2, sheet_name='process_ref')

df_dl.sort_values(['RN'], ascending=False, inplace=True)

df_pr = df_pr[['domain_id', 'process_id']]
domains = df_pr['domain_id'].unique()

process_dict = {'select all...': ['select all...']}
for x_key in domains:
    process_ids = df_pr[df_pr['domain_id'] == x_key]['process_id'].to_list()
    process_dict.update({x_key: process_ids})

rules = df_rd['rule'].unique()
rules_dict = [{'label': i, 'value': i} for i in rules]

def home_page():
    layout = dbc.Container([
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
                        id='rules-dropdown',
                        options=rules_dict,
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
            dbc.Col(id='selected-row-text', width=2),
            dbc.Col(
                dbc.Button('clear selections', id='clear')
            )
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(
                dcc.Graph(id='marimekko-figure')
            ),
        ]),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col(html.Div(id='requirements-table', lang='en', dir='ltr'), width=10),
            dbc.Col(width=1)
        ]),
        html.Hr(),
        dcc.Store(id='domain-id'),
        dcc.Store(id='df-requirements-data'),
        dcc.Store(id='rule-id'),
        dcc.Store(id='clear-out')
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
                dcc.Graph(id='gantt-chart', figure=f_gantt), width=10
            ),
            dbc.Col(width=1),
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col(
                dcc.Graph(id='deliverables-chart', figure=f_dl), width=10
            ),
            dbc.Col(width=1),
        ]),
        html.Hr(),
        dbc.Row([
            dbc.Col(width=1),
            dbc.Col(
                dcc.Graph(id='risk-chart', figure=f_stacked), width=4
            ),
            dbc.Col(
                dcc.Graph(id='risk-stacked', figure=f_risk), width=6
            ),
            dbc.Col(width=1),
        ]),
        html.Hr()
    ], fluid=True )
    return layout

def page_2():
    layout = dbc.Container([
        html.Hr(),
        dbc.Row([
            dbc.Col(

            ),
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
        dv_c = df_dv.copy()
        df_r = df_rd.copy()
        s_str = html.P(str(''))
        return s_str, dv_c, df_r, None

    def is_1():
        """ None None rule """
        # print(is_1.__name__)
        dv_c  = df_dv[df_dv['rule'] == selected_rule]
        df_r = df_rd[df_rd['rule'] == selected_rule]
        s_str = html.P(' -- '.join([selected_rule]))
        return s_str, dv_c, df_r, selected_rule

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
        dv_c = df_dv[df_dv['domain_id'] == selected_domain]
        df_r = df_rd[df_rd['domain_id'] == selected_domain]
        s_str = html.P(selected_domain)
        return s_str, dv_c, df_r, None

    def is_5():
        """ domain None rule """
        # print(is_5.__name__)
        dv_c = df_dv[(df_dv['domain_id'] == selected_domain) & ( df_dv['rule'] == selected_rule )]
        df_r = df_rd[(df_rd['domain_id'] == selected_domain) & ( df_rd['rule'] == selected_rule )]
        s_str = html.P(' -- '.join([selected_domain, selected_process, selected_rule]))
        return s_str, dv_c, df_r, selected_rule

    def is_6():
        """ domain process None """
        # print(is_6.__name__)
        dv_c = df_dv[(df_dv['domain_id'] == selected_domain) & (df_dv['process_id'] == selected_process)]
        df_r = df_rd[(df_rd['domain_id'] == selected_domain) & (df_rd['process_id'] == selected_process)]
        s_str = html.P(' -- '.join([selected_domain, selected_process]))
        return s_str, dv_c, df_r, None

    def is_7():
        """ domain process rule """
        # print(is_7.__name__)
        dv_c = df_dv[(df_dv['domain_id'] == selected_domain)  & (df_dv['process_id'] == selected_process) & ( df_dv['rule'] == selected_rule )]
        df_r = df_rd[(df_rd['domain_id'] == selected_domain) & (df_rd['process_id'] == selected_process) & ( df_rd['rule'] == selected_rule )]
        s_str = html.P(' -- '.join([selected_domain, selected_process, selected_rule]))
        return s_str, dv_c, df_r, selected_rule

    function_list = [is_0, is_1, is_2, is_3, is_4, is_5, is_6, is_7]
    d = 0 if selected_domain in ['select all...', None] else 1
    p = 0 if selected_process is None else 1
    r = 0 if selected_rule is None else 1
    filter_mask = '{}{}{}'.format(d, p, r)
    print(filter_mask)
    f_index = int(filter_mask, 2)

    is_0()
    selected_str, dvc, dfr, rule = function_list[f_index]()
    fig = marimekko_chart(df_dev=dvc.copy())
    return selected_str, fig, dfr.to_dict('records'), selected_rule

def marimekko_chart(df_dev = pd.DataFrame):
    df_dev['month'] = pd.to_datetime(df_dev['Finish_Date']).dt.strftime('%Y-%m')
    df_dev.sort_values('month', inplace=True)
    df_g = df_dev.groupby(['month']).agg({
        'total': 'sum',
        'open': 'sum',
        'closed': 'sum'
    })
    df_g.reset_index(inplace=True)

    gt = df_g['total'].sum()
    df_g['month-label'] = pd.to_datetime(df_g['month'], format='%Y-%m').dt.strftime('%b-%Y')
    df_g['% open']      = (df_g['open'] / df_g['total']) * 100
    df_g['% closed']    = (df_g['closed'] / df_g['total']) * 100
    df_g['width']       = (df_g['total'] / gt) * 100

    df_g['x_open']   = df_g['open'].apply(lambda x: f'{x:.1f}')
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

    c_map = df_sf[['C_MS', 'month', 'y']]
    c_map.set_index('month', inplace=True)
    c_map = c_map.to_dict()
    df_g['c-ms'] = df_g.month.map(c_map.get('C_MS'))
    df_g['c-y'] = df_g.month.map(c_map.get('y'))
    df_g['cumsum'] = df_g['width'].cumsum()
    df_g['cumsum'] = df_g['cumsum'].shift(1)
    df_g.loc[0, 'cumsum'] = 0

    t_yr = today.year
    t_mn = 1 if today.month + 1 > 12 else today.month + 1
    x0_rect = '{}-{}'.format(t_yr, f'{t_mn:0>2}')
    x1_rect = df_g['month'].max()
    rect = True

    df_ms = df_g[df_g['c-ms'].notnull()]
    try:
        x0_vrect = df_g[df_g['month'] == x0_rect]['cumsum'].values[0]
        x1_vrect = df_g[df_g['month'] == x1_rect]['cumsum'].values[0] + df_g[df_g['month'] == x1_rect]['width'].values[0]
        x_vline = df_ms[df_ms['c-ms'] == 'INTEG']['cumsum'].values[0]
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

    shafir = True
    try:
        df_sf['x'] = df_sf['month'].map(marimekko_data_map.get('cumsum'))
    except IndexError:
        shafir = False

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
            ])
        ))
    if shafir:
        fig.add_trace(go.Scatter(
            name='shafir',
            x=df_sf['x'],
            y=df_sf['y'],
            mode='markers+text',
            marker={'size': 18, 'symbol': 'diamond', 'color': 'darksalmon'},
            text=df_sf['C_MS'],
            textposition='top center',
            hovertemplate='%{text}',
            textfont=dict(
                family='sans serif',
                size=14,
                color='Lightsalmon'
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
            fillcolor='#997c2e',
            opacity=0.5,
            layer='above',
            line_width=0
        )
        fig.add_vline(x=x_vline, line_width=3, line_dash='dash', line_color='green')

    if bl_baseline:
        moshe = True
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
        )
    )
    fig.update_layout(
        title_text='%open vs. %closed - Marimekko Chart',
        barmode='stack',
        uniformtext=dict(mode='hide', minsize=10),
        xaxis={'categoryorder': 'category ascending'},
        xaxis_tickangle=-45,
        height=600,
        # annotations=annotations
    )
    return fig

def gantt_fig():
    df1 = df.copy()
    df2 = df_ct.copy()

    d = df2.domain_id.unique()
    fig = px.timeline(
        df1,
        x_start='Start_Date',
        x_end='Finish_Date',
        y='process_id',
        color='m_task',
        color_discrete_sequence=px.colors.qualitative.Pastel,
        hover_name='process_id',
        hover_data={
            'process_id': False,
            'm_task'    : True,
            'Start_Date': False
        },
        title='Gantt Report: ' + today.strftime('%d/%m/%Y'),
        category_orders={
            'm_task': ['BP', 'Dev', 'Test', 'Prod']
        },
        height=800
    )

    for ser in fig['data']:
        ser['hovertemplate'] = \
            '<b>%{hovertext}</b><br><br>step  = %{customdata[1]}<br>finish= %{x|%d/%m/%Y}<extra></extra>'
    #     for ser in fig['data']:
    #         ser['hovertemplate'] = \
    #             '<b>%{hovertext}</b><br><br>step  = %{customdata[1]}<br>start = %{customdata[2]|%d/%m/%Y}<br>' \
    #             'finish= %{x|%d/%m/%Y}<extra></extra>'

    fig.add_vline(
        x=today,
        line_width=3,
        line_dash='dash',
        line_color='green'
    )

    dead_line = pd.to_datetime('15/04/2022', format='%d/%m/%Y')
    fig.add_vrect(
        x0='2022-04-15',
        x1='2022-04-15',
        line_width=3,
        line_dash='dot',
        line_color='blue',
        annotation_text=' integration point',
        annotation_position='top left',
        annotation=dict(
            font_size=20,
            font_family='Times New Roman',
            bgcolor='lightsalmon'
        )
    )

    df2.reset_index(inplace=True)
    fig.add_scatter(
        y=df2['process_id'],
        x=df2['c_date'],
        name='completed',
        mode='markers',
        marker={'size': 10, 'symbol': 'square', 'color': 'black'},
        hovertemplate='<b>%{text}</b>',
        text=['completed: {}'.format(df2.Finish_Date[i].strftime('%Y-%m-%d')) for i in range(len(df2))],
        showlegend=False
    )

    progress_line = config['progress_line']
    history_line  = config['history_line']
    for i, x in enumerate(df2['process_id']):
        progress_line['x0'] = df2.Start_Date[i]
        progress_line['x1'] = df2.c_date[i]
        progress_line['y0'] = df2.process_id[i]
        progress_line['y1'] = df2.process_id[i]
        fig.add_shape(progress_line, name='progress')

        history_line['x0'] = df2.Start_Date[i]
        history_line['x1'] = df2['bl-finish'][i]
        history_line['y0'] = progress_line['y0']
        history_line['y1'] = progress_line['y1']
        # fig.add_shape(history_line, name='baseline', yref='y')

    fig.update_xaxes(title_font_family='Arial')
    # fig.update_yaxes(autorange="reversed")
    # fig.update_layout(xaxis_range=[x_start, pd.to_datetime('2021/01/31', format='%Y/%m/%d')])

    fig.update_layout(
        xaxis_title='תאריך',
        yaxis_title='תהליכים',
        font=dict(
            family='Courier New, monospace',
            size=12,
            color="RebeccaPurple"
        ),
        paper_bgcolor='#9ab3a0',
        plot_bgcolor='#c7d6cb',
        height=800
    )
    return fig

def mta_fig():
    print(f1)
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
    colors = [px.colors.sequential.Blues[i] for i in [3, 6, 4, 5, 8]]
    colors.append('darksalmon')
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
        #width=1000,
        paper_bgcolor='#9ab3a0',
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

def fig_page2_subplots():
    fig = make_subplots(rows=1, cols=2, column_width=[0.7, 0.3])
    fig.add_trace(gantt_fig())
    fig.add_trace(mta_fig())

    return fig

def display_requirements_table(dff=pd.DataFrame(), selected_month=str):
    if len(dff) == 0:
        dff = df_rd.copy()
    if selected_month is not None:
        selected_month = pd.to_datetime(selected_month, format='%b-%Y')
        mn = selected_month.month
        yr = selected_month.year
        dff['Finish_Date'] = pd.to_datetime(dff['Finish_Date'])

        mask_1 = dff['Finish_Date'].map(lambda x: x.month) == mn
        mask_2 = dff['Finish_Date'].map(lambda x: x.year)  == yr

        dff = dff[mask_1 & mask_2]

    df_tmp = dff.copy()
    df_tmp = df_tmp[['pc', 'Finish_Date', 'p_name', 'parent', 'xRN', 'process_id', 'domain_id']]
    df_tmp['Finish_Date'] = pd.to_datetime(df_tmp['Finish_Date']).dt.strftime('%d/%m/%Y')
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
        page_size=8
    )
    return dff_table

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
            size=14,
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
            size=14,
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
        paper_bgcolor='#9ab3a0',
        plot_bgcolor='#f5faf6',
        height=1000
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
                            href='/'
                        ),
                    ]), href='/'
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
                            "MTA",
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

f_mta   = mta_fig()
f_gantt = gantt_fig()
f_dl    = display_deliverable()
f_risk  = r_heatmap.risk_heatmap()
f_stacked = r_stacked.r_stacked_fig()