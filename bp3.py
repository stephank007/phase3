import os
import codecs
import pandas as pd
import numpy as np
import time
import warnings as warning
import workday as wd
import yaml
import re
import mongo_services as ms

def db_query(collection=None, q=None):
    collect = ms.get_collection(db, collection)
    df_h = pd.DataFrame()
    data = []

    x = collect.find(q)
    for r in x:
        data.append(r.get('results'))

    df_h = df_h.append(data, True)
    if len(df_h) > 0:
        print('.', end=' ')
    else:
        print('{:50}:'.format('empty query'))
    return df_h

def insert_mom(sheet_name):
    df_mx = pd.read_excel(fn, sheet_name=sheet_name)
    df_mx['row_type']  = 'MoM'
    df_mx['parent'] = df_mx.get('parent')
    df_mx['due_date']  = pd.to_datetime(df_mx['due_date'], format='%d/%m/%Y')
    df_mx = df_mx.rename(columns={'topic': 'rule'})

    df_mx = df_mx[config['mom_columns']]
    df_mx['RN'] = df_mx['RN']

    return df_mx

def write_consolidated():
    df = df_bp.copy()
    df = df[df['row_type'].isin(['parent', 'rule', 'MoM'])]
    df = df[config['consolidated_columns']]
    newline = r'[\r\n]'

    df_bp['Name'] = df_bp['Name'].apply(lambda x: re.subn(newline, '--', str(x))[0])
    df_bp['task_notes'] = df_bp['task_notes'].apply(lambda x: re.subn(newline, '--', str(x))[0])
    df_bp['task_notes_2'] = df_bp['task_notes_2'].apply(lambda x: re.subn(newline, '--', str(x))[0])

    writer = pd.ExcelWriter(fz, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet2')
    writer.save()
    del df

    return df_bp

def create_cycle_map(topic='BP'):
    df = df_rules_x.copy()
    df = df[['Rule', 'Name']]

    df = df[df['Rule'] == topic]
    df = df.iloc[1:]
    lc_list = df.Name.to_list()
    lc_cat = pd.Categorical(lc_list, categories=lc_list, ordered=True)
    return lc_cat

def check_status(df=pd.DataFrame()):
    try:
        rule = df.rule.values[0]
        lc_category = create_cycle_map(topic=rule)
        p_status = df.status.values[0]
    except Exception as e:
        print('Oops! update rule and MoM tasks', e.__class__, 'occurred', df.RN)
        quit(-1)
    if p_status not in lc_category:
        print('Oops! check_status: No rule found in library', p_status, df.RN)
        quit(-1)
    else:
        return p_status, lc_category

def date_calc(sd, days=0):
    return wd.workday(
        sd,
        days=days,
        holidays=holidays,
        weekends=weekends
    )

def update_lc_block():
    for p in parent_list:
        df_x = df_bp[(df_bp['row_type'].isin(['rule']))   & (df_bp['parent'].isin([p]))]  # LC rule block df
        p_df = df_bp[(df_bp['row_type'].isin(['parent'])) & (df_bp['parent'].isin([p]))]  # parent's row

        p_index  = p_df.index
        try:
            rule     = p_df.rule.values[0]
        except IndexError as ex:
            x = p
            print(p, ex)
            quit(-1)
        p_status = p_df.status.values[0]
        lc_category = create_cycle_map(topic=rule)
        # p_status, lc_category = check_status(df=p_df)
        p_due_date  = pd.to_datetime(p_df.due_date.values[0])  # parent's due-date
        try:
            ''' selecting second row from rule block, since the first row is a milestone for the above sub_tasks '''
            first_row_index  = df_x.index.values[1]
            status_row_index = df_x[df_x.Name.str.contains(p_status)].index.values[0]
        except Exception as ex:
            print('Oops! update rule and MoM tasks, first row index', ex.__class__, 'occurred', p)
            continue

        ''' mark all steps completed from the life cycle block '''
        df_x = df_x.copy()
        steps_completed = np.sum(lc_category < p_status) - 1
        completed = first_row_index + steps_completed
        completed = status_row_index-1
        df_x.loc[first_row_index:completed, percent_complete]  = 1.0
        df_bp.loc[first_row_index:completed, percent_complete] = 1.0
        df_bp.loc[first_row_index:completed, 'c_sum'] = df_x.loc[first_row_index:completed, 'Work'].cumsum()
        df_x = df_x.copy()
        df_x['r_sum'] = df_x.loc[::-1, 'Work'].cumsum()[::-1]

        ''' set start date by subtracting workdays from the due_date (dd) '''
        completed_df = df_x.loc[first_row_index:completed]  # completed df block
        if len(completed_df) == 0:
            continue
        completed_df    = completed_df.copy()
        completed_work  = completed_df['Work'].sum()
        status_row_work = df_bp.loc[status_row_index, 'Work']
        total_work      = completed_work + status_row_work

        ''' update all completed life cycle rows  '''
        start_date = wd.workday(p_due_date, days=int(-total_work), holidays=holidays, weekends=weekends)
        p_finish   = wd.workday(start_date, days=int(total_work),  holidays=holidays, weekends=weekends)
        sd_series  = completed_df.apply(lambda x: date_calc(sd=start_date, days=int(total_work - x.r_sum)), axis=1)
        index = completed_df.index
        if rule != 'BP':
            x = 1
            # df_bp.loc[index, percent_complete] = 0.0
        else:
            df_bp.loc[index, 'Start'] = sd_series.dt.strftime('%d/%m/%Y')
        try:
            df_bp.loc[p_index, 'planned finish'] = p_finish.strftime('%d/%m/%Y')
        except ValueError:
            continue

    return df_bp

def update_mom_block():
    for p in parent_list:
        df_m = df_bp[(df_bp['row_type'].isin(['MoM']))    & (df_bp['parent'].isin([p]))]  # MoM  block df
        ''' -----> MoM Block <----- '''
        df_m = df_m.replace({'status': config['mom_status_map']})
        df_m[pc] = df_m['status'].apply(lambda x: 1.0 if x == 'completed' else None)
        df_m = df_m[['RN', pc, 'p_rule', 'due_date', 'Work']]
        df_m['due_date'] = pd.to_datetime(df_m['due_date'])
        df_c = df_m.copy()
        df_i = df_c.copy()
        df_j = df_c.copy()

        ''' df_mom_completed: all MoM records that are complete they should have due_date with the completed date '''
        df_mom_completed = df_m.copy()
        df_mom_completed = df_mom_completed[df_mom_completed[pc] == 1.0]
        idx_c = df_mom_completed.index
        try:
            df_bp.loc[idx_c, 'Start']  = pd.to_datetime(df_mom_completed.due_date).dt.strftime('%d/%m/%Y')
            df_bp.loc[idx_c, 'Finish'] = pd.to_datetime(df_mom_completed.due_date).dt.strftime('%d/%m/%Y')
        except AttributeError as ex:
            print(p, ex)
            quit(0)
        df_bp.loc[idx_c, pc]         = df_mom_completed[pc]
        # df_bp.loc[idx_c, CT]         = config['constraint_type'].get('MFO')
        # df_bp.loc[idx_c, CD]         = df_mom_completed.due_date.dt.strftime('%d/%m/%Y')
        ''' finished df_mom_completed'''

        if len(df_c) == 0:
            continue
        df_c['next_dd'] = df_c['due_date'].shift(-1)
        df_c.loc[df_c.index.max(), 'next_dd'] = df_c.loc[df_c.index.max(), 'due_date']
        df_c['Start'] = df_c['due_date'].shift(1)
        df_c = df_c[(~df_c['due_date'].isnull())]
        c_index = df_c.index
        i_min = df_c.index.min()
        start_date = pd.to_datetime(df_c.loc[i_min, 'due_date'])

        df_c.loc[i_min, 'Start'] = date_calc(sd=start_date, days= -int(df_c.loc[i_min, 'Work']))

        # print('process_id: {}'.format(p))
        ''' stand alone MoM tasks '''
        df_i = df_i[(df_i[percent_complete] != 1) & (df_i['p_rule'] == 0)]
        if len(df_i) > 0:
            df_i['Start'] = df_i.apply(lambda x: date_calc(sd=x.due_date, days=-int(x.Work)), axis=1)
            df_c.loc[df_i.index, 'Start'] = df_i['Start']

        df_j = df_j[(df_j[percent_complete] == 1) & (df_j['p_rule'] == 0)]
        if len(df_j) > 0:
            df_j['Start'] = df_j.apply(lambda x: date_calc(sd=x.due_date, days=-int(x.Work)), axis=1)
            df_c.loc[df_j.index, 'Start'] = df_j['Start']

        df_cx = df_c[df_c['Start'].isnull()]
        mn    = df_cx.index.min()
        mx    = df_cx.index.max()
        df_c.loc[mn: mx, 'Start'] = df_cx.apply(lambda x: date_calc(sd=x.due_date, days=-int(x.Work)), axis=1)

        df_c['Duration'] = df_c.apply(lambda x: len(pd.date_range(x.Start, x.due_date, freq=custombday)), axis=1)
        df_bp.loc[c_index, 'Start']    = df_c.Start.dt.strftime('%d/%m/%Y')
        df_bp.loc[c_index, 'Finish']   = df_c.due_date.dt.strftime('%d/%m/%Y')
        df_bp.loc[c_index, 'Duration'] = df_c.Duration
        df_bp.loc[c_index, 'Work']     = df_c.Duration
        # df_bp.loc[c_index, CT] = config['constraint_type'].get('MFO')
        # df_bp.loc[c_index, CD] = df_c.due_date.dt.strftime('%d/%m/%Y')
        if len(df_m) > 0:
            mom_index_min = df_m.index.min()
            f_d = df_m.loc[mom_index_min, 'due_date']
            s_d = date_calc(sd=f_d, days= -int(df_m.loc[mom_index_min, 'Work']))
            df_m.loc[mom_index_min, 'Start'] = s_d
            df_bp.loc[mom_index_min, CT]     = config['constraint_type'].get('MSO')
            df_bp.loc[mom_index_min, CD]     = s_d.strftime('%d/%m/%Y')

    return df_bp

def logmar_handler():
    """ df_dv is the dv sheet compiled in skeleton file """
    df_dv.set_index('lc-RN', inplace=True)
    df_dv.columns = ('d-ind', 'd-start', 'd-finish', 'd-pc', 'd-duration')
    df_dv['d-ind'] = df_dv['d-ind'].astype(int)
    dev_map = df_dv.to_dict()

    df_bp['d-ind']      = df_bp.RN.map(dev_map.get('d-ind'))
    df_bp['d-pc']       = df_bp.RN.map(dev_map.get('d-pc'))
    df_bp['d-start']    = pd.to_datetime(df_bp.RN.map(dev_map.get('d-start')))
    df_bp['d-finish']   = pd.to_datetime(df_bp.RN.map(dev_map.get('d-finish')))
    df_x = df_bp[df_bp['d-ind'] > 0]
    df_x = df_x.copy()
    df_x['d-duration'] = df_x.apply(
        lambda x: len(
            pd.date_range(x['d-start'], x['d-finish'], freq='C')
        ), axis=1
    )
    df_x['d-start']    = df_x['d-start'].dt.strftime('%d/%m/%Y')
    df_x['d-finish']   = df_x['d-finish'].dt.strftime('%d/%m/%Y')

    """ !!! Override default behaviour. This is to deal with all development rows received from logmar """
    for index, row in df_x.iterrows():
        df_bp.loc[index, 'source']   = 'logmar'
        df_bp.loc[index, 'Start']    = row['d-start']
        df_bp.loc[index, 'Finish']   = row['d-finish']
        df_bp.loc[index, 'Duration'] = row['d-duration']
        df_bp.loc[index, 'Work']     = row['d-duration']
        df_bp.loc[index, pc]         = row['d-pc']
        df_bp.loc[index, CD]         = row['d-finish']
        df_bp.loc[index, CT]         = config['constraint_type'].get('MFO')

    return df_bp

def predecessor_handler():
    df_bp['p_rule'] = df_bp['p_rule'].fillna('0')
    df_bp['p_rule'] = df_bp['p_rule'].apply(lambda x: int(x) if isinstance(x, float) else str(x))
    cn = [i for (i, j) in enumerate(df_bp.columns) if j == 'row_number'][0]
    for row in df_bp.itertuples():
        index  = row.Index
        p_rule = str(df_bp.loc[index, 'p_rule'])
        rn     = int(df_bp.loc[index, 'row_number'])

        p_list = []
        if p_rule != '0':
            [p_list.append(str(rn - int(p))) for p in p_rule.split(',')]
            df_bp.loc[index, 'Predecessors'] = ','.join(p_list)

        ''' the following section is to add predecessor to first line of LC block '''
        if row.row_type == 'parent':
            rules      = df_bp[(df_bp['parent'] == row.parent) & (df_bp['row_type'].isin(['rule']))]
            rule_index = rules.index.min()  # first line of the LC block found
            df_m       = df_bp[(df_bp['parent'] == row.parent) & (df_bp['row_type'].isin(['MoM']))]
            p_line     = str(df_m.index.max())
            df_bp.loc[rule_index, 'Predecessors'] = [p_line if len(df_m) > 0 else '']

    return df_bp

def m4n_deliverables_as_successors():
    """ This is to deal with all m4n and header delivery rows which are predecessors/milestones to development tasks """
    df_ems = df_bp[
        (df_bp['row_type'].isin(['m4n-dl', 'm4n-ms'])) &
        (df_bp['source'].notnull()) |
        (df_bp['Task Mode'] == 'Manually Scheduled')
    ]
    for index, row in df_ems.iterrows():
        dd = row.due_date
        if row['Task Mode'] != 'Manually Scheduled':
            dd = row.due_date.strftime('%d/%m/%Y')
        df_bp.loc[index, 'Start']    = dd
        df_bp.loc[index, 'Finish']   = dd
        df_bp.loc[index, 'Duration'] = 0
        df_bp.loc[index, 'isMS']     = 1
        df_bp.loc[index, CD]         = dd
        df_bp.loc[index, CT]         = config['constraint_type'].get('MFO')
        df_bp.loc[index, 'source']   = row.source

    """ This block is to assign m4n predecessors to the development tasks """
    df_ems = df_ems.copy()
    df_ems['row_number'] = df_ems.index
    df_ems = df_ems[['source', 'row_number']]
    df_ems.set_index('source', inplace=True)
    ems_map = df_ems.to_dict()

    """ This block is development requirements which have m4n predecessors (m4n-WBS) """
    df_m4n = pd.read_excel(fr, sheet_name='m4n-preds')
    df_m4n = df_m4n.copy()
    for index, row in df_m4n.iterrows():
        row.WBS = re.subn('\n', '', row.WBS)[0]
        wbs_list = row.WBS.split(',')
        predecessor_list = []
        for wbs in wbs_list:
            predecessor_list.append(str(int(ems_map.get('row_number').get(wbs))))
        try:
            rn        = df_bp[df_bp['xRN'] == row.xRN].RN.values[0]
        except IndexError as ex:
            print(row.RN, ex)
        x_df      = df_bp[df_bp['parent'] == rn]
        lc_index  = x_df.index.min() + 5
        p_list    = ','.join(predecessor_list)
        df_bp.loc[lc_index, 'Predecessors'] = p_list
        df_bp.loc[lc_index, CT] = config['constraint_type'].get('ASAP')
        df_bp.loc[lc_index, CD] = None
        """ override the rule, since what's in logmar file takes priority """
        if df_bp.loc[lc_index, 'source'] == 'logmar':
            df_bp.loc[lc_index, CD] = df_bp.loc[lc_index, 'Finish']
            df_bp.loc[lc_index, CT] = config['constraint_type'].get('MFO')

    return df_bp

def update_interfaces():
    cn = [i for (i, j) in enumerate(df_bp.columns) if j == 'row_number'][0]

    df_interfaces           = pd.read_excel(fn, sheet_name='interfaces')
    df_interfaces           = df_interfaces[~df_interfaces['dependency'].isnull()]
    df_interfaces['groups'] = df_interfaces['dependency'].str.split(',')
    df_interfaces           = df_interfaces[['RN', 'process', 'dependency', 'groups', 'Name']]

    ''' create parent list with r_group in the following structure xx.yy.GRP i.e. 01.01.INT '''
    df_parent = df_bp[df_bp['row_type'] == 'parent']
    df_parent = df_parent.copy()
    df_parent['r_group'] = df_parent['RN'].apply(lambda x: '.'.join(x.split('.')[0:3]))
    df_parent = df_parent[['row_number', 'RN', 'Name', 'r_group', 'row_type', 'rule']]

    for index, row in df_interfaces.iterrows():
        int_rn  = row.RN + '.02.01'  # "interface milestone" line from the Interface LC block
        int_idx = df_bp[(df_bp['RN'] == int_rn)]['row_number'].values[0]  # the index of the above

        predecessor_list = []
        for group in row.groups:
            group  = group.strip()
            p_list = df_parent[df_parent['r_group'] == group]['RN']  # all parents that belong to one dependent interface group
            for parent in p_list:
                df_x = df_bp[df_bp['parent'] == parent]
                r = pd.Index(df_x).max()
                predecessor_list.append(str(r[cn]))
        df_bp.loc[int_idx, 'Predecessors'] = ','.join(sorted(predecessor_list))
    return df_bp

def assign_to_major_ms():
    """ This part is predecessors assignments per domain and per rule i.e. SAP, INT etc. """
    domain_list = [i for i in df_bp.domain.unique() if isinstance(i, str)]
    n = 4
    ms_split_p = '00.10.02.'
    x = 0
    d_dict = {}
    for domain in domain_list:
        d_list = []
        s_dict = {}
        for rule in ['SAP', 'INT']:
            s_list = []
            ms_split_prefix = ms_split_p + rule + '.' + domain + '.'
            p_list = []
            alias = rule + ': ' + domain
            domain_ms_index = df_bp[df_bp['alias'].isin([alias])].index
            ms_name = config['project_milestones'].get(rule)
            task_list = df_bp[
                ( df_bp['rule'] == rule ) &
                ( df_bp['Name'].isin([ms_name] )) &
                ( df_bp['domain'] == domain )
            ]['row_number'].sort_values()
            task_list = [str(int(t)) for t in task_list]
            p_list.append(task_list)
            if len(p_list) > 0:
                flat_list = [item for sublist in p_list for item in sublist]
                splited = [flat_list[i::n] for i in range(n)]
                ms_line = 0
                for s in splited:
                    ms_line += 1
                    rn = ms_split_prefix + f'{ms_line:0>2}'
                    try:
                        ms_line_index = df_bp[df_bp['RN'] == rn].index.values[0]
                    except IndexError as ex:
                        continue
                    x = ','.join(s)
                    df_bp.loc[ms_line_index, 'Predecessors'] = x
                    s_list.append(str(ms_line_index))
                    s_dict.update({rule: s_list})
            d_list.append(s_list)
            d_dict.update({domain: s_dict})

    list_01 = []
    list_02 = []
    list_01.append(d_dict.get('01').get('SAP'))
    list_01.append(d_dict.get('02').get('SAP'))

    list_02.append(d_dict.get('01').get('INT'))
    list_02.append(d_dict.get('02').get('INT'))

    list_01 = [item for sublist in list_01 for item in sublist]
    list_02 = [item for sublist in list_02 for item in sublist]
    df_bp.loc[25, 'Predecessors'] = ','.join(list_01)
    df_bp.loc[26, 'Predecessors'] = ','.join(list_02)

    """ This part is predecessors assignments from Blue Print project milestones to major milestones """
    s = 'אישור שגיב שרביט'
    rn_list = df_bp[df_bp['Name'].isin([s])]['row_number'].to_list()
    rn_list = [str(x) for x in rn_list]
    rn_list = ','.join(rn_list)
    df_bp.loc[24, 'Predecessors'] = rn_list

def assign_pred_to_dev_lc():
    # df_bi = pd.read_excel(bi, sheet_name='Task_Table1')
    df_bi = df_bp.copy()
    s = 'אישור שגיב שרביט'

    df_bi = df_bi[df_bi['Name'] == s]
    df_bi['r_group'] = df_bi['RN'].apply(lambda x: '.'.join(x.split('.')[0:2]))

    df_bi = df_bi[df_bi['Name'] == s]
    df_bi['r_group'] = df_bi['RN'].apply(lambda x: '.'.join(x.split('.')[0:2]))

    df_x1 = df_bp[df_bp['Name'] == 'תכנון והתחלת פיתוח']
    df_x1 = df_x1.copy()
    df_x1['r_group'] = df_x1['RN'].apply(lambda x: '.'.join(x.split('.')[0:2]))

    for g in df_x1['r_group'].unique():
        df_x2 = df_x1[df_x1['r_group'] == g]
        df_b2 = df_bi[df_bi['r_group'] == g]
        df_x2 = df_x2.copy()
        df_b2 = df_b2.copy()
        try:
            r_n = str(int(df_b2.get('row_number')))
            s_start = df_b2.get('Finish').values[0]
        except TypeError:
            print(g)
        df_bp.loc[df_x2.index, 'Predecessors'] = r_n
        df_bp.loc[df_x2.index, 'Start'] = s_start

def mark_milestone_complete():
    """ Sub Task Handler - if Sub Task Block is empty then mark as completed """
    sub_tasks = df_bp[df_bp['Name'] == 'Sub Tasks']['RN'].to_list()
    for s in sub_tasks:
        rn = df_bp[df_bp['RN'] == s]['row_number'].to_list()[0]
        st = df_bp[df_bp['RN'].str.contains(s)]['RN'].to_list()
        lx = len(st)
        if lx == 1:
            df_bp.loc[rn, pc] = 1.0
            df_bp.loc[rn + 2, pc] = 1.0
    """ update milestone percent complete, if all of its predecessors are completed """
    df_bp['Predecessors'] = df_bp['Predecessors'].astype(str)
    df_bp['Predecessors'] = df_bp['Predecessors'].replace({'nan': None})
    df_bp['Predecessors'] = df_bp['Predecessors'].replace({'': None})
    df_bp['p_list'] = df_bp['Predecessors'].str.split(',')
    df_milestones = df_bp[df_bp['isMS'] == 1]
    for index, row in df_milestones.iterrows():
        if row.p_list:
            parent = row.parent
            p_list     = row.p_list
            p_list_len = len(row.p_list)
            p_sum      = 0
            r_n        = row['row_number']
            dates = []
            for r in p_list:
                r = int(r.strip())
                p        = df_bp.loc[r, pc]
                p_finish = df_bp.loc[r, 'Finish']
                if p_finish:
                    dates.append(p_finish)
                p_sum += p
            if p_sum == p_list_len and dates:
                dt_max = max(dates)
                df_bp.loc[r_n, pc]       = 1.0
                df_bp.loc[r_n, 'Finish'] = dt_max
                df_bp.loc[r_n, 'Start']  = dt_max
                # df_bp.loc[r_n, CT]       = config['constraint_type'].get('MFO')
                # df_bp.loc[r_n, CD]       = dt_max

def epilog():
    df_bp['isMS']     = df_bp['isMS'].fillna(0)
    df_bp['Work']     = df_bp['Work'].fillna(0)
    # df_bp['Duration'] = df_bp['Work']
    df_bp['Duration'] = df_bp['Duration'].fillna(0)

    df_bp['isMS']     = df_bp['isMS'].apply(lambda x: x if x == 1 else 0)
    df_bp['Work']     = df_bp.apply(lambda x: 0 if x.isMS == 1 else x.Work,     axis=1)
    df_bp['Duration'] = df_bp.apply(lambda x: 0 if x.isMS == 1 else x.Duration, axis=1)
    df_bp['Deadline'] = None
    df_bp[pc] = df_bp[pc].fillna(0)
    df_bp[pc] = df_bp[pc].astype(float)
    df_bp[pc] = df_bp[pc].apply(lambda x: f'{x:.2f}')

    df_bp['Name'] = df_bp['Name'].apply(lambda x: re.subn(r'[\r\n]', '--', str(x))[0])
    df_bp['task_notes'] = df_bp['task_notes'].apply(lambda x: re.subn(r'[\r\n]', '--', str(x))[0])
    df_bp['task_notes_2'] = df_bp['task_notes_2'].apply(lambda x: re.subn(r'[\r\n]', '--', str(x))[0])

    return df_bp

''' Execution Order '''
if __name__ == '__main__':
    try:
        db = ms.mongo_connect()
    except Exception as e:
        print('Oops! could not connect to the DB: ', e.__class__, 'occurred')
        quit(-1)
    print('mongo_db connected successfully')

    # pd.options.mode.chained_assignment = None
    # isinstance(row.due_date, pd._libs.tslibs.nattype.NaTType)
    # pd.bdate_range(start=start_d, end='20/10/2020', weekmask=weekmask, holidays=holidays, freq='C')
    pd.options.mode.chained_assignment = 'raise'
    warning.filterwarnings('ignore')
    print(125 * '=')
    with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    percent_complete = '% Complete'
    pc = '% Complete'
    CT = 'Constraint Type'
    CD = 'Constraint Date'
    OL = 'Outline Level'

    (MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
    holidays = pd.to_datetime(config['holidays'])
    weekends = (FRI, SAT)

    weekmask = config['bdays'].get('bdays')
    custombday = pd.offsets.CustomBusinessDay(weekmask=weekmask, holidays=holidays)
    # print(pd.date_range('20201012', '20201031', freq=custombday))

    start_s = time.time()
    dt = True if config['history'].get('date') > '27/12/2020' else False
    history = config['execution_params'].get('history')
    if history:
        print('History execution')
        path = config['history'].get('path')

        fn = os.path.join(path, config['history'].get('name'))
        fr = os.path.join(path, config['history'].get('pmo'))
        fo = os.path.join(path, config['history'].get('out'))
        fs = os.path.join(path, config['history'].get('skeleton'))
        fz = os.path.join(path, config['history'].get('consolidated'))
    else:
        path = config['config'].get('path')

        fn = os.path.join(path, config['config'].get('name'))
        fr = os.path.join(path, config['config'].get('pmo'))
        fo = os.path.join(path, config['config'].get('out'))
        fs = os.path.join(path, config['config'].get('skeleton'))
        fz = os.path.join(path, config['config'].get('consolidated'))
        bi = os.path.join(path, config['config'].get('p3BI'))
        # f1 = os.path.join(path, config['config'].get('m4n'))

    writer = pd.ExcelWriter(fo, engine='xlsxwriter')
    workbook = writer.book

    df_dv      = pd.read_excel(fs, sheet_name='dev')  # logmar data inside the skeleton file
    df_bp      = pd.read_excel(fs, sheet_name='skeleton')
    df_rules_x = pd.read_excel(fr, sheet_name='Lines')
    df_bp['process'] = df_bp['process_id'].apply(lambda x: x.split(':')[0] if isinstance(x, str) else None)
    df_bp['domain']  = df_bp['domain_id'].apply(lambda x: x.split(':')[0] if isinstance(x, str) else None)

    del df_bp['Unnamed: 0']

    # 1. read MoM tasks from anywhere
    # df_bp = pd.concat([df_bp, insert_mom('MoM'), insert_mom('scm_tasks')], ignore_index=True)
    df_bp = pd.concat([df_bp, insert_mom('MoM')], ignore_index=True)
    print('{:50}: {}'.format('01. read MoM tasks from anywhere df_bp.shape', df_bp.shape))

    # 2. write consolidated file of all inputs
    df_bp = write_consolidated()
    print('{:50}:'.format('02. write consolidated file of all inputs'))

    # 3. create parent list
    parent_list = df_bp['parent'].unique()
    parent_list = [x for x in parent_list if str(x) != 'nan']
    print('{:50}: {}'.format('03. create parent list', f'{len(parent_list):0>5}'))

    # 4. sorting by RN
    df_bp.sort_values(by=['RN'], inplace=True)
    df_bp.index         = np.arange(1, len(df_bp) + 1)
    df_bp['row_number'] = df_bp.index
    print('{:50}:'.format('04. sorting by Requirement Number'))

    # 5. Update Life Cycle and MoM blocks
    start      = time.time()
    # df_bp[percent_complete] = df_bp['status'].apply(lambda xz: 1.0 if xz in config['step'].get('completed') else 0)
    df_bp['Finish'] = None
    df_bp = update_lc_block()
    df_bp = update_mom_block()
    print('{:50}: {:05.2f}'.format('05. Update Life Cycle and MoM blocks', time.time() - start))

    df_bp.sort_values(by=['RN'], inplace=True)
    df_bp.index         = np.arange(1, len(df_bp) + 1)
    df_bp['row_number'] = df_bp.index

    # 5a. Predecessor Handler
    start      = time.time()
    df_bp = df_bp.copy()
    df_bp = logmar_handler()
    print('{:50}: {:05.2f}'.format('5a. Logmar Handler', time.time() - start))

    # 6. Predecessor Handler
    start      = time.time()
    df_bp = predecessor_handler()
    print('{:50}: {:05.2f}'.format('06. Predecessor Handler', time.time() - start))

    # 6a. M4N Successor Handler
    start      = time.time()
    df_bp = m4n_deliverables_as_successors()
    print('{:50}: {:05.2f}'.format('6a. M4N Successor Handler', time.time() - start))

    # 7. Interface Handler
    start      = time.time()
    # df_bp = update_interfaces()
    print('{:50}: {:05.2f}'.format('07. Interface Handler', time.time() - start))

    start      = time.time()
    print('{:50}:'.format('08. Assign tasks to Major Milestones'))
    assign_to_major_ms()
    assign_pred_to_dev_lc()

    # 9. Mark Milestones as Completed as per all of its predecessors
    print('{:50}:'.format('09. Mark completed milestones'))
    mark_milestone_complete()

    # 10. Epilog Handler
    print('{:50}:'.format('10. Epilog Handler, mark completed milestones'))
    epilog()

    print('{:50}: {:05.2f}'.format('10. majorMS and epilog', time.time() - start))
    print(125*'=')
    print('{:50}: {:05.2f}'.format('execution time', time.time() - start_s))
    start      = time.time()

    df_notes = df_bp[['RN', 'task_notes', 'task_notes_2']]

    df_notes.to_excel(writer, sheet_name='task_notes')
    df_bp.to_excel(writer, sheet_name='Sheet1')

    df_bp['RN'] = df_bp['RN'].astype(str)
    df_bp[OL] = df_bp['RN'].apply(lambda x: len(x.split('.')))
    mask = df_bp['row_type'].apply(lambda x: x not in ['header', 'm4n-dl', 'm4n-ms'])
    df_bp = df_bp.copy()
    df_bp.loc[mask, OL] = df_bp.loc[mask, OL] + 1

    df_bp['source'] = df_bp['source'].astype(str)
    df_bp = df_bp.rename(columns=config['dfP'])
    df_bp = df_bp[config['dfP_columns']]

    df_bp.to_excel(writer, sheet_name='Task_Table1')
    worksheet = writer.sheets['Task_Table1']
    writer.save()
    print('{:50}: {:05.2f}'.format('writing p3-out.xlsx file', time.time() - start))
    print('{:50}: {:05.2f}'.format('execution elapse time', time.time() - start_s))

