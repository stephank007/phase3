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
    df_mx['due_date']  = pd.to_datetime(df_mx['due_date'])
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
        rule     = p_df.rule.values[0]
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
        df_bp.loc[index, 'Start'] = sd_series.dt.strftime('%d/%m/%Y')
        # df_bp.loc[index, CT]      = config['constraint_type'].get('MSO')
        # df_bp.loc[index, CD]      = sd_series.dt.strftime('%d/%m/%Y')
        # TODO: need to set the planned finish in reference to actual reporting and remainder of planned work
        try:
            df_bp.loc[p_index, 'planned finish'] = p_finish.strftime('%d/%m/%Y')
        except ValueError:
            continue

    return df_bp

def update_mom_block():
    for p in parent_list:
        df_m = df_bp[(df_bp['row_type'].isin(['MoM']))    & (df_bp['parent'].isin([p]))]  # MoM  block df
        ''' -----> MoM Block <----- '''
        df_m = df_m[['RN', percent_complete, 'p_rule', 'due_date', 'Work']]
        df_c = df_m.copy()
        df_i = df_c.copy()
        df_j = df_c.copy()

        ''' df_mom_completed: all MoM records that are complete they should have due_date with the completed date '''
        df_mom_completed = df_m.copy()
        df_mom_completed = df_mom_completed[df_mom_completed[percent_complete] == 1]
        idx_c = df_mom_completed.index
        df_bp.loc[idx_c, 'Start']    = df_mom_completed.due_date.dt.strftime('%d/%m/%Y')
        df_bp.loc[idx_c, 'Finish']   = df_mom_completed.due_date.dt.strftime('%d/%m/%Y')
#        df_bp.loc[idx_c, CT]         = config['constraint_type'].get('MFO')
#        df_bp.loc[idx_c, CD]         = df_mom_completed.due_date.dt.strftime('%d/%m/%Y')
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
#        df_bp.loc[c_index, CT] = config['constraint_type'].get('MFO')
#        df_bp.loc[c_index, CD] = df_c.due_date.dt.strftime('%d/%m/%Y')
        if len(df_m) > 0:
            mom_index_min = df_m.index.min()
            f_d = df_m.loc[mom_index_min, 'due_date']
            s_d = date_calc(sd=f_d, days= -int(df_m.loc[mom_index_min, 'Work']))
            df_m.loc[mom_index_min, 'Start'] = s_d
            df_bp.loc[mom_index_min, CT]     = config['constraint_type'].get('MSO')
            df_bp.loc[mom_index_min, CD]     = s_d.strftime('%d/%m/%Y')

    return df_bp

def assign_pred_to_dev_lc():
    #df_bi = pd.read_excel(bi, sheet_name='Task_Table1')
    df_bi = df_bp.copy()
    s = 'אישור שגיב שרביט'

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
        r_n = df_b2.get('ID')
        if r_n:
            df_bp.loc[df_x2.index, 'Predecessors'] = int(r_n)

def predecessor_handler():
    # df_bp['p_rule'] = df_bp['p_rule'].fillna('0')
    # df_bp['p_rule'] = df_bp['p_rule'].apply(lambda x: int(x) if isinstance(x, str)  else None)
    # df_bp['p_rule'] = df_bp['p_rule'].apply(lambda x: None if x == 'nan' else x)

    cn = [i for (i, j) in enumerate(df_bp.columns) if j == 'row_number'][0]
    for row in df_bp.itertuples():
        index  = row.Index
        p_rule = str(df_bp.loc[index, 'p_rule'])
        rn     = int(df_bp.loc[index, 'row_number'])

        p_list = []
        #if p_rule != '0':
        if not p_rule == 'nan':
            [p_list.append(str(rn - int(p))) for p in p_rule.split(',')]
            [print(rn) for p in p_list if p == '']
            df_bp.loc[index, 'Predecessors'] = ','.join(p_list)

        ''' the following section is to add predecessor to first line of LC block '''
        if row.row_type == 'parent':
            rules      = df_bp[(df_bp['parent'] == row.parent) & (df_bp['row_type'].isin(['rule']))]
            rule_index = rules.index.min()  # first line of the LC block found
            df_m       = df_bp[(df_bp['parent'] == row.parent) & (df_bp['row_type'].isin(['MoM']))]
            p_line     = str(df_m.index.max())
            df_bp.loc[rule_index, 'Predecessors'] = [p_line if len(df_m) > 0 else None]
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
        int_rn  = row.RN + '.02.01'       # "interface milestone" line from the Interface LC block
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
    project_ms = config['project_milestones']
    for key, value in project_ms.items():
        index = df_bp[df_bp['alias'].isin([key])].index
        task_list = df_bp[df_bp['Name'].isin([value])]['row_number'].sort_values()
        task_list = [str(t) for t in task_list]
        task_list = ','.join(task_list)
        df_bp.loc[index, 'Predecessors'] = task_list

    contract_ms = config['contract_ms']
    for key, value in contract_ms.items():
        index = df_bp[df_bp['Name'].isin([key])].index
        task_list = df_bp[df_bp['Name'].isin([value])]['row_number'].sort_values()
        task_list = [str(t) for t in task_list]
        task_list = ','.join(task_list)
        df_bp.loc[index, 'Predecessors'] = task_list

def assign_complete_to_milestones():
    df_bp['p_list'] = df_bp['Predecessors'].apply(lambda x: x.split(',') if isinstance(x, str) else None)
    for index, row in df_bp.iterrows():
        p_list = row.p_list
        if p_list:
            p_list_len = len(p_list)
            p_sum = 0
            r_n = row['row_number']
            dates = []
            for r in row.p_list:
                if r > '0':
                    r = int(r)
                    p = df_bp.loc[r, pc]
                    p_finish = df_bp.loc[r, 'Finish']
                    if p_finish:
                        dates.append(p_finish) if isinstance(p_finish, str) else ' '
                        p_sum += p
                    if p_sum == p_list_len:
                        dt_max = max(dates)
                        df_bp.loc[r_n, pc] = 1.0
                        df_bp.loc[r_n, pc] = 1.0
                        df_bp.loc[r_n, 'Finish'] = dt_max
                        df_bp.loc[r_n, 'Start'] = dt_max
    return df_bp
def epilog(df_x=pd.DataFrame()):
    """ update milestone if all of its predecessors are complete"""
    df_x = df_x.copy()
    df_x['p_list'] = df_x['Predecessors'].str.split(',')
    for index, row in df_x.iterrows():
        p_list_len = len(row.p_list)
        p_sum      = 0
        r_n = row['row_number']
        dates = []
        for r in row.p_list:
            try:
                r = int(r.strip())
            except ValueError:
                ''' a parent with no sub tasks '''
                # print('RN: {} MS has no predecessors'.format(row.RN))
                dt_max = '29/12/2020'
                df_bp.loc[r_n, percent_complete] = 1.0
                df_bp.loc[r_n, 'Finish'] = dt_max
                df_bp.loc[r_n, 'Start']  = dt_max
                # df_bp.loc[r_n, CT]       = config['constraint_type'].get('MFO')
                # df_bp.loc[r_n, CD]       = dt_max
                continue
            p        = df_bp.loc[r, percent_complete]
            p_finish = df_bp.loc[r, 'Finish']
            dates.append(p_finish) if isinstance(p_finish, str) else ' '
            p_sum += p
        if p_sum == p_list_len:
            try:
                dt_max = max(dates)
            except ValueError:
                df_bp.loc[r_n, percent_complete] = 1.0
                # print('{} has empty sequence: {}'.format(r_n, row.RN))
                continue
            df_bp.loc[r_n, percent_complete] = 1.0
            df_bp.loc[r_n, 'Finish'] = dt_max
            df_bp.loc[r_n, 'Start']  = dt_max
            # df_bp.loc[r_n, CT]       = config['constraint_type'].get('MFO')
            # df_bp.loc[r_n, CD]       = dt_max

    df_bp[percent_complete]     = df_bp[percent_complete].astype(float)
    df_bp['isMS']     = df_bp['isMS'].fillna(0)
    df_bp['Work']     = df_bp['Work'].fillna(0)
    df_bp['Duration'] = df_bp['Work']
    df_bp['Duration'] = df_bp['Duration'].fillna(0)

    df_bp['isMS']     = df_bp['isMS'].apply(lambda x: x if x == 1 else 0)
    df_bp['Work']     = df_bp.apply(lambda x: 0 if x.isMS == 1 else x.Work,     axis=1)
    df_bp['Duration'] = df_bp.apply(lambda x: 0 if x.isMS == 1 else x.Duration, axis=1)
    df_bp['Deadline'] = None

    # df_bp['ms_complete']   = df_bp.apply(lambda x: 1 if x.isMS == 1 and x[percent_complete] == 1 else 0, axis=1)
    # df_bp['Actual Finish'] = df_bp.apply(lambda x: x.Finish if x.ms_complete == 1 else None, axis=1)
    # df_bp['Finish']        = df_bp.apply(lambda x: x.Start  if x.ms_complete != 1 else None, axis=1)

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

    start_s = time.time()

    with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

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

    (MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
    holidays = pd.to_datetime(config['holidays'])
    weekends = (FRI, SAT)

    weekmask = config['bdays'].get('bdays')
    custombday = pd.offsets.CustomBusinessDay(weekmask=weekmask, holidays=holidays)
    # print(pd.date_range('20201012', '20201031', freq=custombday))

    df_rules_x = pd.read_excel(fr, sheet_name='Lines')
    df_bp = pd.read_excel(fs, sheet_name='skeleton')
    del df_bp['Unnamed: 0']

    percent_complete = '% Complete'
    pc = '% Complete'
    CT = 'Constraint Type'
    CD = 'Constraint Date'
    OL = 'Outline Level'
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
    df_bp[percent_complete] = df_bp['status'].apply(lambda xz: 1.0 if xz in config['step'].get('completed') else 0)
    df_bp['Finish'] = None
    df_bp = update_lc_block()
    df_bp = update_mom_block()
    print('{:50}: {:05.2f}'.format('05. Update Life Cycle and MoM blocks', time.time() - start))

    df_bp.sort_values(by=['RN'], inplace=True)
    df_bp.index         = np.arange(1, len(df_bp) + 1)
    df_bp['row_number'] = df_bp.index

    # 6. Predecessor Handler
    start      = time.time()
    df_bp = df_bp.copy()
    df_bp = predecessor_handler()
    print('{:50}: {:05.2f}'.format('06. Predecessor Handler', time.time() - start))

    # 7. Interface Handler
    start      = time.time()
    # df_bp = update_interfaces()
    print('{:50}: {:05.2f}'.format('07. Interface Handler', time.time() - start))

    start      = time.time()
    print('{:50}:'.format('08. Assign tasks to Major Milestones'))
    assign_to_major_ms()
    assign_pred_to_dev_lc()

    # 9. Epilog Handler
    print('{:50}:'.format('09. Epilog Handler, mark completed milestones'))
    df_bp = assign_complete_to_milestones()
#     df_zx = df_bp[
#         (df_bp['isMS'] == 1) &
#         (~df_bp['Predecessors'].isnull()) &
#         (~df_bp['row_type'].isnull())
#     ]
#     df_bp = epilog(df_x=df_zx)
#
#     df_zx = df_bp[
#         (df_bp['isMS'] == 1) &
#         (~df_bp['Predecessors'].isnull()) &
#         (df_bp['row_type'].isnull())
#     ]
#     df_bp = epilog(df_x=df_zx)
    print('{:50}: {:05.2f}'.format('10. majorMS and epilog', time.time() - start))
    print(125*'=')
    print('{:50}: {:05.2f}'.format('execution time', time.time() - start_s))
    start      = time.time()

    df_notes = df_bp[['RN', 'task_notes', 'task_notes_2']]

    writer = pd.ExcelWriter(fo, engine='xlsxwriter')
    workbook = writer.book

    df_notes.to_excel(writer, sheet_name='task_notes')
    df_bp.to_excel(writer, sheet_name='Sheet1')

    df_bp['RN'] = df_bp['RN'].astype(str)
    df_bp[OL] = df_bp.apply(lambda x: len(x['RN'].split('.')) + 1 if isinstance(x.row_type, str) else x[OL], axis = 1)

    df_bp = df_bp.rename(columns=config['dfP'])
    df_bp = df_bp[config['dfP_columns']]

    df_dv = pd.read_excel(fs, sheet_name='dev')
    df_dv.set_index('lc-RN', inplace=True)
    dev_map = df_dv.to_dict()

    df_bp['d-start']  = pd.to_datetime(df_bp.Text1.map(dev_map.get('d-start'))).dt.strftime('%d/%m/%Y')
    df_bp['d-finish'] = pd.to_datetime(df_bp.Text1.map(dev_map.get('d-finish'))).dt.strftime('%d/%m/%Y')
    df_bp['d-pc']     = df_bp.Text1.map(dev_map.get('d-pc'))

    df_bp['Start']  = df_bp.apply(lambda x: x['d-start']  if isinstance(x['d-start'],  str) else x.Start, axis=1)
    df_bp['Finish'] = df_bp.apply(lambda x: x['d-finish'] if isinstance(x['d-finish'], str) else x.Finish, axis=1)
    df_bp[pc]       = df_bp.apply(lambda x: x['d-pc']     if isinstance(x['d-pc'],   float) else x[pc], axis=1)
    df_bp['Task_Mode'] = df_bp.apply(lambda x: 'Manually Scheduled' if isinstance(x['d-start'],  str) else 'Auto Scheduled', axis=1)

    df_bp.to_excel(writer, sheet_name='Task_Table1')
    worksheet = writer.sheets['Task_Table1']
    writer.save()
    print('{:50}: {:05.2f}'.format('writing p3-out.xlsx file', time.time() - start))
    print('{:50}: {:05.2f}'.format('execution elapse time', time.time() - start_s))

