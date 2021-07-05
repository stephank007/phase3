import os
import codecs
import pandas as pd
import numpy as np
import yaml
import time
import re
import workday as wd

def prepare_domain():
    df_dm = pd.DataFrame()
    df = df_domain_reference[['RN', 'domain_id']].dropna()
    df_map = df.copy()
    for d in df.domain_id.unique():
        line = df[df['domain_id'] == d]
        rn = f'{int(line.RN.values[0]):0>2}'
        df_dm = df_dm.append(
            {
                'RN'       : f'{int(rn):0>2}',
                'domain_id': d,
                'row_type' : 'hd_domain',
                'Name'     : d
            }, ignore_index=True
        )
    ''' domain map '''
    domain_m = df_map[['RN', 'domain_id']]
    domain_m['RN'] = domain_m['RN'].apply(lambda x: f'{int(x):0>2}')
    domain_m.set_index('RN', inplace=True)
    domain_m = domain_m.to_dict()
    return df_dm, domain_m

def prepare_process():
    df_bp_x = pd.read_excel(fn, sheet_name='process_ref')
    df_bp_x['process'] = df_bp_x['process'].map(lambda x: f'{str(x):0>5}')
    status_map = config['program_status_map'].get('bp_status_map')
    df_bp_x = df_bp_x.rename(columns={'xRN': 'RN'})

    df_bp_x['parent']   = df_bp_x.get('RN')
    df_bp_x['row_type'] = 'parent'
    df_bp_x['Name']     = df_bp_x.get('process_id')
    df_bp_x['status']   = df_bp_x.status.map(status_map)
    df_bp_x['rule']     = 'BP'
    df_map = df_bp_x.copy()

    df_bp_x = df_bp_x[config['process_ref_columns']]
    print('{:23}: {}'.format('df_bp_x.shape', df_bp_x.shape))

    for p in df_bp_x.process.unique():
        df_p  = df_bp_x[df_bp_x['process'] == p]
        index = df_p.index
        df_p  = df_p.copy()
        df_p.loc[index, 'RN'] = p
        df_p.loc[index, 'row_type']  = 'hd'
        df_p.loc[index, 'Name'] = df_p['process_id'].values[0]
        df_bp_x = df_bp_x.append(df_p)
    df_bp_x = df_bp_x[config.get('process_ref_columns')]

    ''' process map '''
    process_m = df_map[['process', 'process_id']]
    process_m.set_index('process', inplace=True)
    process_m = process_m.to_dict()

    return df_bp_x, process_m

def prepare_interface():
    df_interfaces              = pd.read_excel(fn, sheet_name='interfaces')
    df_interfaces['parent']    = df_interfaces.get('RN')
    df_interfaces['row_type']  = 'parent'
    df_interfaces['ts_#']      = df_interfaces['t_shirt'].map(config.get('t_shirt_#'))

    for pi in df_interfaces.process.unique():
        df_i = df_interfaces[df_interfaces['process'] == pi]
        idx = df_i.index
        df_i = df_i.copy()
        df_i.loc[idx, 'RN'] = pi
        df_i.loc[idx, 'row_type'] = 'hd'
        df_i.loc[idx, 'Name'] = df_i['process_id'].values[0]
        df_i = df_i[['RN', 'Name', 'process', 'domain_id', 'process_id', 'row_type']]
        df_interfaces = df_interfaces.append(df_i)
    print('{:23}: {}'.format('bf_interfaces.shape', df_interfaces.shape))

    return df_interfaces

def insert_hd1_rows(df=pd.DataFrame()):
    df['Deadline'] = pd.to_datetime(df['Deadline'], errors='ignore')
    df['ts_#'] = df['t_shirt'].map(config.get('t_shirt_#'))

    df_hd = df[df['row_type'] == 'parent']
    df_hd = df_hd.copy()
    df_hd['hd_1'] = df_hd['RN'].apply(lambda x: '.'.join(x.split('.')[0:2]))
    df_hd['hd_1'] = df_hd.apply(lambda x: str(x.hd_1) + '.' + str(x.rule), axis=1)

    df_y = pd.DataFrame()
    for p in df_hd.hd_1.unique():
        line = df_hd[df_hd['hd_1'] == p]
        line = line.loc[line.index.min(), :]
        df_y = df_y.append(
            {
                'RN': p,
                'domain_id' : line['domain_id'],
                'process_id': line['process_id'],
                'row_type'  : 'hd_1',
                'Name'      : str(line['process_id']) + '.' + str(line['rule'])
            }, ignore_index=True
        )
    df = pd.concat([df, df_y], ignore_index=True, sort=False)

    return df

def insert_realization_requirements():
    """ create in_dev_map to identify which requirements is in realization """
    df_shortlist = pd.read_excel(f4, sheet_name='Sheet1')
    df_in_dev = df_shortlist.copy()
    df_in_dev.set_index('RN', inplace=True)
    in_dev_map = df_in_dev.to_dict()

    """ 
        df_dev is the full list of requirements --> it's RN is an old RN which is not mapped properly to its rule
        df_shortlist is what had been decided as the shortlist for development
    """
    df_dev = pd.read_excel(f1, sheet_name='דרישות')
    df_dev.rename(columns=config['realization_columns_map'], inplace=True)
    df_dev['in-dev']  = df_dev.RN.map(in_dev_map.get('Task Name'))
    df_dev['in-dev']  = df_dev['in-dev'].apply(lambda x: True if isinstance(x, str) else False)
    """ select realization eligible rows """
    df_dev = df_dev[df_dev['in-dev']]
    df_dev['d-start']     = df_dev.RN.map(in_dev_map.get('Start'))
    df_dev['d-finish']    = df_dev.RN.map(in_dev_map.get('Finish'))
    df_dev['d-pc']        = df_dev.RN.map(in_dev_map.get('% Complete'))
    df_dev['d-duration']  = df_dev.RN.map(in_dev_map.get('Duration'))
    df_dev['d-duration']  = df_dev['d-duration'].apply(lambda x: x.split(' ')[0])
    df_dev['d-duration']  = df_dev['d-duration'].astype(float)

    pmo_p = df_dev.columns[8]
    df_dev.rename(columns={
        pmo_p  : 'pmo_priority',
        'RN'   : 'xRN'
    }, inplace=True)
    df_dev['s_rule'] = df_dev['s_rule'].str.strip()

    """ map rule as per domain_ref--> logmar_rule"""
    s_rule_map = df_domain_reference[['logmar_rule', 'rule']]
    s_rule_map = s_rule_map[s_rule_map['logmar_rule'].notnull()]
    s_rule_map.set_index('logmar_rule', inplace=True)
    s_rule_map = s_rule_map.to_dict()
    df_dev['rule'] = df_dev['s_rule'].map(s_rule_map.get('rule'))
    # df_dev['rule'] = df_dev['s_rule'].map(config.get('s_rule_map'))
    # df_dev = df_dev[df_dev['rule'] != 'ignore']
    # df_dev = df_dev[df_dev['s_rule'].notnull()]
    """ logmar rule map complete"""

    """ !!!!! Not is use !!!!! -- map logmar notes to to ignore/open open: is an issue open to product manager """
    logmar_notes_map = df_domain_reference[['logmar_notes', 'rule']]
    logmar_notes_map.set_index('logmar_notes', inplace=True)
    logmar_notes_map = logmar_notes_map.to_dict()
    df_dev['logmar_rule'] = df_dev.logmar_notes.map(logmar_notes_map.get('rule'))
    """ logmar notes complete"""

    df_dev['process'] = df_dev['process_id'].apply(lambda x1: x1.split(':')[0])
    df_dev['domain']  = df_dev['domain_id'].apply(lambda x1: x1.split(':')[0])

    df_dev.sort_values(by=['domain', 'process', 'rule'], inplace=True)
    df_dev.reset_index(inplace=True)

    """ apply real RN as per the assigned rule ignoring realization file RN while keeping it as xRN """
    for process in df_dev['process_id'].unique():
        df_p = df_dev[df_dev['process_id'] == process]
        for rule in df_p['rule'].unique():
            df_rule = df_p[df_p['rule'] == rule]
            df_rule = df_rule.copy()
            df_rule['number'] = np.arange(1, len(df_rule) + 1)
            df_rule['number'] = df_rule['number'].apply(lambda x1: f'{x1:0>2}')
            df_rule['RN']     = df_rule['process'] + '.' + rule + '.' + df_rule['number']
            df_dev.loc[df_rule.index.min(): df_rule.index.max(), 'RN']   = df_rule['RN']
            df_dev.loc[df_rule.index.min(): df_rule.index.max(), 'rule'] = rule

    """ assigning the matching status to the realization requirements """
    df_rule_status = df_rl[df_rl['RN'].str.contains('.02.03')]
    df_rule_status = df_rule_status[['Rule', 'Name']]
    df_rule_status.set_index('Rule', inplace=True)
    status_map = df_rule_status.to_dict()
    df_dev['status']   = df_dev['rule'].map(status_map.get('Name'))
    # df_dev['status']   = 'תכנון והתחלת פיתוח'

    df_dev['lc-RN']    = df_dev['RN'] + '.02.03'
    df_dev['parent']   = df_dev.get('RN')
    df_dev['row_type'] = 'parent'
    df_dev['due_date'] = df_dev['d-finish'].apply(
        lambda x: wd.workday(
            pd.to_datetime(x, format='%d/%m/%Y'),
            days=10,
            holidays=holidays,
            weekends=weekends
        )
    )
    df_dev['d-finish'] = df_dev['due_date']
    df_dev['t_shirt']  = 'M'
    df_dev['source']   = 'requirements'
    df_dev['waiting']  = None
#     """ This chapter is adding the GEN tasks from the task file """
#     df_tsk = pd.read_excel(f3, sheet_name='משימות')
#     df_tsk.rename(columns=config['tasks_columns_map'], inplace=True)
#     df_tsk['status'] = df_tsk.status.map(config['tasks_status_map'])
#
#     df_tsk = df_tsk[df_tsk['RN'].notnull()]
#     mask = ~df_tsk['status'].isin(['בוטל', 'משימה בוצעה במלואה' ])
#     df_tsk = df_tsk[mask]
#     df_tsk['row_type'] = 'parent'
#     df_tsk['parent']   = df_tsk.get('RN')
#     df_tsk['source']   = 'tasks'
#     df_tsk['rule']     = 'GEN'
#
#     df_dev = pd.concat([df_dev, df_tsk], ignore_index=True)
#     df_dev = df_dev[config.get('realization_columns')]

    """ filter only what is in development """
    df_dev = df_dev[df_dev['RN'].notnull()]

    return df_dev

def assign_fields_from_RN(df=pd.DataFrame()):
    df_dev = df[df['row_type'] == 'parent']
    df_dev = df_dev.copy()

    df_dev['domain'] = df_dev['domain'].apply(lambda x: f'{int(x):0>2}')
    for index, row in df_dev.iterrows():
        wbs = row.get('RN').split('.')
        df.loc[index, 'domain']   = f'{int(wbs[0]):0>2}'
        df.loc[index, 'process']  = '.'.join(wbs[0:2])
        df.loc[index, 'rule']     = wbs[2]

    df[index, 'domain_id']  = df.domain.map(domain_map.get('domain_id'))
    df[index, 'process_id'] = df.process.map(process_map.get('process_id'))

    return df

def insert_project_header():
    df_dl     = pd.read_excel(f5, sheet_name='dl')
    m4n_mm    = pd.read_excel(f5, sheet_name='m4n-mm')
    df_mrs    = pd.read_excel(f6, sheet_name='Task_Table1')
    df_header = pd.read_excel(fr, sheet_name='header')
    df_header['row_type'] = 'header'
    df_dl = df_dl.fillna('')

    mrs_uids = df_header['source'].to_list()
    mrs_uids = [int(x) for x in mrs_uids if np.isnan(x) == False]
    mask = df_mrs['Unique_ID'].isin(mrs_uids)
    df_mrs = df_mrs[mask]
    df_mrs['Start']  = pd.to_datetime(df_mrs['Start_Date'], format='%B %d, %Y %H:%M %p').dt.strftime('%d/%m/%Y')
    df_mrs['Finish'] = pd.to_datetime(df_mrs['Finish_Date'], format='%B %d, %Y %H:%M %p').dt.strftime('%d/%m/%Y')
    df_mrs = df_mrs[['Unique_ID', 'Start', 'Finish', 'Percent_Complete']]
    df_mrs.set_index('Unique_ID', inplace=True)
    mrs_map = df_mrs.to_dict()
    df_header['due_date'] = df_header.source.map(mrs_map.get('Start'))
    df_header[pc] = df_header.source.map(mrs_map.get('Percent_Complete'))

    i = 0
    for index, row in df_dl.iterrows():
        i = i + 1
        rn = '00.50.' + f'{i:0>2}'
        df_dl.loc[index, 'RN']        = rn
        df_dl.loc[index, 'Name']      = df_dl.loc[index, 'p_name'] + ' -- ' + df_dl.loc[index, 'task']
        df_dl.loc[index, 'Deadline']  = df_dl.loc[index, 'dl_date'].strftime('%d/%m/%Y')
        df_dl.loc[index, 'source']    = df_dl.loc[index, 'WBS']
        df_dl.loc[index, 'alias']     = df_dl.loc[index, 'WBS']
        df_dl.loc[index, OL]          = 3
        df_dl.loc[index, 'due_date']  = df_dl.loc[index, 'dl_date']
        df_dl.loc[index, 'isMS']      = 1
        df_dl.loc[index, 'row_type']  = 'm4n-dl'

    """ mark M4N Major Milestones and manage them separately """
    i = 0
    for index, row in m4n_mm.iterrows():
        i += 1
        rn = '00.40.' + f'{i:0>2}'

        m4n_mm.loc[index, 'RN']       = rn
        m4n_mm.loc[index, OL]         = 3
        m4n_mm.loc[index, 'alias']    = row.task
        m4n_mm.loc[index, 'Name']     = row.task
        m4n_mm.loc[index, 'row_type'] = 'm4n-ms'
        m4n_mm.loc[index, 'due_date'] = row.finish
        m4n_mm.loc[index, 'Deadline'] = row.finish
        m4n_mm.loc[index, 'isMS']     = 1
        m4n_mm.loc[index, 'source']   = row.WBS

    m4n_mm['Task Mode'] = 'Manually Scheduled'
    m4n_mm = m4n_mm[['Outline Level', 'RN', 'Name', 'row_type', 'Deadline', 'due_date', 'isMS', 'source', 'alias']]
    df_dl  = df_dl[['Outline Level', 'RN', 'Name', 'row_type', 'Deadline', 'due_date', 'isMS', 'source', 'alias']]
    df_header = pd.concat([df_header, df_dl, m4n_mm], ignore_index=True)

    return df_header

def insert_sub_groups(df=pd.DataFrame()):  # insert sub groups
    parent_list = df['parent'].unique()
    parent_list = [x for x in parent_list if str(x) != 'nan']
    keys = ('Name', 'RN', 'domain_id', 'process_id', 'row_type', 'waiting')
    rows = []
    df_x = pd.DataFrame()
    for p in parent_list:
        row = df[df['RN'] == p]
        p_row = dict(
            zip(
                keys,
                [
                    'Sub Tasks',
                    row['RN'].values[0] + '.01',
                    row.domain_id.values[0],
                    row.process_id.values[0],
                    'Sub Tasks',
                    row.waiting.values[0],
                ]
            )
        )
        rows.append(p_row)
        p_row = dict(
            zip(
                keys,
                [
                    'LifeCycle Tasks',
                    row['RN'].values[0] + '.02',
                    row.domain_id.values[0],
                    row.process_id.values[0],
                    'LC Tasks',
                    row.waiting.values[0],
                ]
            )
        )
        rows.append(p_row)
    return pd.DataFrame(rows)

def insert_rule_block(df=pd.DataFrame()):
    parent_list = df['parent'].unique()
    parent_list = [x for x in parent_list if str(x) != 'nan']
    x = len(parent_list)
    print('{:23}: {}'.format('parent list', len(parent_list)))

    keys = ('RN', 'Rule', 'Name', 'p_rule', 'alias', 'isMS', 'Work', 'S', 'M', 'L',
     'XL', 't_shirt_tuple', 'parent', 'row_type', 'rule', 'domain_id',
     'process_id')
    rule_data = []
    for p in parent_list:
        row = df[df['RN'] == p]
        name = row.get('Name').values[0]
        try:
            rule = row['rule'].values[0]
        except Exception as e:
            print('Oops! insert rule block', e.__class__, 'occurred')
        df.loc[row.index, 'Name'] = str(name) + ' - ' + str(rule)

        df_r = df_rl[df_rl['Rule'].isin([rule])]
        df_r = df_r.copy()

        df_r['RN'] = p + df_r['RN']
        df_r['parent'] = p
        df_r['row_type'] = 'rule'
        df_r['rule'] = rule
        df_r['domain_id'] = df[(df['RN'].isin([p]))]['domain_id'].values[0]
        df_r['process_id'] = df[(df['RN'].isin([p]))]['process_id'].values[0]

        """ this is instead of the old and very slow pd.concat """
        for index, row in df_r.iterrows():
            rule_data.append(row.to_dict())

        p_df = df[(df['row_type'].isin(['parent'])) & (df['parent'].isin([p]))]  # parent's row
        p_status, lc_category = check_status(df=p_df)
    return rule_data
    # return df_r

def create_cycle_map(topic='BP'):
    df = df_rl.copy()
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

if __name__ == '__main__':
    print(125 * '=')
    pc = '% Complete'
    OL = 'Outline Level'

    pd.options.mode.chained_assignment = 'raise'
    start_a = time.time()

    with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    (MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)
    holidays = pd.to_datetime(config['holidays'])
    weekends = (FRI, SAT)
    dt = config['history'].get('date')
    history = config['execution_params'].get('history')
    if history:
        print('History execution')
        path = config['history'].get('path')
        fn = os.path.join(path, config['history'].get('name'))
        fr = os.path.join(path, config['history'].get('pmo'))
        fo = os.path.join(path, config['history'].get('out'))
        fs = os.path.join(path, config['history'].get('skeleton'))
        f1 = os.path.join(path, config['history'].get('requirements'))
        f2 = os.path.join(path, config['history'].get('domain'))
        print(fn)
    else:
        path = config['config'].get('path')
        fn = os.path.join(path, config['config'].get('name'))
        fr = os.path.join(path, config['config'].get('pmo'))
        fo = os.path.join(path, config['config'].get('out'))
        fs = os.path.join(path, config['config'].get('skeleton'))
        f1 = os.path.join(path, config['config'].get('requirements'))
        f2 = os.path.join(path, config['config'].get('name'))
        f3 = os.path.join(path, config['config'].get('tasks'))
        f4 = os.path.join(path, config['config'].get('shortlist'))
        f5 = config['config'].get('pdr-meeting')
        f6 = os.path.join(path, config['config'].get('mrs'))

    df_domain_reference = pd.read_excel(f2, sheet_name='domain_ref')
    df_rl = pd.read_excel(fr, sheet_name='Lines')
    df_rl['t_shirt_tuple'] = df_rl[df_rl.columns[7:12]].apply(tuple, axis=1)

    df_b1, domain_map  = prepare_domain()
    df_b2, process_map = prepare_process()

    df_bp = pd.concat([df_b1, df_b2, insert_realization_requirements()], ignore_index=True, sort=False)

    # df_bp = assign_fields_from_RN(df=df_bp)
    # df_bp = pd.concat([df_bp, prepare_interface(), insert_project_header()], ignore_index=True, sort=False)
    df_bp = pd.concat([df_bp, insert_project_header()], ignore_index=True, sort=False)
    df_bp = insert_hd1_rows(df=df_bp)

    start = time.time()
    if history:
        df_bp['rule'] = df_bp['rule'].fillna(' ')
        df_bp['rule'] = df_bp['rule'].apply(lambda x: 'is' + x if dt < '27/12/2021' else x)

    ''' rules handler '''
    start_z = time.time()
    r_data = insert_rule_block(df=df_bp)
    df_rules = pd.DataFrame(r_data)
    # df_rules = insert_rule_block(df=df_bp)
    print('{:23}: {:.3f}'.format('insert rule block: ', time.time() - start_z))
    ''' end of rules handler'''

    start_z1 = time.time()
    df_sub_groups   = insert_sub_groups(df=df_bp)
    df_bp = pd.concat([df_bp, df_rules, df_sub_groups], ignore_index=True, sort=False)

    df_bp['RN'] = df_bp['RN'].astype(str)
    print('{:23}: {:.3f}'.format('sub groups', time.time() - start_z1))

    df_bp['Name'] = df_bp['Name'].apply(lambda x: re.subn(r'[\r\n]', '--', str(x))[0])
    df_bp['task_notes'] = df_bp['task_notes'].apply(lambda x: re.subn(r'[\r\n]', '--', str(x))[0])

    df_bp.sort_values(by=['RN'], inplace=True)
    df_bp.index = np.arange(1, len(df_bp) + 1)
    df_bp['row_number'] = df_bp.index

    df_bp = df_bp[config['skeleton_columns']]
    df_dv = df_bp[df_bp['lc-RN'].notnull()]
    df_dv = df_dv[['lc-RN', 'd-start', 'd-finish', 'd-pc', 'd-duration']]

    writer = pd.ExcelWriter(fs, engine='xlsxwriter')
    workbook = writer.book
    df_bp.to_excel(writer, sheet_name='skeleton')
    df_dv.to_excel(writer, sheet_name='dev')
    writer.save()
    print('{:23}: {:.3f}'.format('Total Execution time', time.time() - start_a))
    quit(1)