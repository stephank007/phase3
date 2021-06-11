import yaml
import codecs
import os
import time
import subprocess
import asyncio as aio

win_proj = 'WINPROJ.EXE'
win_excel = 'EXCEL.EXE'
python = 'python'

with codecs.open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

office_path = config['config'].get('ms_path')
data_path   = config['config'].get('path')
bin_path    = config['config'].get('bin_path')
sc_path     = config['config'].get('sc_path')

p3_robot = config['config'].get('p3-robot')
p3_skltn = config['config'].get('p3-skeleton')
excel    = os.path.join(office_path, 'EXCEL.EXE')
project  = os.path.join(office_path, 'WINPROJ.EXE')

letters   = os.path.join(data_path, config['config'].get('letters'))
reconcile = os.path.join(data_path, config['config'].get('reconcile'))
moria     = os.path.join(data_path, config['config'].get('moria'))
out_file  = os.path.join(data_path, config['config'].get('out'))
name      = os.path.join(data_path, config['config'].get('name'))
sig_file  = os.path.join(sc_path,   config['config'].get('sig_file'))

os.chdir(bin_path)

def signature():
    print('update signature color file')
    os.startfile(sig_file)
    input('press Enter key to continue... ')
    os.startfile(moria)

def reconcile_f(domain_list):
    for d in domain_list:
        p = subprocess.Popen([python, 'reconcile.py', '-b' + d], cwd=bin_path)
        print('reconcile with: {}'.format(d))
        (output, err) = p.communicate()
        p_status = p.wait()
        print('reconcile ended: {} {}'.format(output, p_status))

        os.startfile(reconcile)
        os.startfile(name)
        input('press Enter key to continue... ')
    return True


def run_skeleton():
    """ run skeleton process """
    p = subprocess.Popen([python, p3_skltn], cwd=bin_path)
    (output, err) = p.communicate()
    p_status = p.wait()
    print('p3 skeleton ended: {} {}'.format(output, p_status))
    return True

def run_rpa():
    """ run p3 management machine """
    p = subprocess.Popen([python, p3_robot], cwd=bin_path)
    (output, err) = p.communicate()
    p_status = p.wait()
    print('p3 robot ended: {} {}'.format(output, p_status))
    return True


def run_project():
    """ update MS Project """
    os.startfile(os.path.join(data_path, '_out\p3-out.mpp'))
    return True


def run_letters():
    """ Produce Letters """
    p = subprocess.Popen([python, 'letters.py'], cwd=bin_path)
    (output, err) = p.communicate()
    p_status = p.wait()
    print('letters produced: {} {}'.format(output, p_status))


def run_gantt():
    p = subprocess.Popen([python, 'gantt_detail.py'], cwd=bin_path)
    (output, err) = p.communicate()
    p_status = p.wait()
    print('gantt detail produced: {} {}'.format(output, p_status))


def run_plots():
    subprocess.run([python, 'approval_cycle_1.py', '-b' + 'SM'], cwd=bin_path)
    subprocess.run([python, 'approval_cycle_1.py', '-b' + 'SCM'], cwd=bin_path)


def run_reconciliation():
    reconcile_f(['SM', 'SCM'])


process_dict = {
    0: run_skeleton,
    1: signature,
    2: run_reconciliation,
    3: run_rpa,
    4: run_project,
    5: run_letters,
    6: run_gantt,
    7: run_plots
}

process_description_dict = {
    0: 'skeleton',
    1: 'signature',
    2: 'reconcile',
    3: 'run RPA',
    4: 'run MS Project',
    5: 'produced letters',
    6: 'produce gantt',
    7: 'produce plots'
}


def print_dict():
    print('\n')
    for key, value in process_description_dict.items():
        print('step {}: {}'.format(key, value))


step = 0
try:
    while int(step) in range(0, 7):
        print_dict()
        step = input('\nplease choose your step(0-6): ')
        print('your pick is-->: {}'.format(process_description_dict.get(int(step))))
        if int(step) < 7:
            v = process_dict.get(int(step))()
        else:
            print('exiting...')
            quit(0)
finally:
    print('exit...')
