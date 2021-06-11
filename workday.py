import pandas as pd
import yaml

pd.options.mode.chained_assignment = 'raise'

(MON, TUE, WED, THU, FRI, SAT, SUN) = range(7)

default_weekends = (FRI, SAT)

def cmp(a, b):
    return (a > b) - (a < b)

def in_between(a, b, x):
    return a <= x <= b or b <= x <= a


def workday(start_date, days=0, holidays=[], weekends=default_weekends):
    if days == 0:
        return start_date
    if days > 0 and start_date.weekday() in weekends:
        while start_date.weekday() in weekends:
            start_date -= pd.to_timedelta(1, unit='d')
    elif days < 0:
        while start_date.weekday() in weekends:
            start_date += pd.to_timedelta(1, unit='d')

    full_weeks, extra_days = divmod(days, 7 - len(weekends))
    new_date = start_date + pd.to_timedelta(full_weeks, unit='w')
    for i in range(int(extra_days)):
        new_date += pd.to_timedelta(1, unit='d')
        while new_date.weekday() in weekends:
            new_date += pd.to_timedelta(1, unit='d')
    # to account for days=0 case
    while new_date.weekday() in weekends:
        new_date += pd.to_timedelta(1, unit='d')

    if len(holidays) > 0:
        delta = pd.to_timedelta(1 * cmp(days, 0), unit='d')
        # skip holidays that fall on weekends
        holidays = [x for x in holidays if x.weekday() not in weekends]
        holidays = [x for x in holidays if x != start_date]
        for d in sorted(holidays, reverse=(days < 0)):
            # if d in between start and current push it out one working day
            if in_between(start_date, new_date, d):
                new_date += delta
                while new_date.weekday() in weekends:
                    new_date += delta

    return new_date
