import pandas as pd
import requests
import os


class ImmigrationData:
    base_url = 'https://trac.syr.edu/phptools/immigration/court_backlog/state.php?isbacklog=1&stat=count&fy={}&charge=imm'
    data_root = os.getcwd() + '\\db'
    imm_dir = '\\imm_data'

    if not os.path.isdir(data_root+imm_dir):
        os.makedirs(data_root+imm_dir)

    for year in range(1998, 2020):
        url = base_url.format(year)
        resp = requests.get(url).content

        # TODO: add a better parser to deal with non-breaking spaces
        #def non_breaking_despacer(): return lambda s: s.replace(u'\xa0', ' ')
        #df = pd.read_html(resp, header=0, skiprows=0, converters={'State': non_breaking_despacer})[0].drop(0)
        df = pd.read_html(resp, header=0, skiprows=0)[0].drop(0)
        df = df.rename(columns={'Pending\xa0Cases': 'Pending Cases'})
        df['year'] = year

        filename = 'imm_backlog_{}.csv'.format(year)
        df.to_csv(data_root + imm_dir + '\\{}'.format(filename), encoding='utf8')

    print('done')

class DataDotWorld:
    pass