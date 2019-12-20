import pandas as pd
import requests
import os
import json
import datetime
import logging
import datadotworld as dw
import datapackage


class ImmigrationData:
    base_url = r'https://trac.syr.edu/phptools/immigration/court_backlog/state.php?isbacklog=1&stat=count&fy={}&charge=imm'
    data_dir = './db'

    def __init__(self, url=base_url, data_dir=data_dir, refresh: bool = True):
        self.name = self.__class__.__name__
        self.filenames = None
        self.dir = data_dir + '/imm_data'

        # initialize logfile
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        root_log_path = './log'
        log_file = '/{}_{}.log'.format(self.name, timestamp)
        if not os.path.isdir(root_log_path):
            os.mkdir(root_log_path)
        # initialize logger and std out handler
        logging.basicConfig(filename=root_log_path+log_file, level=logging.DEBUG)
        if logging.StreamHandler not in [type(handler) for handler in logging.getLogger().handlers]:
            logging.getLogger().addHandler(logging.StreamHandler())
        logging.info("Logging Started: {}".format(log_file))

        # initialize db directory
        if not os.path.isdir(self.dir):
            os.makedirs(self.dir)

        # refresh data if needed
        if refresh:
            self.filenames = self.download(url, self.dir)
        else:
            self.filenames = [os.path.join(self.dir, file)
                              for file in os.listdir(self.dir)
                              if os.path.isfile(os.path.join(self.dir, file))]

        # convert data to df
        self.df = self.data_to_df(self.filenames)
        filename = 'imm_backlog.csv'
        target = data_dir + '/{}'.format(filename)

        # output
        self.df.to_csv(target, encoding='utf8', index=False)
        self.df.to_pickle(target.replace('csv', 'pkl'))

        logging.info("Completed Ingestion at {}.".format(datetime.datetime.now()))

    @staticmethod
    def download(source_url, target_dir):
        filenames = []
        for year in range(1998, 2020):
            url = source_url.format(year)
            resp = requests.get(url).content

            # TODO: add a better parser to deal with non-breaking spaces
            #def non_breaking_despacer(): return lambda s: s.replace(u'\xa0', ' ')
            #df = pd.read_html(resp, header=0, skiprows=0, converters={'State': non_breaking_despacer})[0].drop(0)
            df = pd.read_html(resp, header=0, skiprows=0)[0].drop(0)
            df = df.rename(columns={'Pending\xa0Cases': 'Pending Cases'})
            df['year'] = year

            filename = 'imm_backlog_{}.csv'.format(year)
            target = target_dir + '/{}'.format(filename)
            df.to_csv(target, encoding='utf8', index=False)
            filenames.append(target)

        logging.info('Download Complete.')
        logging.debug(filenames)
        return filenames


    @staticmethod
    def data_to_df(data_locations, missing_values: list = None, allowed_filetypes: list = None):
        """
        Performs union on flat data at data_locations, if data is in allowed_filetypes.
        :param data_locations: str or list of data locations, ie ['./file1.csv', './file2.csv'].
        :param missing_values: list of na values to nullify, ie ["n/a", "na", "--", '*']
        :param allowed_filetypes: list of filetype extensions, ie ['xlsx', 'csv'].
        :return: pandas dataframe concatenated objects
        """
        logging.info("Only CSV supported.")
        if allowed_filetypes is None:
            allowed_filetypes = ['csv']
        if missing_values is None:
            missing_values = ["n/a", "na", "--", '*']

        dataframe_list = []

        # make iterable
        if type(data_locations) is str:
            csv_locations = [data_locations]

        for file in data_locations:
            if os.path.isfile(file):
                if file.split('.')[-1] in allowed_filetypes:
                    try:
                        df = pd.read_csv(file, error_bad_lines=False, warn_bad_lines=True, na_values=missing_values)
                        dataframe_list.append(df)
                    except pd.io.common.CParserError:
                        logging.warning("Some data rows couldn't be parsed.")
        return pd.concat(dataframe_list)


class DataDotWorld:
    def __init__(self, dataset='dhs/immigration-statistics-2000', user_id=None):
        self.name = self.__class__.__name__

        # initialize logfile
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        root_log_path = './log'
        log_file = '/{}_{}.log'.format(self.name, timestamp)
        if not os.path.isdir(root_log_path):
            os.mkdir(root_log_path)
        # initialize logger and std out handler
        logging.basicConfig(filename=root_log_path+log_file, level=logging.DEBUG)
        if logging.StreamHandler not in [type(handler) for handler in logging.getLogger().handlers]:
            logging.getLogger().addHandler(logging.StreamHandler())
        logging.info("Logging Started: {}".format(log_file))

        # init ddw api if necessary
        if user_id is not None:
            rd_tkn = self.get_token(user_id)
            self.export_token(rd_tkn)

        # import dataset
        self.data_dir = os.path.join(os.path.expanduser('~'), '.dw/cache', dataset)
        try:
            self.dataset = dw.load_dataset(dataset)
        except datapackage.exceptions.DataPackageException as e:
            logging.debug(e)
            logging.warning("Unable to parse JSON, attempting to fix encoding.")
            self.fix_encoding()
            self.dataset = dw.load_dataset(dataset)
            logging.warning("JSON Repair Completed.")

    @staticmethod
    def configure():
        os.system("dw configure")

    @staticmethod
    def export_token(token):
        """
        Linux-only method to export token.
        :param token:
        :return:
        """
        os.system("export DW_AUTH_TOKEN = {}".format(token))

    @staticmethod
    def get_token(user_id):
        with open('./db/udata.json') as json_data:
            data = json.load(json_data)[user_id]
        return [token['read_tkn'] for token in data['tokens'] if token['site'] == 'data.world'].pop()

    def fix_encoding(self, datapackage_loc=None):
        if datapackage_loc is None:
            datapackage_loc = os.path.join(self.data_dir, 'latest', 'datapackage.json')
        # decode json
        data = json.load(open(datapackage_loc, "r", encoding='utf-8'))
        with open(datapackage_loc, "w") as file_write:
            # write json back into file with standard encoding
            json.dump(data, file_write)