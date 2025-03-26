import cutlet
import pandas as pd
from pandas import DataFrame

from ap import log_execution_time

# demo combine eFA + V2 measure
from ap.common.common_utils import (
    detect_encoding,
    detect_file_delimiter,
    get_dummy_data_config_path,
    open_with_zip,
)
from ap.common.memoize import memoize

WORD_PREDICTION_FILE_NAME = 'word_prediction.tsv'
WORD_PREDICTION_FILE_PATH = get_dummy_data_config_path(filename=WORD_PREDICTION_FILE_NAME)

# DEMO_FILE = './word_prediction_en.csv'     # demo import/display english
# DEMO_FILE = './word_prediction_vi.csv'     # demo import/display other language


@memoize(duration=15 * 60)
@log_execution_time()
def get_word_prediction(column_name=None):  # temp for demo
    """
    Use DEMO FILE to change similar column name to 1 common name
    :param column_name:
    :return:
    """

    with open_with_zip(WORD_PREDICTION_FILE_PATH, 'rb') as file_stream:
        encoding = detect_encoding(file_stream)
        file_delimiter = detect_file_delimiter(file_stream, '\t', encoding)
    df = pd.read_csv(
        WORD_PREDICTION_FILE_PATH,
        skipinitialspace=True,
        error_bad_lines=False,
        encoding=encoding,
        skip_blank_lines=True,
        sep=file_delimiter,
    )
    if column_name:
        df = df[df['column'] == column_name]
        df.reset_index(drop=True, inplace=True)
    return df.convert_dtypes()


_cutlet = cutlet.Cutlet()


def to_en__demo__(df, model_cls):
    df_word_prediction = DataFrame(columns=['column', 'left', 'right'])
    if model_cls.get_jp_name_column() in df.columns:
        df_word_prediction['left'] = df[model_cls.get_jp_name_column()]
        df_word_prediction['column'] = model_cls.get_table_name()
        df_word_prediction['right'] = df_word_prediction['left'].apply(
            lambda jp_word: f'{_cutlet.romaji(jp_word)} (this is a english sentence)',
        )
    df_word_prediction.to_csv('./word_prediction_en.csv', sep='\t', header=False, index=False, mode='a')


def to_vi__demo__(df, model_cls):
    df_word_prediction = DataFrame(columns=['column', 'left', 'right'])
    if model_cls.get_jp_name_column() in df.columns:
        df_word_prediction['left'] = df[model_cls.get_jp_name_column()]
        df_word_prediction['column'] = model_cls.get_table_name()
        df_word_prediction['right'] = df_word_prediction['left'].apply(
            lambda jp_word: f'{_cutlet.romaji(jp_word)} (đây là một câu tiếng việt)',
        )
    df_word_prediction.to_csv('./word_prediction_vi.csv', sep='\t', header=False, index=False, mode='a')
