from __future__ import annotations

import os
from typing import Any, Iterator, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from ap.api.setting_module.services.show_latest_record import gen_dummy_header
from ap.common.common_utils import (
    add_months,
    calculator_month_ago,
    check_exist,
    convert_time,
)
from ap.common.constants import (
    COLUMN_CONVERSION,
    CSV_HORIZONTAL_ROW_INDEX_COL,
    CSV_INDEX_COL,
    DATETIME_DUMMY,
    DEFAULT_NONE_VALUE,
    DF_CHUNK_SIZE,
    IMPORT_FUTURE_MONTH_AGO,
    DataGroupType,
    DataType,
    JobType,
    MasterDBType,
    TransactionForPurpose,
)
from ap.common.logger import log_execution_time, logger
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.services.csv_content import get_number_of_reading_lines, read_csv_with_transpose
from ap.common.services.csv_header_wrapr import add_suffix_if_duplicated
from ap.setting_module.models import (
    CfgConstant,
    FactoryImport,
    MappingFactoryMachine,
    MappingPart,
    MappingProcessData,
)
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.t_csv_management import CsvManagement
from bridge.services.data_import import NA_VALUES
from bridge.services.etl_services.etl_service import ETLService

PREDICT_MEMORY_USAGE_BUFFER = 0.05
MAX_MEMORY_USAGE_FOR_READ_FILE = 50_000_000  # 50MB
CSV_CHUNK_RECORD = 1_000_000
HORIZON_HAVE_MASTER_CHUNK_RECORD = 20_000
SCAN_MASTER_NO_AUTOINCREMENT_COL = 10_000


class EtlCsvService(ETLService):
    def __init__(self, cfg_data_table, root_directory=None, db_instance: PostgreSQL = None):
        super().__init__(cfg_data_table, db_instance=db_instance)
        if root_directory:
            self.root_directory = root_directory
            return

        if cfg_data_table.detail_master_type == MasterDBType.V2_HISTORY.name:
            self.root_directory = self.cfg_data_table.data_source.csv_detail.second_directory
        else:
            self.root_directory = self.cfg_data_table.data_source.csv_detail.directory

        if self.cfg_data_table.data_source.csv_detail.is_file_path:
            self.root_directory = os.path.dirname(os.path.abspath(self.root_directory))

    def _get_scan_targets(self, db_instance, scan_status, is_horizon_data=None):
        _, rows = CsvManagement.get_scan_master_target_files(db_instance, self.cfg_data_table.id, scan_status)

        dic_cloned_files = {}
        scan_master_records = []
        for dic_row in rows:
            target_rec = CsvManagement(dic_row)
            # full_file_path = self.rename_to_zip_file(target_rec)
            full_file_path = f'{self.root_directory}{target_rec.file_name}'
            dic_cloned_files[target_rec.id] = full_file_path
            scan_master_records.append(target_rec)

            # get 1 file
            if is_horizon_data:
                break

        return dic_cloned_files, scan_master_records

    def get_scan_master_target_files(self, db_instance):
        return self._get_scan_targets(db_instance, False)

    def get_scan_data_type_target_files(self, db_instance, is_horizon_data=None):
        # if scan_master save scan_status == True, its must be True here
        return self._get_scan_targets(db_instance, self.is_user_approved_master, is_horizon_data)

    def get_import_target_files(self, db_instance, job_type: JobType):
        """Get file to be imported
        - past pull:
            Get all files until feature month
        - future pull:
            Get all files since feature month
        :param db_instance:
        :param job_type:
        :return: files to be imported
        """
        is_has_auto_increment_col = self.cfg_data_table.is_has_auto_increment_col()
        if not is_has_auto_increment_col:
            _, rows = CsvManagement.get_import_target_files(db_instance, self.cfg_data_table.id)
        else:
            feature_month_ago = self.get_import_feature_month(db_instance)
            if feature_month_ago is None:
                return {}, []

            start_dt = add_months(months=-feature_month_ago, is_format_yymm=True)
            if job_type is JobType.PULL_PAST_CSV_DATA:
                _, rows = CsvManagement.get_import_target_files(db_instance, self.cfg_data_table.id, to_month=start_dt)
                rows = rows[:10]
            elif job_type is JobType.PULL_FOR_AUTO_LINK:
                _, rows = CsvManagement.get_import_target_files(db_instance, self.cfg_data_table.id)
                from bridge.services.pull_for_auto_link import PullForAutoLink

                # Get latest 3 files
                rows = rows[-PullForAutoLink.LIMIT_LATEST_FILE_COUNT :]
            else:
                _, rows = CsvManagement.get_import_target_files(
                    db_instance,
                    self.cfg_data_table.id,
                    from_month=start_dt,
                )

        if not rows:
            return {}, []

        dic_cloned_files = {}
        scan_target_records = []
        # dic_success_file, dic_error_file = get_last_csv_import_info(self.cfg_data_table.id)
        for dic_row in rows:
            target_rec = CsvManagement(dic_row)
            # filter target files
            # TODO: remove this logic and remove t_csv_import table ( manage in table CsvManagement)
            # TODO: file_name is base or zip file ?
            # if check_valid_import_target_file(target_rec.file_name, dic_success_file, dic_error_file):
            # full_file_path = self.rename_to_zip_file(target_rec)
            full_file_path = f'{self.root_directory}{target_rec.file_name}'
            dic_cloned_files[target_rec.id] = full_file_path
            scan_target_records.append(target_rec)

        return dic_cloned_files, scan_target_records

    def get_import_feature_month(self, db_instance) -> int | None:
        """Get months feature to be imported
        If we haven't pulled before:
                     (IMPORT_FUTURE_MONTH_AGO)        now
             -------------------|----------------------|------------>
        - case 1:  min --- max => months_ago = now - min
        - case 2:         min ------ max => months_ago = IMPORT_FUTURE_MONTH_AGO
        - case 3:                    min ------ max => months_ago = IMPORT_FUTURE_MONTH_AGO
        - case 4:                                min ------ max => months_ago = IMPORT_FUTURE_MONTH_AGO
        - case 5:                                          min ------ max => months_ago = IMPORT_FUTURE_MONTH_AGO
        If we pulled at least once:
            months_ago = now - latest pull
        :param db_instance:
        :return: different time calculated as above
        """
        last_transaction_import = FactoryImport.get_last_import_transaction(
            self.cfg_data_table.id,
            JobType.TRANSACTION_IMPORT.name,
        )
        if last_transaction_import:
            # neu co thi return
            last_pull = CsvManagement.get_last_pull(db_instance, self.cfg_data_table.id) or {}
            date_time = last_pull.get(CsvManagement.Columns.data_time.name)
            if not date_time:
                month_ago = IMPORT_FUTURE_MONTH_AGO
            else:
                min_time = f'20{date_time}01'
                min_time = convert_time(min_time, return_string=False)
                month_ago = calculator_month_ago(min_time)
        else:
            row_min, row_max = CsvManagement.get_min_max_date_time(db_instance, self.cfg_data_table.id)
            if not row_min and not row_max:
                return None

            max_time = row_max.get(CsvManagement.Columns.data_time.name)
            max_time = f'20{max_time}01'
            max_time = convert_time(max_time, return_string=False)
            if calculator_month_ago(max_time) < IMPORT_FUTURE_MONTH_AGO:
                month_ago = IMPORT_FUTURE_MONTH_AGO
            else:
                min_time = row_min.get(CsvManagement.Columns.data_time.name)
                min_time = f'20{min_time}01'
                min_time = convert_time(min_time, return_string=False)
                month_ago = calculator_month_ago(min_time)

        return month_ago

    @log_execution_time(prefix='etl_csv_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_master_data(
        self,
        db_instance: PostgreSQL = None,
    ) -> Iterator[
        Tuple[
            dict[Type[Union[MappingProcessData, MappingPart, MappingFactoryMachine, str]], DataFrame],
            Union[int, float],
        ]
    ]:
        generator_df = self.get_transaction_data(
            for_purpose=TransactionForPurpose.FOR_SCAN_MASTER,
            db_instance=db_instance,
        )
        yield from self.split_master_data(generator_df)

    @log_execution_time(prefix='etl_csv_service')
    @BridgeStationModel.use_db_instance_generator()
    def get_data_for_data_type(self, generator_df=None, db_instance: PostgreSQL = None):
        if generator_df is None:
            generator_df = self.get_transaction_data(
                for_purpose=TransactionForPurpose.FOR_SCAN_DATA_TYPE,
                db_instance=db_instance,
            )

        for df_original, _, progress_percentage in generator_df:
            if df_original is None or len(df_original) == 0:
                continue

            ignore_cols = None
            df, dic_df_horizons = self.transform_horizon_columns_for_import(
                df_original,
                ignore_cols=ignore_cols,
            )
            yield df, dic_df_horizons, progress_percentage

    @log_execution_time()
    @BridgeStationModel.use_db_instance_generator()
    def get_transaction_data(
        self,
        for_purpose: TransactionForPurpose = None,
        job_type: JobType = None,
        db_instance: PostgreSQL = None,
    ) -> Iterator[Tuple[Optional[DataFrame], Optional[list], Union[int, float]]]:
        is_horizon_data = self.is_horizon_data()
        horizontal_data_columns = self.get_horizontal_data_columns()
        have_real_master_columns = self.have_real_master_columns()
        is_horizon_data_no_master = is_horizon_data and not have_real_master_columns
        if not is_horizon_data_no_master and self.cfg_data_source.master_type == MasterDBType.OTHERS.name:
            csv_chunk_record = HORIZON_HAVE_MASTER_CHUNK_RECORD
        else:
            csv_chunk_record = CSV_CHUNK_RECORD

        first_nrows = None
        is_scan_master = False
        is_pull_csv = for_purpose not in (
            TransactionForPurpose.FOR_SCAN_MASTER,
            TransactionForPurpose.FOR_SCAN_DATA_TYPE,
        )
        if for_purpose is TransactionForPurpose.FOR_SCAN_MASTER:
            if is_horizon_data_no_master:
                df = self.generate_input_scan_master_for_horizontal_columns()
                yield df, [], 100
                return
            is_scan_master = True
            # first_nrows = CSV_LIMIT_SCAN_MASTER
            dic_target_files, records = self.get_scan_master_target_files(db_instance)
            if self.cfg_data_table.is_has_auto_increment_col():
                csv_chunk_record = SCAN_MASTER_NO_AUTOINCREMENT_COL

            duplicate_columns = [
                col.column_name
                for col in self.cfg_data_table_columns
                if col.data_group_type
                not in [
                    DataGroupType.DATA_VALUE.value,
                    DataGroupType.DATA_SERIAL.value,
                    DataGroupType.DATA_TIME.value,
                    DataGroupType.HORIZONTAL_DATA.value,
                ]
            ]
        elif for_purpose is TransactionForPurpose.FOR_SCAN_DATA_TYPE:
            dic_target_files, records = self.get_scan_data_type_target_files(db_instance, is_horizon_data_no_master)
            # csv_chunk_record = SQL_LIMIT_SCAN_DATA_TYPE
            # first_nrows = LATEST_RECORDS_SQL_LIMIT if is_horizon_data_no_master else CSV_LIMIT_SCAN_MASTER
        else:
            dic_target_files, records = self.get_import_target_files(db_instance, job_type)
            if for_purpose is TransactionForPurpose.FOR_AUTO_LINK:
                from bridge.services.pull_for_auto_link import PullForAutoLink

                first_nrows = PullForAutoLink.LIMIT_RECORD_COUNT

        dic_target_files = {key: val for key, val in dic_target_files.items() if check_exist(val)}
        target_files = [(key, val) for key, val in dic_target_files.items() if check_exist(val)]

        if not target_files:
            yield pd.DataFrame(), [], 0
            return

        # chunk_count = min(chunk_count or max_files_count, max_files_count or chunk_count)
        dic_target_records = {rec.id: rec for rec in records}
        one_loop_percent = 100 / len(target_files)
        dic_use_cols = {
            col.column_name: col.data_type
            for col in self.cfg_data_table_columns
            if not (is_scan_master and col.column_name in horizontal_data_columns)
            # TODO: ignore file name properly instead of column name
            and col.column_name != DataGroupType.FileName.name
        }
        # dic_use_cols = {col.column_name: col.data_type for col in self.cfg_data_table_columns}
        use_dummy_datetime = DATETIME_DUMMY in dic_use_cols
        loop_count = 0
        progress_percent = 0
        df_multi_files = pd.DataFrame()
        file_paths = []
        csv_management_ids: list[int] = []
        while target_files:
            if is_pull_csv:
                CfgConstant.force_running_job()

            loop_count += 1
            progress_percent = round(one_loop_percent * loop_count, 2)
            csv_management_id, file_path = target_files.pop()
            file_paths.append(file_path)
            csv_management_ids.append(csv_management_id)
            data_encoding = dic_target_records[csv_management_id].data_encoding
            data_delimiter = dic_target_records[csv_management_id].data_delimiter
            file_name = dic_target_records[csv_management_id].file_name
            file_name = file_name.split('\\')[-1].split('.')[0]
            data_stream = self.standard_csv(
                dic_use_cols,
                file_path,
                data_encoding,
                data_delimiter,
                file_name=file_name,
                limit=first_nrows,
                for_purpose=for_purpose,
            )

            for df_chunk_one_file in data_stream:
                # TODO: recheck this function and gen datetime column function
                self.tracking_group_csv(df_chunk_one_file, loop_count, use_dummy_datetime)
                df_multi_files = df_multi_files.append(df_chunk_one_file, ignore_index=True)
                if is_scan_master:
                    drop_cols = [
                        col
                        for col in df_multi_files.columns
                        if col in duplicate_columns or col in [DataGroupType.DATA_NAME.name]
                    ]
                    df_multi_files = df_multi_files.drop_duplicates(subset=drop_cols)

                if len(df_multi_files) >= csv_chunk_record:
                    yield from self.preprocess_dataframe(
                        df_multi_files,
                        dic_use_cols,
                        use_dummy_datetime,
                        for_purpose,
                        file_paths,
                        progress_percent,
                    )
                    self.update_status_of_file(
                        csv_management_ids[:-1],
                        for_purpose=for_purpose,
                        db_instance=db_instance,
                    )

                    # reset status of loop's params
                    df_multi_files = pd.DataFrame()
                    file_paths = [file_path]
                    csv_management_ids = [csv_management_id]
                    if is_scan_master and not self.cfg_data_table.is_has_auto_increment_col():
                        # if have not auto_increment_col read 10000 record
                        break

        yield from self.preprocess_dataframe(
            df_multi_files,
            dic_use_cols,
            use_dummy_datetime,
            for_purpose,
            file_paths,
            progress_percent,
        )
        self.update_status_of_file(csv_management_ids, for_purpose=for_purpose, db_instance=db_instance)

    @BridgeStationModel.use_db_instance()
    def update_status_of_file(
        self,
        csv_management_ids: list[int],
        for_purpose: TransactionForPurpose = None,
        db_instance: PostgreSQL = None,
    ):
        if not csv_management_ids:
            return

        dic_update_values: dict[str, bool] = {}
        if for_purpose is TransactionForPurpose.FOR_SCAN_DATA_TYPE:
            # scan master - done
            dic_update_values = {CsvManagement.Columns.scan_status.name: True}
        elif for_purpose is None:
            # import - done
            dic_update_values = {CsvManagement.Columns.dump_status.name: True}
        elif for_purpose is TransactionForPurpose.FOR_AUTO_LINK:
            # do nothing
            return
        else:
            # do nothing
            return

        CsvManagement.bulk_update_by_ids(
            db_instance,
            ids=csv_management_ids,
            dic_update_values=dic_update_values,
        )

    @BridgeStationModel.use_db_instance()
    def set_all_scan_data_type_status_done(self, db_instance: PostgreSQL = None):
        csv_managements = CsvManagement.get_by_data_table_id(
            db_instance=db_instance,
            data_table_id=self.cfg_data_table.id,
            row_is_dict=False,
        )
        CsvManagement.bulk_update_by_ids(
            db_instance,
            ids=[csv_management.id for csv_management in csv_managements],
            dic_update_values={CsvManagement.Columns.scan_status.name: True},
        )

    def preprocess_dataframe(
        self,
        df: DataFrame,
        dic_use_cols: dict,
        use_dummy_datetime: bool,
        for_purpose: TransactionForPurpose,
        file_paths: list[str],
        progress_percent: float,
    ):
        df.reset_index(drop=True, inplace=True)
        df.replace({np.NAN: DEFAULT_NONE_VALUE}, inplace=True)

        # Add NULL for master column not select
        master_columns, master_values = self.get_dummy_master_column_value()
        self.add_dummy_master_columns(df, master_columns, master_values, dic_use_cols)

        # Add default dummy datetime column if datetime column does not exist
        if use_dummy_datetime and for_purpose is not TransactionForPurpose.FOR_SCAN_MASTER:
            df[DATETIME_DUMMY] = DEFAULT_NONE_VALUE

        yield df, file_paths, progress_percent

    @log_execution_time()
    def convert_df_horizontal_to_vertical(self, df_horizontal_data: DataFrame):
        # Convert "ワーク種別", "良否", "ロットNo", "トレイNo" to vertical
        data_name_col = None
        data_value_col = None
        unique_cols: list[str] = []
        horizontal_cols = []

        for data_table_column in self.cfg_data_table_columns:
            if data_table_column.column_name not in df_horizontal_data or data_table_column.data_group_type in [
                DataGroupType.SUB_PART_NO.value,
                DataGroupType.SUB_LOT_NO.value,
                DataGroupType.SUB_TRAY_NO.value,
                DataGroupType.SUB_SERIAL.value,
                DataGroupType.DATA_ID.value,
            ]:
                continue
            if data_table_column.data_group_type == DataGroupType.DATA_NAME.value:
                data_name_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.DATA_VALUE.value:
                data_value_col = data_table_column.column_name
            elif data_table_column.data_group_type == DataGroupType.HORIZONTAL_DATA.value:
                horizontal_cols.append(data_table_column.column_name)
            else:
                unique_cols.append(data_table_column.column_name)

        if not horizontal_cols:
            return pd.DataFrame()

        if data_name_col is None:
            data_name_col = DataGroupType.DATA_NAME.name
        if data_value_col is None:
            data_value_col = DataGroupType.DATA_VALUE.name

        dfs: list[DataFrame] = []
        for horizontal_col in horizontal_cols:
            df_vertical_data = df_horizontal_data[[*unique_cols, horizontal_col]].rename(
                columns={horizontal_col: data_value_col},
            )
            df_vertical_data[data_name_col] = horizontal_col
            dfs.append(df_vertical_data)

        return pd.concat(dfs).reset_index(drop=True)

    @log_execution_time()
    def standard_csv(
        self,
        dic_use_cols,
        file_path,
        encoding,
        delimiter,
        file_name=None,
        limit=None,
        for_purpose: TransactionForPurpose = None,
    ):
        logger.info(f'[ReadFile] {file_path}')

        metadata = {'encoding': encoding, 'sep': delimiter}
        # force data type = True will raise error float64 to int32
        read_csv_param = self.build_read_csv_params(self.cfg_data_table, metadata, force_data_type=None)

        n_rows = self.get_limit_records(limit)
        if n_rows is not None:
            read_csv_param['nrows'] = n_rows

        if self.get_skip_head():
            read_csv_param['skiprows'] = self.get_skip_head()

        params, dict_rename_columns = self.get_alternative_params(
            file_path,
            read_csv_param,
            dic_use_cols,
        )
        limit_row: int | None = params.get('nrows', None)
        chunk_size = limit_row if limit_row else DF_CHUNK_SIZE
        for data_chunk in self.read_csv_with_transpose(file_path, chunk_size=chunk_size, **params):
            if file_name:
                data_chunk[DataGroupType.FileName.name] = file_name

            if MasterDBType.is_v2_group(self.master_type):
                from bridge.services.etl_services.etl_v2_measure_service import V2MeasureService

                self: V2MeasureService
                yield self.convert_to_standard_v2(data_chunk, for_purpose=for_purpose)
            else:
                if dict_rename_columns:
                    data_chunk.rename(columns=dict_rename_columns, inplace=True)
                yield data_chunk

    def get_limit_records(self, user_limit: int | None = None) -> int | None:
        nrows = get_number_of_reading_lines(self.cfg_data_source.csv_detail.n_rows, user_limit)

        # do not use user provided limit if we need to transpose
        if self.cfg_data_source.csv_detail.is_transpose:
            return nrows

        if nrows is None:
            return user_limit

        # need to escape 1 records because of the header
        nrows = nrows - 1

        if user_limit is None:
            return nrows

        return min(nrows, user_limit)

    def read_csv_with_transpose(self, file_path: str, chunk_size, **params):
        if self.cfg_data_source.csv_detail.is_transpose:
            use_cols = params.pop('usecols', [])
            dtypes = params.pop('dtype', {})
            # cannot usecols atm
            df = read_csv_with_transpose(file_path, is_transpose=True, **params)

            # rename columns
            df_headers = df.columns
            parsed_headers = df.columns.to_series().replace({np.nan: ''}).to_list()
            _, parsed_headers, *_ = gen_dummy_header(parsed_headers, data_details=[], line_skip=self.get_skip_head())
            dropped_columns = []
            renamed_columns = {}
            for df_header, parsed_header in zip(df_headers, parsed_headers):
                if parsed_header not in use_cols:
                    dropped_columns.append(df_header)
                else:
                    renamed_columns[df_header] = parsed_header
            df = df.drop(dropped_columns, axis=1)
            df = df.rename(columns=renamed_columns)

            # cast datatype
            for col_name, dtype in dtypes.items():
                if col_name in df:
                    df[col_name] = df[col_name].astype(dtype)

            yield df
        else:
            yield from pd.read_csv(file_path, chunksize=chunk_size, **params)

    def get_alternative_params(
        self,
        file_path: str,
        read_csv_param: dict[str, Any],
        dic_use_cols: dict,
    ) -> [dict[str, Any], dict[str, Any]]:
        """
        Read csv Params dùng cho những file có column bị sai
        """
        # Support load file cho cùng một thư mục nhưng column name khác tên (ví dụ トレイ/トレー  番号/No)
        # Dùng pandas chỉ để đọc header. Vì file zip nên dùng pandas cho tiện

        if self.has_dummy_header():
            dict_dummy_master_columns, _ = self.get_dummy_master_column_value()
            # exclude dummy master columns
            header = sorted(set(dic_use_cols.keys()) - set(dict_dummy_master_columns.keys()))
        else:
            header = EtlCsvService.get_header_row(
                file_path,
                sep=read_csv_param.get('sep', None),
                encoding=read_csv_param.get('encoding', None),
                skip_head=self.get_skip_head(),
                n_rows=self.get_limit_records(),
                is_transpose=self.cfg_data_source.csv_detail.is_transpose,
            )

        dtype = {}
        usecols = []

        params = read_csv_param.copy()
        dic_dtype = params.get('dtype', {})
        for col in header:
            if col in dic_use_cols:
                # normal. nếu header cũng là dict use thì append
                usecols.append(col)
                if dic_dtype:
                    dtype[col] = dic_dtype[col]
            else:
                # alternate_col_name -> tên column đúng dùng trong Bridge Station
                alternate_col_name = COLUMN_CONVERSION.get(col, None)
                if not alternate_col_name:
                    continue

                # nếu tên column đúng không trong dict use thì continue
                if alternate_col_name not in dic_use_cols:
                    continue

                if alternate_col_name in dic_dtype:
                    dtype[col] = dic_dtype[alternate_col_name]

                usecols.append(col)  # "rename from"

        dtype.update(
            {key: pd.StringDtype() for key in dic_use_cols.keys()},
        )
        csv_cols, with_dupl_cols, pd_names = add_suffix_if_duplicated(header)
        dict_rename_columns = {}
        for org_col_name, is_add_suffix, pd_name in zip(csv_cols, with_dupl_cols, pd_names):
            if is_add_suffix:
                # [a, a_01, a_02] -> [a, a.1, a.2]
                matched = org_col_name.split('_')
                if len(matched) > 1 and matched[-1].isdigit():
                    dict_rename_columns[pd_name] = org_col_name
                    usecols.append(pd_name)
        params.update(
            {
                'usecols': usecols,
                'dtype': dtype,
            },
        )

        if self.has_dummy_header():
            params.pop('usecols')
            params.update({'names': header, 'header': None})

        return params, dict_rename_columns

    @staticmethod
    def get_header_row(
        file_path,
        sep: str | None = None,
        encoding: str | None = None,
        skip_head: int = 0,
        n_rows: int | None = None,
        is_transpose: bool = False,
    ) -> list[str]:
        df = read_csv_with_transpose(
            file_path,
            is_transpose,
            sep=sep,
            encoding=encoding,
            nrows=n_rows if is_transpose else 1,
            header=None,
            skiprows=skip_head,
        )
        headers = df.columns.to_series().replace({np.nan: ''}).to_list() if is_transpose else df.iloc[0].tolist()
        _, header_names, *_ = gen_dummy_header(headers, data_details=[], line_skip=skip_head)
        return header_names

    @staticmethod
    def from_data_type_to_read_csv_dtype(data_type):
        if data_type == DataType.TEXT.name:
            return 'string'
        if data_type == DataType.INTEGER.name:
            return 'Int32'
        if data_type == DataType.REAL.name:
            return 'Float64'  # Dùng float64 chứ không dùng Float32, để giảm effect của dấu chấm động
        if data_type == DataType.DATETIME.name:
            return 'string'

    @staticmethod
    def build_read_csv_params(cfg_data_table, metadata, force_data_type=None):
        # data_source_csv = cfg_data_table.data_source.csv_detail  # type: CfgDataSourceCSV
        # csv delimiter from config
        # csv_delimiter = get_csv_delimiter(data_source_csv.delimiter)
        dic_use_cols = {col.column_name: col.data_type for col in cfg_data_table.get_sorted_columns()}

        # skip_row = 0
        # data_first_row = data_source_csv.skip_head + 1
        # head_skips = list(range(0, data_source_csv.skip_head))
        # read csv file
        read_csv_param = {**metadata}
        if force_data_type:
            dic_dtype = {
                'dtype': {
                    col: EtlCsvService.from_data_type_to_read_csv_dtype(data_type)
                    for col, data_type in dic_use_cols.items()
                },
            }
            read_csv_param.update(dic_dtype)

        # read_csv_param.update(dict(skiprows=head_skips + list(range(data_first_row, skip_row + data_first_row))))
        read_csv_param.update(
            {
                'usecols': list(dic_use_cols),
                'skipinitialspace': True,
                'na_values': NA_VALUES,
                'error_bad_lines': False,
                'skip_blank_lines': True,
            },
        )
        # get_metadata.metadata = None
        return read_csv_param

    @staticmethod
    def tracking_group_csv(df: DataFrame, idx, is_use_dummy_datetime=False):
        if is_use_dummy_datetime and CSV_INDEX_COL not in df.columns:
            df[CSV_INDEX_COL] = idx
            df[CSV_HORIZONTAL_ROW_INDEX_COL] = range(len(df))

    def has_dummy_header(self):
        return self.cfg_data_table.data_source.csv_detail.dummy_header

    def get_skip_head(self):
        return self.cfg_data_table.data_source.csv_detail.skip_head
