import pandas as pd

from ap.api.setting_module.services.process_data_count import (
    save_proc_data_count_multiple_dfs,
)
from ap.common.constants import AnnounceEvent
from ap.common.services.sse import background_announcer
from ap.setting_module.services.backup_and_restore.backup_file_manager import BackupKey, BackupKeysManager
from ap.setting_module.services.backup_and_restore.duplicated_check import (
    get_df_insert_and_duplicated_ids,
    remove_unused_columns_and_add_missing_columns,
)
from bridge.models.bridge_station import BridgeStationModel
from bridge.models.transaction_model import TransactionData


def backup_db_data(process_id: int, start_time: str, end_time: str):
    backup_keys_manager = BackupKeysManager(process_id=process_id, start_time=start_time, end_time=end_time)

    # TODO: get min max date in database before running this
    backup_keys = backup_keys_manager.get_backup_keys_by_day()
    total_backup_keys = len(backup_keys)

    if total_backup_keys == 0:
        # nothing to do
        yield 100

    # create transaction outside to avoid looping, we only modify `t_table` so this is fine
    transaction_data = TransactionData(process_id)
    for i, backup_key in enumerate(backup_keys):
        backup_db_data_from_key(transaction_data, backup_keys_manager, backup_key)
        yield (i + 1) * 100 / total_backup_keys

    background_announcer.announce(True, AnnounceEvent.BACKUP_DATA_FINISHED.name)


def backup_db_data_from_key(
    transaction_data: TransactionData,
    backup_keys_manager: BackupKeysManager,
    backup_key: BackupKey,
):
    with BridgeStationModel.get_db_proxy() as db_instance:
        get_date_col = transaction_data.getdate_column.bridge_column_name
        df_from_db: pd.DataFrame = transaction_data.get_transaction_by_time_range(
            db_instance,
            backup_keys_manager.get_start_time(backup_key),
            backup_keys_manager.get_end_time(backup_key),
        )
        if df_from_db.empty:
            return

        # remove data in transaction table
        remove_ids = df_from_db[transaction_data.id_col_name].to_list()
        transaction_data.remove_by_ids(db_instance, remove_ids)

        df_file = backup_key.read_file()
        # overwrite columns from database to file
        df_file = remove_unused_columns_and_add_missing_columns(df_file, df_from_db.columns)

        df_insert, duplicated_ids = get_df_insert_and_duplicated_ids(
            db_instance,
            transaction_data,
            df_insert=df_from_db,
            df_old=df_file,
        )

        is_duplicated_in_df_file = (
            df_file[transaction_data.id_col_name].isin(duplicated_ids) if not df_file.empty else pd.Series()
        )

        df_file_overwrite = pd.concat(
            [
                # remove duplicated ids in `df_file`
                df_file[~is_duplicated_in_df_file],
                df_insert,
            ],
        )

        save_proc_data_count_multiple_dfs(
            db_instance,
            proc_id=backup_key.process_id,
            get_date_col=get_date_col,
            dfs_pop_from_db=df_from_db,
            dfs_pop_from_file=df_file[is_duplicated_in_df_file],
            dfs_push_to_file=df_insert,
        )

        backup_key.write_file(df_file_overwrite)
