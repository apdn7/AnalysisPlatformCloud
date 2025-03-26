from bridge.models.bridge_station import BridgeStationModel


def bulk_insert_sync_data(db_instance, table_name, cols, rows):
    if not rows:
        return False

    row = rows[0]
    if isinstance(row, dict):
        cols = list(row)
        rows = [list(dic_row.values()) for dic_row in rows]
    if isinstance(row, BridgeStationModel):
        cols = row.Columns.get_column_names()
        rows = [row.convert_to_list_of_values() for row in rows]

    # if table_name == ColumnGroup.get_table_name():
    #     dic_row = dict(zip(cols, rows[0]))
    #     dic_con = ColumnGroup.gen_pk_condition(dic_row)
    #     _, is_exist = ColumnGroup.select_records(db_instance, dic_conditions=dic_con)
    #     if is_exist:
    #         return False

    db_instance.bulk_insert(table_name, cols, rows)

    return True


# def import_partition_table(table_name, cols, rows):
#     dic_table_class = get_dic_tablename_models(DataTypeModel)
#     model_cls = dic_table_class.get(table_name)
#     if model_cls is None:
#         return False
#
#     with make_session() as session:
#         model_cls.insert_records(cols, rows, session)
#
#     return True


# def import_non_partition_table(table_name, rows):
#     dic_table_class = get_dic_tablename_models([TransactionDBModel, OthersDBModel])
#     model_cls = dic_table_class.get(table_name)
#     if model_cls is None:
#         return False
#
#     with make_session() as session:
#         model_cls.insert_records(None, rows, session)
#
#     return True


# def import_proc_link_from_bridge(db_instance, self_proc_id, target_proc_id, cols, rows):
#     if not cols or not rows:
#         return False
#
#     table_name = ProcLink.get_table_name(self_proc_id + target_proc_id)
#     db_instance.bulk_insert(table_name, cols, rows)
#     updated_at_idx = cols.index('updated_at')
#     max_updated_at = max(rows, key=lambda _cols: _cols[updated_at_idx])[updated_at_idx]
#
#     # save the latest time to constant
#     with make_session() as session:
#         CfgConstant.create_or_update_by_type(session, const_type=CfgConstantType.SYNC_PROC_LINK.name,
#                                              const_name=ProcLink.get_table_name(), const_value=max_updated_at)
#     return True


# def import_proc_link_count_from_bridge(db_instance, cols, rows):
#     table_name = ProcLinkCount.get_table_name()
#     db_instance.bulk_insert(table_name, cols, rows)
#
#     return True
