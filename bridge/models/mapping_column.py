from copy import deepcopy
from typing import Union

from sqlalchemy.orm import scoped_session

from ap.common.pydn.dblib.mssqlserver import MSSQLServer
from ap.common.pydn.dblib.mysql import MySQL
from ap.common.pydn.dblib.oracle import Oracle
from ap.common.pydn.dblib.postgresql import PostgreSQL
from ap.common.pydn.dblib.sqlite import SQLite3
from ap.setting_module.models import (
    CfgProcessColumn,
    MColumnGroup,
    MData,
    MDataGroup,
    MGroup,
    insert_or_update_config,
    make_session,
)
from bridge.models.cfg_process_column import CfgProcessColumn as BsCfgProcessColumn


class MappingColumn:
    _data_ids: list[int] = []  # used to create a new group only
    _mapping_category_data_dict: dict = {}
    _m_group: MGroup = None
    _m_column_groups: list[MColumnGroup] = []
    _m_data_group: MDataGroup = None

    def __init__(
        self,
        m_data_group_id,
        m_group_id: int = None,
        data_ids: list[int] = [],
        mapping_category_data_dict: dict = {},
        meta_session: scoped_session = None,
    ):
        self._m_data_group: MDataGroup = MDataGroup.get_by_id(m_data_group_id, session=meta_session)
        if m_group_id:
            self._m_group: MGroup = MGroup.get_by_id(m_group_id, session=meta_session)
            self._m_column_groups = self._m_group.column_groups

        if data_ids:
            self._data_ids = deepcopy(data_ids)
            if not self._m_group:
                self._m_column_groups = MColumnGroup.get_by_data_ids(self._data_ids, session=meta_session)
                if self._m_column_groups:
                    self._m_group = self._m_column_groups[0].group

        self.mapping_category_data_dict = deepcopy(mapping_category_data_dict)

    def get_all_data_ids(self) -> list[int]:
        if self._m_column_groups:
            return [x.data_id for x in self._m_column_groups]
        return []

    def update_cfg_process_column(
        self,
        db_instance: Union[SQLite3, PostgreSQL, Oracle, MySQL, MSSQLServer, scoped_session],
    ):
        update_data_ids = self.get_all_data_ids()
        if not update_data_ids:
            return  # do nothing due to group have no column or not exist group

        dic_update_values = {
            BsCfgProcessColumn.Columns.name.name: self._m_data_group.data_name_jp,
            BsCfgProcessColumn.Columns.english_name.name: self._m_data_group.data_name_en,
        }
        BsCfgProcessColumn.bulk_update_by_ids(db_instance, update_data_ids, dic_update_values)

    def gen_m_group(self, meta_session: scoped_session):
        if self._m_group is None:  # In case create new group
            new_m_group = MGroup()
            new_m_group.data_group_id = self._m_data_group.id
            new_m_group = insert_or_update_config(meta_session, new_m_group)
            meta_session.flush()
            self._m_group = new_m_group
            self._add_column(self._data_ids, meta_session)

        else:  # In case update exist group
            self._m_group = MGroup.get_by_id(self._m_group.id, meta_session)
            self._m_group.data_group_id = self._m_data_group.id
            insert_or_update_config(meta_session, self._m_group)

            exist_data_ids = [x.data_id for x in self._m_group.column_groups]
            add_data_ids = [data_id for data_id in self._data_ids if data_id not in exist_data_ids]
            remove_data_ids = [data_id for data_id in exist_data_ids if data_id not in self._data_ids]

            self._remove_column(remove_data_ids, meta_session)
            self._add_column(add_data_ids, meta_session)

        # Create mapping value
        from bridge.models.mapping_category_data import (
            gen_factor_n_group_id,
            ungroup_category_values,
        )

        ungroup_category_values(meta_session, self._m_group.id, self.mapping_category_data_dict)
        gen_factor_n_group_id(meta_session, self._m_group.id, self.mapping_category_data_dict)

    def _add_column(self, data_ids, meta_session: scoped_session):
        for data_id in data_ids:
            new_m_column_group = MColumnGroup()
            new_m_column_group.group_id = self._m_group.id
            new_m_column_group.data_id = data_id
            new_m_column_group = insert_or_update_config(meta_session, new_m_column_group)
            meta_session.flush()
            self._m_column_groups.append(new_m_column_group)

    def _remove_column(self, data_ids, meta_session: scoped_session):
        if not data_ids:
            return

        from bridge.models.mapping_category_data import ungroup_category_values

        ungroup_category_values(meta_session, self._m_group.id, self.mapping_category_data_dict)

        remove_m_column_groups = list(filter(lambda x: x.data_id in data_ids, self._m_column_groups))
        for remove_group in remove_m_column_groups:
            self._m_column_groups.remove(remove_group)
        MColumnGroup.delete_by_data_ids(data_ids, meta_session)

        # Revert origin column names on cfg_process_column
        self._revert_original_column_name(data_ids, meta_session)

    def delete_column_group(self, meta_session: scoped_session):
        from bridge.models.mapping_category_data import ungroup_category_values

        ungroup_category_values(meta_session, self._m_group.id, self.mapping_category_data_dict)

        MGroup.delete(self._m_group.id, meta_session)
        data_ids = [x.data_id for x in self._m_column_groups]
        self._revert_original_column_name(data_ids, meta_session)
        self._m_group = None
        self._m_column_groups = []
        self._data_ids = []
        self._m_data_group = None

        # remove exist mapping value
        _mapping_category_data_dict: dict = {}
        _m_group: MGroup = None
        _m_column_groups: list[MColumnGroup] = []
        _m_data_group: MDataGroup

    @classmethod
    def _revert_original_column_name(cls, data_ids, meta_session: scoped_session):
        # Revert origin column names on cfg_process_column
        for data_id in data_ids:
            m_data: MData = MData.get_by_id(data_id, meta_session)
            m_data_group: MDataGroup = MDataGroup.get_by_id(m_data.data_group_id, meta_session)
            cfg_process_column: CfgProcessColumn = CfgProcessColumn.get_by_id(data_id, meta_session)
            cfg_process_column.english_name = cfg_process_column.column_name
            cfg_process_column.name = m_data_group.data_name_jp


def gen_default_m_group(data_id):  # cho column category k dc mapping column
    # check exist in m_column_group
    m_column_group: MColumnGroup = MColumnGroup.get_by_data_ids([data_id])
    if m_column_group:
        return m_column_group[0].group_id
    else:
        m_data: MData = MData.get_by_id(data_id)
        data_group_id = m_data.data_group_id
        with make_session() as meta_session:
            mapping_column_obj = MappingColumn(
                m_data_group_id=data_group_id,
                data_ids=[data_id],
                meta_session=meta_session,
            )
            mapping_column_obj.gen_m_group(meta_session)
            return mapping_column_obj._m_group.id
