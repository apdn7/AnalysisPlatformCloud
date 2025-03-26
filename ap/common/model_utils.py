from ap.common.memoize import memoize


@memoize()
def get_dic_tablename_models(modelType):
    attr_table_names = ['__tablename__', '_table_name']
    db_modules = modelType
    if not isinstance(db_modules, (tuple, list)):
        db_modules = [db_modules]

    dic_tables = {}
    for db_module in db_modules:
        all_sub_classes = list(db_module.__subclasses__())
        all_sub_classes.append(db_module)
        for _class in all_sub_classes:
            for attr_name in attr_table_names:
                if hasattr(_class, attr_name):
                    table_name = getattr(_class, attr_name)
                    dic_tables[table_name] = _class
                    break

    return dic_tables
