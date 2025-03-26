const DataTypes = Object.freeze({
    NONE: {
        name: 'NONE',
        value: 0,
        label: '---',
        short: 'NONE',
        exp: 'NONE',
        org_type: 'NONE',
        operator: [''],
    },
    INTEGER: {
        name: 'INTEGER',
        value: 1,
        bs_value: 'INTEGER',
        label: $('#i18nInteger').text() || '整数',
        i18nLabelID: 'i18nIntegerOthers',
        i18nAllLabel: 'i18nAllInt',
        short: 'Int',
        exp: 'i18nInteger',
        org_type: 'INTEGER',
        operator: ['', '+', '-', '*', '/'],
        selectionBoxDisplay: 'Int',
    },
    INTEGER_CAT: {
        name: 'INTEGER_CAT',
        value: 1,
        label: $('#i18nIntegerCat').text() || '整数(カテゴリ)',
        i18nLabelID: 'i18nIntegerCat',
        i18nAllLabel: 'i18nAllIntCatLabel',
        short: 'Int(Cat)',
        exp: 'i18nIntegerCat',
        org_type: 'INTEGER',
        operator: ['', '+', '-', '*', '/'],
        selectionBoxDisplay: 'Int(Cat)',
    },
    REAL: {
        name: 'REAL',
        value: 2,
        bs_value: 'REAL',
        label: $('#i18nFloat').text() || '実数',
        i18nLabelID: 'i18nFloat',
        i18nAllLabel: 'i18nAllReal',
        short: 'Real',
        exp: 'i18nFloatTypeExplain',
        org_type: 'REAL',
        operator: ['', '+', '-', '*', '/'],
        selectionBoxDisplay: 'Real',
    },
    STRING: {
        name: 'TEXT',
        value: 3,
        bs_value: 'TEXT',
        label: $('#i18nString').text() || '文字列',
        i18nLabelID: 'i18nString',
        i18nAllLabel: 'i18nAllStr',
        short: 'Str',
        exp: 'i18nString',
        org_type: 'TEXT',
        operator: ['', 'Valid-like'],
        selectionBoxDisplay: 'Str',
    },
    DATETIME: {
        name: 'DATETIME',
        value: 4,
        bs_value: 'DATETIME',
        label: $('#i18nDateTime').text() || '日付',
        i18nLabelID: 'i18nDateTime',
        short: 'CT',
        exp: 'i18nCTTypeExplain',
        org_type: 'DATETIME',
        operator: [''],
        selectionBoxDisplay: 'Datetime',
    },
    // DATE: {
    //     name: 'DATE',
    //     value: 4,
    //     label: $('#i18nMainDate').text() || '日付',
    //     i18nLabelID: 'i18nMainDate',
    //     short: 'Date',
    //     exp: 'i18nCTTypeExplain',
    //     org_type: 'DATE',
    //     operator: [''],
    //     selectionBoxDisplay: 'Datetime'
    // },
    // TIME: {
    //     name: 'TIME',
    //     value: 4,
    //     label: $('#i18nMainTime').text() || '日付',
    //     i18nLabelID: 'i18nMainTime',
    //     short: 'Time',
    //     exp: 'i18nCTTypeExplain',
    //     org_type: 'TIME',
    //     operator: [''],
    //     selectionBoxDisplay: 'Datetime'
    // },
    TEXT: {
        name: 'TEXT',
        value: 3,
        bs_value: 'TEXT',
        label: $('#i18nString').text() || '文字列',
        i18nLabelID: 'i18nString',
        i18nAllLabel: 'i18nAllStr',
        short: 'Str',
        exp: 'i18nString',
        org_type: 'TEXT',
        operator: ['', 'Valid-like'],
        selectionBoxDisplay: 'Str',
    },
    REAL_SEP: {
        name: 'REAL_SEP',
        value: 5,
        bs_value: 'REAL_SEP',
        label: $('#i18nRealSep').text(),
        i18nLabelID: 'i18nFloatSep',
        i18nAllLabel: 'i18nAllRealSep',
        short: 'Real_Sep',
        exp: '',
        org_type: 'REAL',
        operator: ['', '+', '-', '*', '/'],
    },
    INTEGER_SEP: {
        name: 'INTEGER_SEP',
        value: 6,
        bs_value: 'INTEGER_SEP',
        label: $('#i18nIntSep').text(),
        i18nLabelID: 'i18nIntSep',
        i18nAllLabel: 'i18nAllIntSep',
        short: 'Int_Sep',
        exp: '',
        org_type: 'INTEGER',
        operator: ['', '+', '-', '*', '/'],
    },
    EU_REAL_SEP: {
        name: 'EU_REAL_SEP',
        value: 7,
        bs_value: 'EU_REAL_SEP',
        label: $('#i18nEURealSep').text(),
        i18nLabelID: 'i18nEUFloatSep',
        i18nAllLabel: 'i18nAllEURealSep',
        short: 'EU_Real_Sep',
        exp: '',
        org_type: 'REAL',
        operator: ['', '+', '-', '*', '/'],
    },
    EU_INTEGER_SEP: {
        name: 'EU_INTEGER_SEP',
        value: 8,
        bs_value: 'EU_INTEGER_SEP',
        label: $('#i18nEUIntSep').text(),
        i18nLabelID: 'i18nEUIntSep',
        i18nAllLabel: 'i18nAllEUIntSep',
        short: 'EU_Int_Sep',
        exp: '',
        org_type: 'INTEGER',
        operator: ['', '+', '-', '*', '/'],
    },
    BOOLEAN: {
        name: 'BOOLEAN',
        value: 11,
        bs_value: 'BOOLEAN',
        label: $('#i18nBoolean').text(),
        i18nLabelID: 'i18nBool',
        i18nAllLabel: 'i18nAllIntCategory',
        short: 'Bool',
        exp: 'i18nIntegerCategory',
        org_type: 'CATEGORY_INTEGER',
    },
    JUDGE: {
        name: 'JUDGE',
        value: 101,
        bs_value: 'JUDGE',
        label: $('#i18nJudge').text(),
        i18nLabelID: 'i18nJudge',
        i18nAllLabel: 'i18nAllJudge',
        short: 'Judge',
        exp: 'i18nIntegerCategory',
        org_type: 'CATEGORY_INTEGER',
    },
    SMALL_INT: {
        name: 'SMALL_INT',
        value: 9,
        bs_value: 'SMALL_INT',
        label: $('#i18nSmallInt').text(),
        i18nLabelID: 'i18nSmallInt',
        i18nAllLabel: 'i18nAllSmallInt',
        short: 'Int(Category)',
        exp: 'i18nIntegerCategory',
        org_type: 'INTEGER',
    },
    SMALL_INT_SEP: {
        name: 'SMALL_INT_SEP',
        value: 14,
        bs_value: 'SMALL_INT_SEP',
        label: $('#i18nIntSep').text(),
        i18nLabelID: 'i18nSmallIntSep',
        i18nAllLabel: 'i18nAllSmallIntSep',
        short: '',
        exp: '',
        org_type: 'INTEGER',
    },
    EU_SMALL_INT_SEP: {
        name: 'EU_SMALL_INT_SEP',
        value: 16,
        bs_value: 'EU_SMALL_INT_SEP',
        label: $('#i18nEUSmallIntSep').text(),
        i18nLabelID: 'i18nEUSmallIntSep',
        i18nAllLabel: 'i18nAllEUSmallIntSep',
        short: '',
        exp: '',
        org_type: 'INTEGER',
        operator: ['', '+', '-', '*', '/'],
    },
    BIG_INT: {
        name: 'BIG_INT',
        value: 10,
        bs_value: 'BIG_INT',
        label: $('#i18nBigInt').text(),
        i18nLabelID: 'i18nInteger_Int64',
        i18nAllLabel: 'i18nAllBigInt',
        short: 'Str',
        exp: 'i18nString',
        org_type: 'STRING',
        operator: ['', '+', '-', '*', '/'],
    },
    SERIAL: {
        name: 'SERIAL',
        short: 'Seri',
    },
    BIGINT_SEP: {
        name: 'BIGINT_SEP',
        value: 15,
        bs_value: 'BIGINT_SEP',
        label: $('#i18nBigInt').text(),
        i18nLabelID: 'i18nBigIntSep',
        i18nAllLabel: 'i18nAllBigIntSep',
        short: '',
        exp: '',
        org_type: 'INTEGER',
    },
    EU_BIGINT_SEP: {
        name: 'EU_BIGINT_SEP',
        value: 17,
        bs_value: 'EU_BIGINT_SEP',
        label: $('#i18nEUBigIntSep').text(),
        i18nLabelID: 'i18nEUBigIntSep',
        i18nAllLabel: 'i18nAllEUBigIntSep',
        short: '',
        exp: '',
        org_type: 'INTEGER',
    },
    MASTER: {
        name: 'MASTER',
        value: 100,
        bs_value: 'TEXT',
        label: 'Master',
        i18nLabelID: 'i18nMaster',
        i18nAllLabel: 'i18nAllMaster',
        short: 'Master',
        exp: 'i18nCategory',
        org_type: 'TEXT',
    },
    CATEGORY: {
        name: 'CATEGORY',
        value: 13,
        bs_value: 'CATEGORY',
        label: '文字列(カテゴリ)',
        i18nLabelID: 'i18nCategory',
        i18nAllLabel: 'i18nAllCategory',
        short: 'Cat',
        exp: 'i18nCategory',
        org_type: 'TEXT',
    },
    LOGICAL: {
        name: 'LOGICAL',
        value: 102,
        bs_value: 'BOOLEAN',
        label: 'Not yet implemented',
        i18nLabelID: 'i18nLogical',
        i18nAllLabel: 'Not yet implemented',
        short: 'Not yet implemented',
        exp: 'Not yet implemented',
        org_type: 'BOOLEAN',
    },
    ORDER: {
        name: 'DATETIME',
        value: 8,
        bs_value: 'DATETIME',
        label: `${$('#i18nDateTime').text() || '日付'}(Order)`,
        i18nLabelID: 'i18nDateTime',
        short: 'CT',
        exp: 'i18nCTTypeExplain',
        org_type: 'DATETIME',
    },
    DATE: {
        name: 'DATE',
        value: 103,
        bs_value: 'DATE',
        label: `${$('#i18nDate').text() || '日付'}`,
        i18nLabelID: 'i18nDate',
        short: 'Str',
        exp: 'i18nDate',
        org_type: 'DATE',
    },
    TIME: {
        name: 'TIME',
        value: 104,
        bs_value: 'TIME',
        label: `${$('#i18nTime').text() || '時間'}`,
        i18nLabelID: 'i18nTime',
        short: 'Str',
        exp: 'i18nTime',
        org_type: 'TIME',
    },
});

const dataTypeShort = (col) => {
    if (col.is_serial_no) {
        return DataTypes.SERIAL.short;
    }

    if (isMasterDataType(col.column_type)) {
        return DataTypes.MASTER.short;
    }

    if (col.is_int_category && !col.is_serial_no) {
        return DataTypes.CATEGORY.short;
    }
    const dataType = col.data_type || col.type;
    return dataType ? DataTypes[dataType].short : '';
};

const getDataTypeByRawDataType = ({ raw_data_type }) => {
    const foundDataType = Object.entries(DataTypes).find(
        ([_key, dataTypes]) => dataTypes.bs_value === raw_data_type,
    );
    if (foundDataType) {
        return foundDataType[1];
    }
    return null;
};

const DataTypeShort = {
    i: 1,
    r: 2,
    t: 3,
    d: 4,
};

const filterOptions = {
    NO_FILTER: 'NO_FILTER',
    ALL: 'ALL',
};

const CfgProcess_CONST = {
    REAL_TYPES: [
        DataTypes.REAL.name,
        DataTypes.EU_REAL_SEP.name,
        DataTypes.REAL_SEP.name,
    ],
    NUMERIC_TYPES: [
        DataTypes.REAL.name,
        DataTypes.INTEGER.name,
        DataTypes.EU_REAL_SEP.name,
        DataTypes.REAL_SEP.name,
        DataTypes.INTEGER_SEP.name,
        DataTypes.EU_INTEGER_SEP.name,
    ],
    NUMERIC_AND_STR_TYPES: [
        DataTypes.REAL.name,
        DataTypes.INTEGER.name,
        DataTypes.STRING.name,
        DataTypes.TEXT.name,
        DataTypes.EU_REAL_SEP.name,
        DataTypes.REAL_SEP.name,
        DataTypes.INTEGER_SEP.name,
        DataTypes.EU_INTEGER_SEP.name,
    ],
    ALL_TYPES: [
        DataTypes.DATETIME.name,
        DataTypes.REAL.name,
        DataTypes.INTEGER.name,
        DataTypes.STRING.name,
        DataTypes.TEXT.name,
        DataTypes.EU_REAL_SEP.name,
        DataTypes.REAL_SEP.name,
        DataTypes.INTEGER_SEP.name,
        DataTypes.EU_INTEGER_SEP.name,
        DataTypes.DATE.name,
        DataTypes.TIME.name,
    ],
    CATEGORY_TYPES: [
        DataTypes.STRING.name,
        DataTypes.INTEGER.name,
        DataTypes.TEXT.name,
        DataTypes.INTEGER_SEP.name,
        DataTypes.EU_INTEGER_SEP.name,
        DataTypes.BIG_INT.name,
        DataTypes.DATE.name,
        DataTypes.TIME.name,
    ],
    CT_TYPES: [DataTypes.DATETIME.name],
    INTEGER_TYPES: [
        DataTypes.INTEGER.bs_value,
        DataTypes.SMALL_INT.bs_value,
        DataTypes.BIG_INT.bs_value,
    ],
    EU_TYPE_VALUE: [
        DataTypes.REAL_SEP.value,
        DataTypes.EU_REAL_SEP.value,
        DataTypes.INTEGER_SEP.value,
        DataTypes.EU_INTEGER_SEP.value,
    ],
};

class CfgColumn {
    id;
    column_name;
    column_raw_name;
    bridge_column_name;
    column_type;
    data_type;
    raw_data_type;
    name_en;
    name_jp;
    name_local;
    shown_name;
    is_auto_increment;
    is_get_date;
    is_serial_no;
    is_int_category;
    is_category;
    is_linking_column;
    operator;
    coef;
    order;

    constructor(inObj) {
        // set data
        Object.assign(this, inObj);
    }
}

class CfgFilter {
    id;
    name;
    column_id;
    filter_details;
    filter_type;
    parent_id;
    process_id;
    master_info; // Contain detail information of this master base on id

    constructor(inObj) {
        // set data
        Object.assign(this, inObj);
    }

    static filterTypes = {
        LINE: 'LINE_ID',
        MACHINE: 'EQUIP_ID',
        PART_NO: 'PART_NO',
        OTHER: 'OTHER',
    };

    static filterOptions = {
        NO_FILTER: 'NO_FILTER',
        ALL: 'ALL',
    };
}

class CfgVisualization {
    id = null;
    process_id = null;
    control_column_id = null;
    filter_column_id = null;
    filter_value = null;
    is_from_data = null;
    filter_detail_id = null;
    ucl = null;
    lcl = null;
    upcl = null;
    lpcl = null;
    ymax = null;
    ymin = null;
    act_from = null;
    act_to = null;
    order = null;

    constructor(inObj) {
        // set data
        Object.assign(this, inObj);
    }
}

class CfgProcess {
    id;
    name;
    name_jp;
    name_en;
    name_local;
    shown_name;
    table_name;
    data_source_id;
    data_source;
    order;
    columns = [CfgColumn]; // may be dictionary {colId: columnObject}
    filters = [CfgFilter]; // may be dictionary {filterId: filterObject}
    visualizations = [CfgVisualization]; // may be dictionary {filterId: filterObject}

    // TODO use dict, remove array above
    dicColumns = {};

    // col -> univeral data
    dicColumnData = {}; // columnId -> [val1, val2, ...]

    ct_range = [];

    constructor(inObj) {
        // set data
        Object.assign(this, inObj);

        // instantiate column objects
        this.columns = [];
        if (inObj && inObj.columns) {
            const colJsons = inObj.columns || [];
            for (const colJson of colJsons) {
                this.addColumn(colJson);
            }
        }

        // instantiate filter objects
        this.filters = [];
        if (inObj && inObj.filters) {
            const filterJsons = inObj.filters || [];
            for (const filterJson of filterJsons) {
                this.addFilter(filterJson);
            }
        }

        // instantiate filter objects
        this.visualizations = [];
        if (inObj && inObj.visualizations) {
            const visualizationJsons = inObj.visualizations || [];
            for (const vJson of visualizationJsons) {
                this.addVisualization(vJson);
            }
        }
    }

    addColumn = (column) => {
        const newColumn = new CfgColumn(column);
        this.columns.push(newColumn);
        this.dicColumns[newColumn.id] = newColumn;
    };

    addFilter = (filter) => {
        this.filters.push(new CfgFilter(filter));
    };

    addVisualization = (visualizationJson) => {
        this.visualizations.push(new CfgVisualization(visualizationJson));
    };

    getColumns = () => {
        return this.columns;
    };

    getColumnById = (colId) => {
        return this.dicColumns[colId];
    };

    getFilters = () => {
        return this.filters;
    };

    getVisualizations = () => {
        return this.visualizations;
    };

    getFiltersByType = (filterType) => {
        if (this.filters) {
            return this.filters.filter((pf) => pf.filter_type === filterType);
        }
        return;
    };

    getOneFilterByType = (filterType) => {
        const relevantFilters = this.filters.filter(
            (filter) => filter.filter_type === filterType,
        );
        if (relevantFilters.length) {
            return relevantFilters[0];
        }
        return null;
    };

    getFilterByColumnId = (columnId) => {
        const relevantFilters = this.filters.filter(
            (filter) => `${filter.column_id}` === `${columnId}`,
        );
        if (relevantFilters.length) {
            return relevantFilters[0];
        }
        return null;
    };

    getCategoryColumns() {
        return this.columns.filter((col) =>
            CfgProcess_CONST.CATEGORY_TYPES.includes(col.data_type),
        );
    }

    getNumericColumns() {
        return this.columns.filter((col) =>
            CfgProcess_CONST.NUMERIC_TYPES.includes(col.data_type),
        );
    }

    getCTColumn() {
        return this.columns.filter(
            (col) =>
                CfgProcess_CONST.CT_TYPES.includes(col.data_type) &&
                col.is_get_date,
        );
    }

    getDatetimeColumns() {
        return this.columns.filter(
            (col) =>
                CfgProcess_CONST.CT_TYPES.includes(col.data_type) &&
                !col.is_get_date,
        );
    }

    updateColumns = async () => {
        if (this.columns && this.columns.length) {
            return;
        } else {
            await this.getColumnFromDB();
        }
    };

    getColumnFromDB = async () => {
        const url = `/ap/api/setting/proc_config/${this.id}/columns`;
        const res = await fetchData(url, {}, 'GET');
        if (res.data) {
            this.columns = [];
            for (let colJson of res.data) {
                const cfgColumn = new CfgColumn(colJson);
                this.columns.push(cfgColumn);
                this.dicColumns[cfgColumn.id] = cfgColumn;
            }
        }
    };

    updateFilters = async () => {
        if (this.filters && this.filters.length) {
            return;
        } else {
            await this.updateProcFilters();
        }
    };

    // get filter from process config
    updateProcFilters = async () => {
        const url = `/ap/api/setting/proc_config/${this.id}/filters`;
        const res = await fetchData(url, {}, 'GET');
        this.filters = [];
        if (res.data) {
            for (let filterItem of res.data) {
                const cfgFilter = new CfgFilter(filterItem);
                this.filters.push(cfgFilter);
            }
        }
    };

    setColumnData = (columnId, data) => {
        this.dicColumnData[columnId] = data;
    };

    getColumnData = (columnId) => {
        return this.dicColumnData[columnId] || [];
    };

    updateColDataFromUDB = async (columnId) => {
        if (
            this.dicColumnData[columnId] &&
            this.dicColumnData[columnId].length
        ) {
            return;
        } else {
            await this.getColumnDataFromUDB(columnId);
        }
    };

    getColumnDataFromUDB = async (columnId) => {
        if (isEmpty(columnId)) return;
        const url = `/ap/api/setting/distinct_sensor_values/${columnId}`;
        const res = await fetchData(url, {}, 'GET');
        if (res.data) {
            this.dicColumnData[columnId] = res.data || [];
        }
    };

    getXAxisSetting = () => {
        const columns = this.columns;
        return columns;
    };

    getCTRange = async () => {
        const url = `/ap/api/setting/proc_config/${this.id}/get_ct_range`;
        const res = await fetchData(url, {}, 'GET');
        if (res.data) {
            this.ct_range = res.data;
        }
    };
}

class CfgDataSourceDB {
    id;
    host;
    port;
    dbname;
    schema;
    username;
    password;
    hashed;
    use_os_timezone;

    constructor(inObj) {
        // set data
        Object.assign(this, inObj);
    }
}

class CfgCsvColumn {
    id;
    data_source_id;
    column_name;
    data_type;

    constructor(inObj) {
        // set data
        Object.assign(this, inObj);
    }
}

class CfgDataSourceCSV {
    id;
    directory;
    skip_head;
    skip_tail;
    delimiter;
    etl_func;
    csv_columns;

    constructor(inObj) {
        // set data
        Object.assign(this, inObj);

        // instantiate column objects
        this.csv_columns = [];
        const csv_cols = inObj.csv_columns || [];
        for (const csv_col of csv_cols) {
            this.add_column(csv_col);
        }
    }

    add_column(column) {
        this.csv_columns.push(new CfgCsvColumn(column));
    }
}

class CfgDataSource {
    id;
    name;
    type;
    comment;
    order;
    db_detail;
    csv_detail;
    processes;

    constructor(inObj) {
        // set data
        Object.assign(this, inObj);

        // instantiate column objects
        this.processes = [];
        const procs = inObj.processes || [];
        for (const proc of procs) {
            this.add_process(proc);
        }

        this.csv_detail = new CfgDataSourceCSV(inObj.csv_detail);
        this.db_detail = new CfgDataSourceDB(inObj.db_detail);
    }

    add_process(proc) {
        this.processes.push(new CfgProcess(proc));
    }
}

function sortByOrderOrID(proc1, proc2) {
    const order1 = proc1.order + 1 || proc1.id;
    const order2 = proc2.order + 1 || proc2.id;
    return order1 < order2 ? -1 : order1 > order2 ? 1 : 0;
}

const genProcessDropdownData = (procConfigs = {}) => {
    const ids = [''];
    const names = ['---'];
    Object.values(procConfigs)
        .sort(sortByOrderOrID)
        .forEach((proc) => {
            ids.push(proc.id);
            names.push({
                shown_name: proc.shown_name,
                name_en: proc.name_en,
            });
        });

    return {
        ids,
        names,
    };
};

const rawDataTypeToDataType = (s) => {
    switch (s) {
        case DataTypes.REAL:
            return DataTypes.REAL.bs_value;
        case DataTypes.SMALL_INT:
        case DataTypes.INTEGER:
        case DataTypes.BIG_INT:
            return DataTypes.INTEGER.bs_value;
        case DataTypes.MASTER:
        case DataTypes.STRING:
        case DataTypes.CATEGORY:
            return DataTypes.STRING.bs_value;
        case DataTypes.DATETIME:
            return 'd';
        case DataTypes.LOGICAL:
        case DataTypes.BOOLEAN:
        case DataTypes.JUDGE:
            return DataTypes.BOOLEAN.bs_value;
        default:
            console.log(`Could not convert ${s}`);
        // case            thousand_sep_comma: $('#i18nThousand_Sep_Comma').text():
        // case            thousand_sep_dot: $('#i18nThousand_Sep_Dot').text():
    }
};
