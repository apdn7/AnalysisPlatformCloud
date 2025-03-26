const HTTP_RESPONSE_CODE_500 = 500;
const FALSE_VALUES = new Set(['', 0, '0', false, 'false', 'FALSE', 'f', 'F']);
// current selected process id, data source and table name
let procModalCurrentProcId = null;
let currentAsLinkIdBox = null;
let currentProcColumns = null;
let currentProcDataCols = [];
let userEditedProcName = false;
let userEditedDSName = false;
let setDatetimeSelected = false;
let prcPreviewData;
let isClickPreview = false;
const BGR_COLOR = '#303030';
let allProcessColumns = [];
let columnNames = [];
let errCells = [];
let dataGroupType = {};
let checkOnFocus = true;
let currentProcessId = null;
let currentProcess = null;

const procModalElements = {
    procModal: $('#procSettingModal'),
    dataTableSettingModal: $('#procSettingModal'),
    procModalBody: $('#procSettingModalBody'),
    procPreviewSection: $('#procPreviewSection'),
    procSettingContent: $('#procSettingContent'),
    proc: $('#procSettingModal input[name=processName]'),
    procJapaneseName: $('#procSettingModal input[name=processJapaneseName]'),
    procLocalName: $('#procSettingModal input[name=processLocalName]'),
    showName: $('#procSettingModal input[name=showName]'),
    cfgDataTable: $('#procSettingModal input[name=dataTableName]'),
    dataTableName: $('#procSettingModal input[name=dataTableName]'),
    comment: $('#procSettingModal input[name=cfgProcComment]'),
    isShowFileName: $('#procSettingModal input[name=isShowFileName]'),
    databases: $('#procSettingModal select[name=cfgProcDatabaseName]'),
    tables: $('#procSettingModal select[name=cfgProcTableName]'),
    showRecordsBtn: $('#procSettingModal button[name=showRecords]'),
    showPreviewBtnToMerge: $(
        '#procSettingMergeModeModal button[name=showRecords]',
    ),
    latestDataHeader: $('#procSettingModal table[name=latestDataTable] thead'),
    latestDataBody: $('#procSettingModal table[name=latestDataTable] tbody'),
    processColumnsTableBody: $(
        '#procSettingModal table[name=processColumnsTable] tbody',
    ),
    processColumnsTable: $('#procSettingModal table[name=processColumnsTable]'),
    processColumnsSampleDataTable: $(
        '#procSettingModal table[name=processColumnsTableSampleData]',
    ),
    processColumnsSampleDataTableBody: $(
        '#procSettingModal table[name=processColumnsTableSampleData] tbody',
    ),
    processColumnsTableId: 'processColumnsTable',
    okBtn: $('#procSettingModal button[name=okBtn]'),
    scanBtn: $('#procSettingModal button[name=scanBtn]'),
    reRegisterBtn: $('#procSettingModal button[name=reRegisterBtn]'),
    loadingSampleData: () => $('#procSettingModal div[name=loadingSampleData]'),
    confirmImportDataBtn: $('#confirmImportDataBtn'),
    confirmScanMasterDataBtn: $('#confirmScanMasterDataBtn'),
    confirmReRegisterProcBtn: $('#confirmReRegisterProcBtn'),
    revertChangeAsLinkIdBtn: $('#revertChangeAsLinkIdBtn'),
    confirmImportDataModal: $('#confirmImportDataModal'),
    confirmScanMasterDataModal: $('#confirmScanMasterDataModal'),
    confirmReRegisterProcModal: $('#confirmReRegisterProcModal'),
    warningResetDataLinkModal: $('#warningResetDataLinkModal'),
    warningResetDataLinkModalDataTable: $(
        '#warningResetDataLinkModalDataTable',
    ),
    procConfigConfirmSwitchModal: $('#procConfigConfirmSwitchModal'),
    procConfigCategoryErrorModal: $('#procConfigCategoryErrorModal'),
    totalColumns: $('#totalColumns'),
    totalCheckedColumns: $('#totalCheckedColumn'),
    totalCheckedColumnsContent: $('.total-checked-columns'),
    searchInput: $('#processConfigModalSearchInput'),
    searchSetBtn: $('#processConfigModalSetBtn'),
    searchResetBtn: $('#processConfigModalResetBtn'),
    confirmDataTypeModal: '#confirmChangeKSepDataTypeModal',
    confirmNullValue: '#confirmNullValueInColumnModal',
    confirmSameValue: '#confirmSameValueInColumnModal',
    dateTime: 'dateTime',
    serial: 'serial',
    mainSerial: 'mainSerial',
    order: 'order',
    dataSerial: 'dataSerial',
    dataType: 'dataType',
    format: 'format',
    auto_increment: 'auto_increment',
    columnName: 'columnName',
    columnRawName: 'columnRawName',
    englishName: 'englishName',
    systemName: 'systemName',
    shownName: 'shownName',
    japaneseName: 'japaneseName',
    localName: 'localName',
    unit: 'unit',
    operator: 'operator',
    coef: 'coef',
    lineName: 'lineName',
    lineNo: 'lineNo',
    equiptName: 'equiptName',
    equiptNo: 'equiptNo',
    partName: 'partName',
    partNo: 'partNo',
    intCat: 'intCat',
    columnType: 'columnType',
    procsMasterName: 'processName',
    procsComment: 'cfgProcComment',
    procsdbName: 'cfgProcDatabaseName',
    procsTableName: 'cfgProcTableName',
    // procsComment: 'comment',
    // procsdbName: 'databaseName',
    // procsTableName: 'tableName',
    isDummyDatetime: 'isDummyDatetime',
    columnNameInput:
        '#procSettingModal table[name=processColumnsTable] input[name="columnName"]',
    systemNameInput:
        '#procSettingModal table[name=processColumnsTable] input[name="systemName"]',
    masterNameInput:
        '#procSettingModal table[name=processColumnsTable] input[name="shownName"]',
    latestDataTable: $('#procSettingModal table[name=latestDataTable]'),
    msgProcNameAlreadyExist: '#i18nAlreadyExist',
    msgProcNameBlank: '#i18nProcNameBlank',
    msgGenDateTime: '#i18nGenDateTimeNotification',
    msgSelectDateAndTime: '#i18nRequestSelectDateAndTime',
    msgShownNameBlank: '#i18nShownNameBlank',
    msgModal: '#msgModal',
    msgContent: '#msgContent',
    msgConfirmBtn: '#msgConfirmBtn',
    // selectAllSensor: '#selectAllSensor',
    selectAllSensor: '#selectAllSensor',
    alertMessage: '#alertMsgProcessColumnsTable',
    alertProcessNameErrorMsg: '#alertProcessNameErrorMsg',
    alertErrMsgContent: '#alertProcessNameErrorMsg-content',
    selectAllColumn: '#selectAllColumn',
    procID: $('#procSettingModal input[name=processID]'),
    dataTableID: $('#dataTableSettingModal input[name=dataTableId]'),
    dsID: $('#procSettingModal input[name=processDsID]'),
    formId: '#procCfgForm',
    procSettingModalDownloadAllBtn: '#procSettingModalDownloadAllBtn',
    procSettingModalCopyAllBtn: '#procSettingModalCopyAllBtn',
    procSettingModalPasteAllBtn: '#procSettingModalPasteAllBtn',
    settingContent: '#procSettingContent',
    cfgDataTableContent: '#dataTableSettingContent',
    selectedColumnsTable: 'selectedColumnsTable',
    prcSM: '#prcSM',
    dataSM: '#dataSM',
    autoSelectAllColumn: '#autoSelectAllColumn',
    autoSelect: '#autoSelect',
    checkColsContextMenu: '#checkColsContextMenu',
    createOrUpdateProcCfgBtn: $('#createOrUpdateProcCfgBtn'),
    dbTableList: '#dbTableList',
    fileInputPreview: '#fileInputPreview',
    fileName: $('#procSettingModal input[name=fileName]'),
    dataGroupTypeClassName: 'data-type-selection',
    dataTypeSelection: 'dataTypeSelection',
    sampleDataColumnClassName: 'sample-data-column',
    mainDate: 'mainDate',
    mainTime: 'mainTime',
    formEles: {
        colName: 'col_name',
        columnRawName: 'column_raw_name',
        englishName: 'name_en',
        japaneseName: 'name_jp',
        localName: 'name_local',
        dataType: 'data_type',
        format: 'format',
        sampleData: 'sample_data',
    },
    procDateTimeFormatCheckbox: $(
        '#procDateTimeFormatCheckbox input[name="toggleProcDatetimeFormat"]',
    ),
    procDateTimeFormatInput: $(
        '#procDateTimeFormatInput input[name="procDatetimeFormat"]',
    ),
};

const procConfigColumnsIndex = {
    columnName: 0,
    englishName: 1,
    japaneseName: 2,
    localName: 3,
    dataType: 4,
    format: 5,
    sampleData: [6, 7, 8, 9, 10],
};

const procModali18n = {
    emptyShownName: '#i18nEmptyShownName',
    useEnglishName: '#i18nUseEnglishName',
    noMasterName: '#validateNoMasterName',
    duplicatedSystemName: '#validateDuplicatedSystem',
    noEnglishName: '#validateNoEnglishName',
    duplicatedJapaneseName: '#validateDuplicatedJapaneseName',
    duplicatedLocalName: '#validateDuplicatedLocalName',
    duplicatedMasterName: '#validateDuplicatedMaster',
    noZeroCoef: '#validateCoefErrMsgNoZero',
    emptyCoef: '#validateCoefErrMsgEmptyCoef',
    needOperator: '#validateCoefErrMsgNeedOperator',
    settingMode: '#filterSettingMode',
    editMode: '#filterEditMode',
    copyToAllColumn: '#i18nCopyToAllBelow',
    copyToAllBelow: '#i18nCopyToAllBelow',
    i18nMainDatetime: '#i18nMainDatetime',
    i18nMainSerialInt: '#i18nMainSerialInt',
    i18nMainSerialStr: '#i18nMainSerialStr',
    i18nDatetimeKey: '#i18nDatetimeKey',
    i18nSerialInt: '#i18nSerialInt',
    i18nSerialStr: '#i18nSerialStr',
    i18nCategory: '#i18nCategory',
    i18nLineNameStr: '#i18nLineNameStr',
    i18nLineNoInt: '#i18nLineNoInt',
    i18nEqNameStr: '#i18nEqNameStr',
    i18nEqNoInt: '#i18nEqNoInt',
    i18nPartNameStr: '#i18nPartNameStr',
    i18nPartNoInt: '#i18nPartNoInt',
    i18nStNoInt: '#i18nStNoInt',
    i18nCopyToFiltered: '#i18nCopyToFiltered',
    i18nColumnRawName: '#i18nColumnRawName',
    i18nSampleData: '#i18nSampleData',
    i18nSpecial: '#i18nSpecial',
    i18nFilterSystem: '#i18nFilterSystem',
    i18nMultiset: '#i18nMultiset',
    i18nDatatype: '#i18nDatatype',
    noDatetimeCol: $('#i18nNoDatetimeCol').text(),
    i18nMainDate: '#i18nMainDate',
    i18nMainTime: '#i18nMainTime',
};

const isJPLocale = docCookies.isJaLocale();
const translateToEng = async (text) => {
    const result = await fetchData(
        '/ap/api/setting/to_eng',
        JSON.stringify({ colname: text }),
        'POST',
    );
    return result;
};

const convertIdxToExcelCol = (idx) => {
    const alphabet = 'ABCDEFGHIJKLMNOPQRSTUWXYZ';
    if (isEmpty(idx) || idx >= alphabet.length) return '';
    return alphabet[idx];
};

const setProcessName = (dataRowID = null) => {
    const procDOM = $(`#tblProcConfig tr[data-rowid=${dataRowID}]`);

    const procNameInput = procModalElements.proc.val();

    if (userEditedProcName && procNameInput) {
        return;
    }

    const dsNameSelection = getSelectedOptionOfSelect(
        procModalElements.databases,
    ).text();
    const tableNameSelection = getSelectedOptionOfSelect(
        procModalElements.tables,
    ).text();

    // get setting information from outside card
    const settingProcName = procDOM.find('input[name="processName"]')[0];
    // const settingDSName = procDOM.find('select[name="cfgProcDatabaseName"]')[0];
    const settingTableName = procDOM.find('select[name="cfgProcTableName"]')[0];

    // add new proc row
    let firstGenerated = false;
    if (dataRowID && settingProcName && !procNameInput) {
        procModalElements.proc.val($(settingProcName).val());
        firstGenerated = true;
    }

    // when user change ds or table, and empty process name
    // || !userEditedProcName
    // if (!procModalElements.proc.val()) {
    // if (!userEditedProcName) {
    if (
        (!firstGenerated && !userEditedProcName) ||
        !procModalElements.proc.val()
    ) {
        let firstTableName = '';
        if (tableNameSelection) {
            firstTableName = tableNameSelection;
        } else if (settingTableName) {
            firstTableName = $(settingTableName).val();
        }
        let combineProcName = firstTableName
            ? `${dsNameSelection}_${firstTableName}`
            : dsNameSelection;
        // set default jp and local process name
        if (isJPLocale) {
            procModalElements.procJapaneseName.val(combineProcName);
        } else {
            procModalElements.procLocalName.val(combineProcName);
        }
        translateToEng(combineProcName).then((res) => {
            if (res.data) {
                procModalElements.proc.val(res.data);
            } else {
                procModalElements.proc.val(combineProcName);
            }
        });
    }

    if (!procModalCurrentProcId) {
        // remove selected table
        // $(`table[name=${procModalElements.processColumnsTable}] tbody`).empty();
        procModalElements.showRecordsBtn.trigger('click');
    }
};

// reload tables after change
const loadTables = (databaseId, dataRowID = null, selectedTbl = null) => {
    // if new row have process name, set new process name in modal
    if (!databaseId) {
        setProcessName(dataRowID);
        return;
    }
    if (isEmpty(databaseId)) return;
    procModalElements.tables.empty();
    procModalElements.tables.prop('disabled', false);

    const isHiddenFileInput = $(procModalElements.fileInputPreview).hasClass(
        'hide',
    );
    const isHiddenDbTble = $(procModalElements.dbTableList).hasClass('hide');

    ajaxWithLog({
        url: `api/setting/database_table/${databaseId}`,
        method: 'GET',
        cache: false,
    }).done((res) => {
        res = jsonParse(res);
        if (res.ds_type.toLowerCase() === 'csv') {
            procModalElements.tables.empty();
            procModalElements.tables.prop('disabled', true);
            if (!procModalCurrentProcId) {
                setProcessName(dataRowID);
            }
            // hide 'table' dropdown if there is CSV datasource
            if (isHiddenFileInput) {
                toggleDBTableAndFileName();
            }
        } else if (res.tables) {
            res.tables.forEach((tbl) => {
                const options = {
                    value: tbl,
                    text: tbl,
                };
                if (selectedTbl && tbl === selectedTbl) {
                    options.selected = 'selected';
                }
                procModalElements.tables.append($('<option/>', options));
            });

            if (!procModalCurrentProcId && selectedTbl) {
                setProcessName(dataRowID);
            }
            // hide 'fileName' input if there is DB datasource
            if (isHiddenDbTble) {
                toggleDBTableAndFileName();
            }
        }

        if (!selectedTbl) {
            procModalElements.tables.val('');
        }
    });
};
const toggleDBTableAndFileName = () => {
    $(procModalElements.dbTableList).toggleClass('hide');
    $(procModalElements.fileInputPreview).toggleClass('hide');
};
// load current proc name, database name and tables name

const loadProcModal = async (procId = null, dataRowID = null, dbsId = null) => {
    // set current proc
    procModalCurrentProcId = procId;

    // load databases
    const dbInfo = await getAllDatabaseConfig();

    if (dbInfo != null && Array.isArray(dbInfo) && dbInfo.length > 0) {
        procModalElements.databases.html('');
        let selectedDs = null;
        let selectedTbls = null;
        if (dataRowID) {
            const procDOM = $(`#tblProcConfig tr[data-rowid=${dataRowID}]`);
            const settingDSName = procDOM.find(
                'select[name="cfgProcDatabaseName"]',
            )[0];
            const settingTableName = procDOM.find(
                'select[name="cfgProcTableName"]',
            )[0];

            selectedDs = settingDSName ? $(settingDSName).val() : null;
            selectedTbls = settingTableName ? $(settingTableName).val() : null;
        }
        const currentDs = procModalElements.dsID.val() || dbsId || selectedDs;
        dbInfo.forEach((ds) => {
            const options = {
                type: ds.type,
                value: ds.id,
                text: ds.name,
                title: ds.en_name,
            };
            if (currentDs && ds.id === Number(currentDs)) {
                options.selected = 'selected';
            }
            procModalElements.databases.append($('<option/>', options));
        });

        if (!currentDs) {
            procModalElements.databases.val('');
        }

        const selectedDbInfo = dbInfo.filter(
            (db) => Number(db.id) === Number(currentDs),
        )[0];

        // hide 'table' dropdown if there is CSV datasource
        const isHiddenFileInput = $(
            procModalElements.fileInputPreview,
        ).hasClass('hide');
        const isHiddenDbTble = $(procModalElements.dbTableList).hasClass(
            'hide',
        );
        const isCSVDS =
            selectedDbInfo &&
            ['v2', 'csv'].includes(selectedDbInfo.type.toLowerCase());
        if ((isCSVDS && isHiddenFileInput) || (!isCSVDS && isHiddenDbTble)) {
            toggleDBTableAndFileName();
        }

        if (currentDs) {
            if (
                ![
                    DB.DB_CONFIGS.CSV.type.toLowerCase(),
                    DB.DB_CONFIGS.V2.type.toLowerCase(),
                ].includes(selectedDbInfo.type.toLowerCase()) ||
                dbsId
            ) {
                const defaultDSID = selectedDs || selectedDbInfo.id;
                loadTables(defaultDSID, dataRowID, selectedTbls);
            } else {
                procModalElements.databases.trigger('change');
            }
        } else {
            procModalElements.tables.append(
                $('<option/>', {
                    value: '',
                    text: '---',
                }),
            );

            procModalElements.tables.prop('disabled', true);
        }
    }

    addAttributeToElement();
};

const getColDataType = (colName) => {
    const colInSettingTbl = $('#selectedColumnsTable').find(
        `input[name="columnName"][value="${colName}"]`,
    );
    const datType = colInSettingTbl
        ? $(colInSettingTbl[0]).attr('data-type')
        : '';
    return datType;
};

// --- B-Sprint36+80 #5 ---
const storePreviousValue = (element) => {
    element.previousValue = element.value;
};
// --- B-Sprint36+80 #5 ---

// generate 5 records and their header
const genColumnWithCheckbox = (cols, rows, dummyDatetimeIdx, yoyakugoRows) => {
    const header = [];
    const datas = [];
    const dataTypeSelections = [];
    const yoyakugoSelections = [];
    const colTypes = [];

    const intlabel = $(`#${DataTypes.INTEGER.i18nLabelID}`).text();
    const strlabel = $(`#${DataTypes.STRING.i18nLabelID}`).text();
    const dtlabel = $(`#${DataTypes.DATETIME.i18nLabelID}`).text();
    const reallabel = $(`#${DataTypes.REAL.i18nLabelID}`).text();
    const realSepLabel = $(`#${DataTypes.REAL_SEP.i18nLabelID}`).text();
    const intSepLabel = $(`#${DataTypes.INTEGER_SEP.i18nLabelID}`).text();
    const euRealSepLabel = $(`#${DataTypes.EU_REAL_SEP.i18nLabelID}`).text();
    const euIntSepLabel = $(`#${DataTypes.EU_INTEGER_SEP.i18nLabelID}`).text();

    const getColType = (col) => {
        const orgColType = getColDataType(col.column_name);
        const colType = orgColType ? orgColType : col.data_type;
        return colType;
    };

    const yoyakugoOptions = [];
    yoyakugoOptions.push(`<option value="">---</option>`);
    yoyakugoRows.forEach((row) => {
        yoyakugoOptions.push(`<option value="${row.id}">${row.name}</option>`);
    });

    let isRemoveDummyDatetime =
        cols.filter((col) => {
            const colType = getColType(col);
            if (colType === DataTypes.DATETIME.name) {
                return true;
            }
            return false;
        }).length > 1 && dummyDatetimeIdx !== null;

    cols.forEach((col, i) => {
        let isNull = true;
        const isDummyDatetimeCol = dummyDatetimeIdx === i;
        rows.forEach((row) => {
            if (!isEmpty(row[col.name])) {
                isNull = false;
            }
        });
        header.push(`
        <th class="${isDummyDatetimeCol ? 'dummyDatetimeCol' : ''}">
            <div class="custom-control custom-checkbox">
                <input type="checkbox" onChange="selectTargetColDataTable(this)" ${getColDataType(col.name) ? 'checked' : ''}
                    class="check-item custom-control-input col-checkbox" value="${col.name}"
                    id="checkbox-${col.name_en}" data-type="${col.data_type}" data-romaji="${col.name_en}"
                    data-isnull="${isNull}" data-col-index="${i}" data-m-data-group-id="${col.data_group_type}" data-is-dummy-datetime="${isDummyDatetimeCol}" data-name-jp="${col.name_jp || ''}" data-name-local="${col.name_local || ''}">
                <label class="custom-control-label" for="checkbox-${col.name_en}">${col.name}</label>
            </div>
        </th>`);

        const checkedColType = (type) => {
            if (type === getColType(col)) {
                return ' selected="selected"';
            }
            return '';
        };

        const colType = getColType(col);
        colTypes.push(DataTypes[colType].value);

        dataTypeSelections.push(`<td class="${isDummyDatetimeCol ? 'dummyDatetimeCol' : ''}">
            <select id="col-${i}" class="form-control csv-datatype-selection"
                 onchange="parseDataType(this, ${i}, true)" onfocus="storePreviousValue(this)">  <!-- B-Sprint36+80 #5 -->
                <option value="${DataTypes.REAL.value}"${checkedColType(DataTypes.REAL.name)}>${reallabel}</option>
                <option value="${DataTypes.INTEGER.value}"${checkedColType(DataTypes.INTEGER.name)} ${col.is_big_int ? 'disabled' : ''}>${intlabel}</option>
                <option value="${DataTypes.STRING.value}"${checkedColType(DataTypes.STRING.name)}>${strlabel}</option>
                <option value="${DataTypes.DATETIME.value}"${checkedColType(DataTypes.DATETIME.name)}>${dtlabel}</option>
                <option value="${DataTypes.REAL_SEP.value}"${checkedColType(DataTypes.REAL_SEP.name)}>${realSepLabel}</option>
                <option value="${DataTypes.INTEGER_SEP.value}"${checkedColType(DataTypes.INTEGER_SEP.name)}>${intSepLabel}</option>
                <option value="${DataTypes.EU_REAL_SEP.value}"${checkedColType(DataTypes.EU_REAL_SEP.name)}>${euRealSepLabel}</option>
                <option value="${DataTypes.EU_INTEGER_SEP.value}"${checkedColType(DataTypes.EU_INTEGER_SEP.name)}>${euIntSepLabel}</option>
                <option data-all="all">${copyToAllColumnLabel}</option>
            </select>
            <input id="dataTypeTemp-${i}" value="${DataTypes[colType].value}" hidden disabled>
        </td>`);

        yoyakugoSelections.push(`<td>
            <select id="yoyakugo-col-${i}" class="form-control csv-datatype-selection" name="dataGroupType" onchange="changeSelectedYoyakugoRaw(this, ${i})">  <!-- B-Sprint36+80 #5 -->
                ${yoyakugoOptions.join('')}
            </select>
        </td>`);
        // const selectedVal = $(`#col-${i}`).value();
        // changeBackgroundColor(selectedVal);
    });

    procModalElements.latestDataHeader.empty();
    procModalElements.latestDataHeader.append(`
        <tr>${header.join('')}</tr>
        <tr>${dataTypeSelections.join('')}</tr>
        <tr>${yoyakugoSelections.join('')}</tr>`);

    rows.forEach((row) => {
        const data = [];
        cols.forEach((col, i) => {
            let val;
            const columnColor =
                dummyDatetimeIdx === i ? ' dummy_datetime_col' : '';
            if (col.is_get_date) {
                val = parseDatetimeStr(row[col.column_name]);
            } else {
                val = row[col.column_name];
                if (col.data_type === DataTypes.INTEGER.name) {
                    val = parseIntData(val);
                } else if (col.data_type === DataTypes.REAL.name) {
                    val = parseFloatData(val);
                }
            }
            const isKSep = [
                DataTypes.REAL_SEP.name,
                DataTypes.EU_REAL_SEP.name,
            ].includes(getColType(col));
            data.push(
                `<td style="color: ${isKSep ? 'orange' : ''}" is-big-int="${col.is_big_int ? 1 : 0}" data-original="${row[col.name] || ''}" class="${columnColor}"> ${val || ''} </td>`,
            );
        });
        datas.push(`<tr>${data.join('')}</tr>`);
    });
    // const dataTypeSel = `<tr>${dataTypeSelections.join('')}</tr>`;
    procModalElements.latestDataBody.empty();
    if (datas.length) {
        // procModalElements.latestDataBody.append(dataTypeSel);
        procModalElements.latestDataBody.append(`${datas.join('')}`);
    }

    if (isRemoveDummyDatetime) {
        procModalElements.latestDataTable.find('.dummyDatetimeCol').remove();
        procModalElements.latestDataTable.find('.dummy_datetime_col').remove();
    }

    parseEUDataTypeInFirstTimeLoad();

    showConfirmKSepDataModal(colTypes);

    showConfirmSameAndNullValueInColumn(cols);
};

const generateProcessList = (
    cols,
    rows,
    dummyDatetimeIdx,
    fromRegenerate = false,
    force = false,
    autoCheckSerial = false,
    isImportedProcess = false,
) => {
    if (!cols || !cols.length) return;

    if (!fromRegenerate && Object.values(dicProcessCols).length) {
        // reassign column_type
        cols = cols.map((col) => {
            const columnRawName = dicProcessCols[col.column_name]
                ? dicProcessCols[col.column_name]['column_raw_name'] ||
                  col['column_raw_name']
                : col['column_raw_name'];
            return {
                ...col,
                ...dicProcessCols[col.column_name],
                column_raw_name: columnRawName,
            };
        });
    }

    // sort columns by column_type
    // const sortedCols = [...cols].sort((a, b) => { return a.column_type - b.column_type}); // BS sort by order, not sort by column type
    const sortedCols = [...cols];
    // get sample data for generated datetime column
    const mainDateCol = sortedCols.find(
        (col) => col.column_type === masterDataGroup.MAIN_DATE,
    );
    const mainTimeCol = sortedCols.find(
        (col) => col.column_type === masterDataGroup.MAIN_TIME,
    );
    const getDateCol = sortedCols.find((col) => col.is_get_date);
    if (getDateCol && mainDateCol && mainTimeCol) {
        rows.map((row) => {
            row[getDateCol.column_name] = [
                row[mainDateCol.column_name],
                row[mainTimeCol.column_name],
            ].join(' ');
        });
    }
    if (fromRegenerate && JSON.stringify(cols) === JSON.stringify(sortedCols))
        return;

    procModalElements.processColumnsTableBody.empty();
    procModalElements.processColumnsSampleDataTableBody.empty();
    let hasMainSerialCol = false;

    // case rows = [] -> no data for preview
    if (rows.length == 0) {
        rows = Array(10).fill({});
    }
    const sampleData = (col, i, checkedAtr) =>
        rows.map((row) => {
            const key = col.column_name; //col.column_name ||
            let val;
            const columnColor =
                dummyDatetimeIdx === i ? ' dummy_datetime_col' : '';
            if (col.is_get_date) {
                val = parseDatetimeStr(row[col.column_name]);
            } else {
                val = row[key];
                if (
                    [
                        DataTypes.SMALL_INT.bs_value,
                        DataTypes.INTEGER.bs_value,
                    ].includes(col.raw_data_type)
                ) {
                    val = parseIntData(val);
                } else if (col.raw_data_type === DataTypes.REAL.bs_value) {
                    val = parseFloatData(val);
                } else if (col.raw_data_type === DataTypes.BOOLEAN.bs_value) {
                    val = parseBooleanData(val);
                }
            }
            const isKSep = [
                DataTypes.REAL_SEP.name,
                DataTypes.EU_REAL_SEP.name,
            ].includes(col.data_type);
            return `<td style="color: ${isKSep ? 'orange' : ''}" is-big-int="${col.raw_data_type === DataTypes.BIG_INT.bs_value ? 1 : 0}" data-original="${row[col.column_name] || ''}" class="sample-data row-item show-raw-text${columnColor}" ${checkedAtr}>${!isEmpty(val) ? val : ''}</td>`;
        });
    const colTypes = [];

    let checkedTotal = 0;

    // const isRegisterProc = !_.isEmpty(dicProcessCols);

    let tableContent = '';
    let sampleContent = '';
    const dataTypeObjs = [];

    sortedCols.forEach((col, i) => {
        const column_raw_name = col.column_raw_name;
        const registerCol = dicProcessCols[col.column_name];
        col = fromRegenerate ? col : registerCol || col;
        if (!col.name_en) {
            col.name_en = col.romaji;
        }
        if (!col.column_raw_name) {
            col.column_raw_name = column_raw_name;
        }
        const isChecked = fromRegenerate ? col.is_checked : !!registerCol;
        if (col.is_show) {
            checkedTotal++;
        }
        const isDummyDatetimeCol = dummyDatetimeIdx === i;
        const isDummyDatetime = col.is_dummy_datetime ? true : false;
        // if v2 col_name is シリアルNo -> auto check
        // BS: select serial and datetime in column attribute
        // if ((!registerCol && !fromRegenerate) || autoCheckSerial) {
        //     const isSerial = /^.*シリアル|serial.*$/.test(col.column_name.toString().toLowerCase()) && [DataTypes.STRING.name, DataTypes.INTEGER.name].includes(col.data_type);
        //     if (isSerial && hasMainSerialCol) {
        //         col.is_serial_no = true;
        //     }
        //
        //     if (isSerial && !hasMainSerialCol) {
        //         col.is_main_serial_no = true;
        //         hasMainSerialCol = true;
        //     }
        // }

        // convert column_type to attr key
        // col = { // BS not use
        //     ...col,
        //     ...DataTypeDropdown_Controller.convertColumnTypeToAttrKey(
        //         col.column_type,
        //     ),
        // };

        colTypes.push(DataTypes[col.data_type].value);

        const checkedAtr = 'checked=checked';

        // const isNumeric = isNumericDatatype(col.data_type);
        // const [numericOperators, textOperators, coefHTML] = createOptCoefHTML(col.operator, col.coef, isNumeric, checkedAtr);

        const dataTypeObject = {
            ...col,
            value: col.data_type,
            checked: checkedAtr,
            isRegisteredCol: !!registerCol,
            isRegisterProc: isImportedProcess,
            is_main_date: col.column_type === masterDataGroup.MAIN_DATE,
            is_main_time: col.column_type === masterDataGroup.MAIN_TIME,
        };
        let getKey = '';
        for (const attr of DataTypeAttrs) {
            if (dataTypeObject[attr]) {
                getKey = attr;
                break;
            }
        }

        tableContent +=
            ProcessConfigSection.generateOneRowOfProcessColumnConfigHTML(
                i,
                col,
                getKey,
                dataTypeObject,
                isImportedProcess,
            );

        sampleContent += `<tr class="${col.is_show === true ? '' : 'd-none'}">${sampleData(col, i, checkedAtr).join('')}</tr>`;
        dataTypeObjs.push(dataTypeObject);
    });

    procModalElements.processColumnsTableBody.html(tableContent);
    procModalElements.processColumnsSampleDataTableBody.html(sampleContent);
    document
        .querySelectorAll('div.config-data-type-dropdown')
        .forEach((dataTypeDropdownElement) =>
            DataTypeDropdown_Controller.addEvents(dataTypeDropdownElement),
        );
    let totalColumns = 0;
    for (const column of sortedCols) {
        if (column.is_show) {
            totalColumns++;
        }
    }
    showTotalCheckedColumns(totalColumns, checkedTotal);
    parseEUDataTypeInFirstTimeLoad();

    if (!fromRegenerate) {
        showConfirmSameAndNullValueInColumn(sortedCols);
        showConfirmKSepDataModal(colTypes);
    }
    handleScrollSampleDataTable();
    handleHoverProcessColumnsTableRow();
    validateSelectedColumnInput();
    showProcDatetimeFormatSampleData();
};

const generatedDateTimeSampleData = (dateColId, timeColId) => {
    const dateSampleData = ProcessConfigSection.collectSampleData(dateColId);
    const timeSampleData = ProcessConfigSection.collectSampleData(timeColId);
    let generatedDateTimeSampleData = [];
    dateSampleData.forEach((data, i) => {
        generatedDateTimeSampleData.push(`${data} ${timeSampleData[i]}`);
    });
    return generatedDateTimeSampleData;
};

const showTotalCheckedColumns = (totalColumns, totalCheckedColumn) => {
    procModalElements.totalCheckedColumnsContent.show();
    procModalElements.totalColumns.text(totalColumns);
    setTotalCheckedColumns(totalCheckedColumn);

    procModalElements.searchInput.val('');
};

const setTotalCheckedColumns = (totalCheckedColumns = 0) => {
    procModalElements.totalCheckedColumns.text(totalCheckedColumns);
};

const cleanOldData = () => {
    // clear user editted input flag
    userEditedProcName = false;

    // clear old procInfo
    currentProcColumns = null;
    $(procModalElements.prcSM).html('');
    $(procModalElements.settingContent).removeClass('hide');
    $(procModalElements.prcSM).parent().addClass('hide');

    procModalElements.comment.val('');
    procModalElements.proc.val('');
    procModalElements.procLocalName.val('');
    procModalElements.procJapaneseName.val('');
    procModalElements.procID.val('');
    procModalElements.comment.val('');
    procModalElements.databases.html('');
    procModalElements.tables.html('');
    procModalElements.tables.prop('disabled', false);

    procModalElements.totalCheckedColumnsContent.hide();
    procModalElements.processColumnsTableBody.empty();
    procModalElements.processColumnsSampleDataTableBody.empty();
    procModalElements.fileName.val('');
};

/**
 * Set | unset check status attribute for target row
 *
 * If row is set, row will be color white. Else row will be set gray color
 * @param {jQuery} $targetRow
 * @param {jQuery} $sampleDataRow
 * @param {boolean} isChecked
 */
const setCheckStatusAttributeForRow = (
    $targetRow,
    $sampleDataRow,
    isChecked,
) => {
    if (isChecked) {
        $targetRow.find('.row-item').attr('checked', 'checked');
        $sampleDataRow.find('.row-item').attr('checked', 'checked');
    } else {
        $targetRow.find('.row-item').removeAttr('checked');
        $sampleDataRow.find('.row-item').removeAttr('checked');
    }
};

/**
 * Handle Check/Uncheck a process column
 * @param {HTMLInputElement} ele
 * @param {boolean} isRegisteredProc
 */
const handleChangeProcessColumn = (ele, isRegisteredProc) => {
    const $ele = $(ele);
    const $processConfigTableBody = $ele.closest('tbody');
    const $processColumnsSampleDataTableBody = $ele
        .closest('div.proc-config-content')
        .find('table[name="processColumnsTableSampleData"] tbody');
    const $targetRow = $ele.closest('tr');
    const $sampleDataRow = $processColumnsSampleDataTableBody.find(
        `tr:eq(${$targetRow.index()})`,
    );
    setCheckStatusAttributeForRow($targetRow, $sampleDataRow, ele.checked);

    const isFileNameCol = $ele.attr('data-column_name') === 'FileName';
    if (isFileNameCol) {
        procModalElements.isShowFileName.prop('checked', ele.checked);
    }

    const isDummyDatetimeCol = $ele.attr('data-is-dummy-datetime') === 'true';
    if (!isRegisteredProc && !ele.checked && isDummyDatetimeCol) {
        // remove dummy datetime if uncheck this column
        $targetRow.remove();
        $sampleDataRow.remove();
        prcPreviewData.dummy_datetime_idx = null;
        ProcessConfigSection.sortProcessColumns(
            $processConfigTableBody,
            $processColumnsSampleDataTableBody,
        );
    }

    const checkedTotal = $processConfigTableBody.find(
        'input.col-checkbox:checked[is-show="true"]',
    ).length;
    setTotalCheckedColumns(checkedTotal);
};

const parseDatetimeStr = (datetimeStr, dateOnly = false) => {
    datetimeStr = trimBoth(String(datetimeStr));
    datetimeStr = convertDatetimePreview(datetimeStr);
    const millis = checkDatetimeHasMilliseconds(datetimeStr);
    let formatStr = dateOnly ? DATE_FORMAT : DATE_FORMAT_TZ;
    if (millis && !dateOnly) {
        formatStr = DATE_FORMAT_WITHOUT_TZ + millis + ' Z';
    }
    datetimeStr = moment(datetimeStr);
    if (!datetimeStr._isValid) {
        return '';
    }
    return datetimeStr.format(formatStr);
};

const parseTimeStr = (timeStr) => {
    timeStr = trimBoth(String(timeStr));
    timeStr = convertDatetimePreview(timeStr);
    const millis = checkDatetimeHasMilliseconds(timeStr);
    let formatStr = TIME_FORMAT_TZ;
    if (millis) {
        formatStr = TIME_FORMAT_TZ + millis + ' Z';
    }
    // today
    _today = new Date();
    _today = _today.toISOString().split('T').shift();
    timeStr = moment(_today + ' ' + timeStr);
    if (!timeStr._isValid) {
        return '';
    }
    return timeStr.format(formatStr);
};

const showConfirmSameAndNullValueInColumn = (cols) => {
    if (currentProcItem.data('proc-id')) return;

    let isSame = false;
    let isNull = false;
    cols.forEach((col, i) => {
        if (col.check_same_value.is_same && !col.check_same_value.is_null) {
            isSame = true;
        }

        if (col.check_same_value.is_null) {
            isNull = true;
        }
    });

    if (isNull) {
        $(procModalElements.confirmNullValue).modal('show');
    }

    if (isSame) {
        $(procModalElements.confirmSameValue).modal('show');
    }
};

const parseEUDataTypeInFirstTimeLoad = () => {
    $('.csv-datatype-selection').each((i, el) => {
        if (CfgProcess_CONST.EU_TYPE_VALUE.includes(Number(el.value))) {
            // change to trigger onclick li element
            // $(el).trigger('change');
            $(el)
                .closest('.config-data-type-dropdown')
                .find('li.active')
                .trigger('click');
        }
    });
};

const showConfirmKSepDataModal = (types) => {
    if (
        types.includes(DataTypes.REAL_SEP.value) ||
        types.includes(DataTypes.EU_REAL_SEP.value)
    ) {
        $(procModalElements.confirmDataTypeModal).modal('show');

        // const indexOfColumn = types.indexOf(DataTypes.REAL_SEP.value) !== -1 ? types.indexOf(DataTypes.REAL_SEP.value)
        //     : types.indexOf(DataTypes.EU_REAL_SEP.value);

        // const offsetLeft = procModalElements.latestDataHeader.find(`tr th:eq(${indexOfColumn})`).offset().left;
        // procModalElements.latestDataTable.parents().animate({ scrollLeft: offsetLeft });
    }
};

const validateFixedColumns = () => {
    if (isAddNewMode()) {
        return;
    }
    $(
        `table[name=processColumnsTable] input:checkbox[name="${procModalElements.dateTime}"]`,
    ).each(function disable() {
        $(this).attr('disabled', true);
        if ($(this).is(':checked')) {
            // disable serial as the same row
            $(this)
                .closest('tr')
                .find(`input:checkbox[name="${procModalElements.serial}"]`)
                .attr('disabled', true);
        }
    });
    $(
        `table[name=processColumnsTable] input:checkbox[name="${procModalElements.auto_increment}"]`,
    ).each(function disable() {
        $(this).attr('disabled', true);
    });
};

// validation checkboxes of selected columns
const validateCheckBoxesAll = () => {
    $(
        `table[name=processColumnsTable] input:checkbox[name="${procModalElements.serial}"]`,
    ).each(function validateSerial() {
        $(this).on('change', function f() {
            if ($(this).is(':checked')) {
                // uncheck datetime at the same row
                $(this)
                    .closest('tr')
                    .find(
                        `input:checkbox[name="${procModalElements.dateTime}"]`,
                    )
                    .prop('checked', false);
            }

            // show warning about resetting trace config
            // todo check reset datalink for new GUI
            showResetDataLink($(this));
        });
    });
};

const showResetDataLink = (boxElement) => {
    const currentProcId = procModalElements.procID.val() || null;
    if (!currentProcId) {
        return;
    }
    currentAsLinkIdBox = boxElement;
    $(procModalElements.warningResetDataLinkModal).modal('show');
};

const validateAllCoefs = () => {
    $(
        `#processColumnsTable tr input[name="${procModalElements.coef}"]:not(.text)`,
    ).each(function validate() {
        validateNumericInput($(this));
    });
};

const validateSelectedColumnInput = () => {
    validateCheckBoxesAll();
    handleEnglishNameChange($(procModalElements.systemNameInput));
    addAttributeToElement();
    validateAllCoefs();
    validateFixedColumns();
    updateTableRowNumber(null, $('table[name=selectedColumnsTable]'));
};

const createOptCoefHTML = (operator, coef, isNumeric, checkedAtr = '') => {
    const operators = ['+', '-', '*', '/'];
    let numericOperators = '';
    operators.forEach((opr) => {
        const selected = operator === opr ? ' selected="selected"' : '';
        numericOperators += `<option value="${opr}" ${selected}>${opr}</option>`;
    });
    const selected = operator === 'regex' ? ' selected="selected"' : '';
    const textOperators = `<option value="regex" ${selected}>${i18n.validLike}</option>`;
    let coefHTML = `<input name="coef" class="form-control row-item" type="text" value="${coef || ''}" ${checkedAtr}>`;
    if (!isNumeric) {
        coefHTML = `<input name="coef" class="form-control text row-item" type="text" value="${coef || ''}" ${checkedAtr}>`;
    }
    return [numericOperators, textOperators, coefHTML];
};

// tick checkbox event

const selectTargetCol = (col, doValidate = true) => {
    if (col.checked) {
        // add new record
        const isJPLocale = docCookies.isJaLocale();
        const colDataType = col.getAttribute('data-type');
        const romaji = col.getAttribute('data-romaji');
        const isDummyDatetime =
            col.getAttribute('data-is-dummy-datetime') === 'true';
        const nameJp =
            col.getAttribute('data-name-jp') || isJPLocale ? col.value : '';
        const nameLocal =
            col.getAttribute('data-name-local') || !isJPLocale ? col.value : '';
        const isDatetime =
            !setDatetimeSelected && DataTypes.DATETIME.name === colDataType;
        const colConfig = {
            is_get_date: isDatetime,
            is_serial_no: false,
            is_auto_increment: false,
            data_type: colDataType,
            column_name: col.value,
            name_en: romaji,
            name_jp: nameJp,
            name_local: nameLocal,
            // name: col.value,
            is_dummy_datetime: isDummyDatetime,
        };
        if (isDatetime) {
            setDatetimeSelected = true;
        }
        procModalElements.seletedColumnsBody.append(
            genColConfigHTML(colConfig),
        );

        if (doValidate) {
            validateSelectedColumnInput();
        }
    } else {
        // remove record
        $(`#selectedColumnsTable tr[uid="${col.value}"]`).remove();
        const remainDatetimeCols = $('tr[name=selectedColumn]').find(
            'input[data-type=DATETIME]',
        ).length;
        // reset datetime col selection
        setDatetimeSelected = remainDatetimeCols > 0;
        updateTableRowNumber(null, $('table[name=selectedColumnsTable]'));
    }

    // update selectAll input
    if (doValidate) {
        updateSelectAllCheckbox();
    }
};

const autoSelectColumnEvent = (selectAllElement) => {
    const isAllChecked = selectAllElement.checked;

    // check selectAll input
    if (isAllChecked) {
        changeSelectionCheckbox();
    }

    $('.col-checkbox').each(function f() {
        const isColChecked = $(this).prop('checked');
        const isDisabledInput = $(this).attr('disabled') !== undefined;
        const isNull = $(this).data('isnull');
        if (!isNull && (!isColChecked || !isAllChecked) && !isDisabledInput) {
            // select null cols only and select only once
            $(this).prop('checked', isAllChecked);
            selectTargetCol($(this)[0], (doValidate = false));
        } else if (isNull && isColChecked && !isDisabledInput) {
            // if null col is selected -> unselected
            $(this).prop('checked', false);
            selectTargetCol($(this)[0], (doValidate = false));
        }
    });

    // validate after selecting all to save time
    validateSelectedColumnInput();
};
const selectAllColumnEvent = (selectAllElement) => {
    const isAllChecked = selectAllElement.checked;
    changeSelectionCheckbox((autoSelect = false), (selectAll = true));
    // if (isAllChecked) {
    //     changeSelectionCheckbox(autoSelect = false, selectAll = true);
    // }
    $('.col-checkbox').each(function f() {
        const isDisabledInput = $(this).attr('disabled') !== undefined;
        const isColChecked = $(this).prop('checked');
        if (!isDisabledInput && (!isColChecked || !isAllChecked)) {
            // select null cols only and select only once
            $(this).prop('checked', isAllChecked);
            selectTargetCol($(this)[0], (doValidate = false));
        }
    });

    // validate after selecting all to save time
    validateSelectedColumnInput();
};

const changeSelectionCheckbox = (autoSelect = true, selectAll = false) => {
    $(procModalElements.autoSelect).prop('checked', autoSelect);
    $(procModalElements.selectAllSensor).prop('checked', selectAll);
};

const updateSelectAllCheckbox = () => {
    let selectAll = true;
    let autoSelect = true;

    if (renderedCols) {
        // update select all check box based on current selected columns
        $('.col-checkbox').each(function f() {
            const isColChecked = $(this).prop('checked');
            const isNull = $(this).data('isnull');
            if (!isColChecked) {
                selectAll = false;
            }
            if ((isNull && isColChecked) || (!isNull && !isColChecked)) {
                autoSelect = false;
            }
        });
        changeSelectionCheckbox(autoSelect, selectAll);
    }
};

// update latest records table by yaml data
const updateLatestDataCheckbox = () => {
    const getHtmlEleFunc = genJsonfromHTML(
        procModalElements.seletedColumnsBody,
        'selects',
        true,
    );
    const selectJson = getHtmlEleFunc(
        procModalElements.columnName,
        (ele) => ele.value,
    );
    const SELECT_ROOT = Object.keys(selectJson)[0];
    const datetimeColSelected = $('tr[name=selectedColumn]').find(
        'input[data-type=DATETIME]',
    );
    const datetimeColName = datetimeColSelected.length
        ? datetimeColSelected[0].value
        : undefined;
    if (
        procModalElements.columnName in selectJson[SELECT_ROOT] &&
        selectJson[SELECT_ROOT][procModalElements.columnName]
    ) {
        for (const colname of selectJson[SELECT_ROOT][
            procModalElements.columnName
        ]) {
            const isDisabledCol = colname === datetimeColName;
            const currentInput = $(
                `input[value="${colname}"][data-shown-name!='1']`,
            );
            currentInput.prop('checked', true);
            if (isDisabledCol) {
                currentInput.prop('disabled', true);
                // currentInput.attr('data-is-disabled', true);
            }
        }
    }
};

const handleCheckAutoAndAllSelect = (el, autoSelect = false) => {
    const isChecked = el.checked;
    if (isChecked && autoSelect) {
        changeSelectionCheckbox();
    }

    if (isChecked && !autoSelect) {
        changeSelectionCheckbox(false, true);
    }

    let checkCols = null;

    if (isChecked) {
        if (autoSelect) {
            checkCols = $(
                'table[name=processColumnsTable] .col-checkbox:not(:checked):not(:disabled):not([data-isnull=true])',
            );
        } else {
            checkCols = $(
                'table[name=processColumnsTable] .col-checkbox:not(:checked):not(:disabled)',
            );
        }
    } else {
        if (autoSelect) {
            checkCols = $(
                'table[name=processColumnsTable] .col-checkbox:checked:not(:disabled):not([data-isnull=true])',
            );
        } else {
            checkCols = $(
                'table[name=processColumnsTable] .col-checkbox:checked:not(:disabled)',
            );
        }
    }

    [...checkCols].forEach((col) => {
        const colCfg = getColCfgInCheckbox(col);
        if (colCfg.is_get_date) {
            setDatetimeSelected = true;
        }
    });
    checkCols.prop('checked', isChecked).trigger('change');
    if (!isChecked) {
        const remainDatetimeCols = $(
            'span[name=dataType][value=DATETIME][checked]',
        ).length;
        // reset datetime col selection
        setDatetimeSelected = remainDatetimeCols > 0;
    }
    // validate after selecting all to save time
    validateSelectedColumnInput();
};

const handleShowFileNameColumn = (el) => {
    const isChecked = el.checked;
    const isDisabled = el.disabled;
    let fileNameColumn = null;

    fileNameColumn = $(
        `.col-checkbox:not(:disabled)[data-column_name="FileName"]`,
    );
    if (isDisabled) {
        fileNameColumn.prop('disabled', true);
    }
    fileNameColumn.prop('checked', isChecked).trigger('change');
};

const getColCfgInCheckbox = (col) => {
    const colDataType = col.getAttribute('data-type');
    const romaji = col.getAttribute('data-romaji');
    const isDummyDatetime =
        col.getAttribute('data-is-dummy-datetime') === 'true';
    const nameJp =
        col.getAttribute('data-name-jp') || isJPLocale ? col.value : '';
    const nameLocal =
        col.getAttribute('data-name-local') || !isJPLocale ? col.value : '';
    const isDatetime =
        !setDatetimeSelected && DataTypes.DATETIME.name === colDataType;
    const colConfig = {
        is_get_date: isDatetime,
        is_serial_no: false,
        is_auto_increment: false,
        data_type: colDataType,
        column_name: col.value,
        name_en: romaji,
        name_jp: nameJp,
        name_local: nameLocal,
        // name: col.value,
        is_dummy_datetime: isDummyDatetime,
    };

    return colConfig;
};

const preventSelectAll = (preventFlag = false) => {
    // change render flag
    renderedCols = !preventFlag;
    $(procModalElements.selectAllSensor).prop('disabled', preventFlag);
    $(procModalElements.autoSelect).prop('disabled', preventFlag);
};

const updateCurrentDatasource = () => {
    const currentShownTableName =
        getSelectedOptionOfSelect(procModalElements.tables).val() || null;
    const currentShownDataSouce = procModalElements.databases.val() || null;
    // re-assign datasource id and table of process
    if (currentShownDataSouce) {
        currentProcData.ds_id = Number(currentShownDataSouce);
    }
    if (currentShownTableName) {
        currentProcData.table_name = currentShownTableName;
    }
};

const showLatestRecordsFromPrc = (json) => {
    dataGroupType = json.data_group_type;
    const dummyDatetimeIdx = json.dummy_datetime_idx;
    // genColumnWithCheckbox(json.cols, json.rows, dummyDatetimeIdx);
    generateProcessList(json.cols, json.rows, dummyDatetimeIdx);
    preventSelectAll(renderedCols);
    if (clearSelectedColumnBody) {
        procModalElements.seletedColumnsBody.empty();
    } else {
        // update column checkboxes from selected columns
        updateLatestDataCheckbox();
    }

    // update changed datasource
    updateCurrentDatasource();

    // update select all check box after update column checkboxes
    updateSelectAllCheckbox();

    // bind select columns with context menu
    bindSelectColumnsHandler();

    // update columns from process
    currentProcColumns = json.cols;
    if (!procModalCurrentProcId) {
        // remove selected table
        // $(`table[name=${procModalElements.processColumnsTableId}] tbody`).empty();
        // auto click auto select
        $(procModalElements.autoSelect).prop('checked', true).change();
        isClickPreview = false;
    }
    // handle file name column
    handleShowFileNameColumn(procModalElements.isShowFileName[0]);
};

const addDummyDatetimePrc = (addCol = true) => {
    if (!addCol) {
        // update content of csv
        prcPreviewData.cols.shift();
        prcPreviewData.dummy_datetime_idx = null;
    }
    showLatestRecordsFromPrc(prcPreviewData);
    // todo: disable check/uncheck to prevent remove datetime column
    // if (!addCol && dummyCol.length) {
    //     dummyCol.remove();
    // }
    if (addCol) {
        // clear old error message
        $(procModalElements.alertErrMsgContent).html('');
        $(procModalElements.alertProcessNameErrorMsg).hide();
        showToastrMsgNoCTCol(prcPreviewData.has_ct_col);
    } else {
        // clear old alert-top-fixed
        $('.alert-top-fixed').hide();
        displayRegisterMessage(procModalElements.alertProcessNameErrorMsg, {
            message: procModali18n.noDatetimeCol,
            is_error: true,
        });
    }
    // disable submit button
    updateBtnStyleWithValidation(
        procModalElements.createOrUpdateProcCfgBtn,
        addCol,
    );
};

// get latestRecords
const showLatestRecords = (formData, clearSelectedColumnBody = true) => {
    loading.css('z-index', 9999);
    loading.show();
    ajaxWithLog({
        url: '/ap/api/setting/show_latest_records',
        data: formData,
        dataType: 'json',
        type: 'POST',
        contentType: false,
        processData: false,
        success: (json) => {
            json = jsonParse(json);
            loading.hide();
            if (json.cols_duplicated) {
                showToastrMsg(i18nCommon.colsDuplicated);
            }
            prcPreviewData = json;
            showToastrMsgFailLimit(json);
            const isEditPrc = currentProcItem.data('proc-id') !== undefined;
            // show gen dummy datetime col for new proces only
            if (!isEditPrc && !json.has_ct_col && !json.is_rdb) {
                showDummyDatetimeModal(json, true);
            } else {
                showLatestRecordsFromPrc(json);
                // checkDateAndTimeChecked();
                updateBtnStyleWithValidation(
                    procModalElements.createOrUpdateProcCfgBtn,
                );
            }

            allProcessColumns = json
                ? json.cols.map((col) => col.column_name)
                : [];

            initDatetimeFormatCheckboxAndInput();
        },
        error: (e) => {
            loading.hide();
            console.log('error', e);
        },
    });
};

const clearWarning = () => {
    $(procModalElements.alertMessage).css('display', 'none');
    $('.column-name-invalid').removeClass('column-name-invalid');
};

// validate coefs
const validateCoefOnSave = (coefs, operators) => {
    if (isEmpty(coefs) && isEmpty(operators)) return [];

    const errorMsgs = new Set();
    const coefArray = [].concat(coefs);
    const operatorArray = [].concat(operators);

    if (coefArray && coefArray.includes(0)) {
        errorMsgs.add($(procModali18n.noZeroCoef).text() || '');
    }

    for (i = 0; i < operatorArray.length; i++) {
        if (
            coefs[i] === '' &&
            ['+', '-', '*', '/', 'regex'].includes(operatorArray[i])
        ) {
            errorMsgs.add($(procModali18n.emptyCoef).text() || '');
        }
        if (
            coefs[i] !== '' &&
            !['+', '-', '*', '/', 'regex'].includes(operatorArray[i])
        ) {
            errorMsgs.add($(procModali18n.needOperator).text() || '');
        }
    }
    errorMsgs.delete('');
    return Array.from(errorMsgs) || [];
};

const handleEnglishNameChange = (ele) => {
    ['keyup', 'change'].forEach((event) => {
        ele.off(event).on(event, (e) => {
            // replace characters which isnot alphabet
            e.currentTarget.value = e.currentTarget.value.replace(
                /[^\w-]+/g,
                '',
            );
        });
    });
};

const handleEmptySystemNameJP = async (ele, targetEle) => {
    const systemNameInput = $(ele)
        .parent()
        .siblings()
        .children(`input[name=${targetEle}]`);
    if (!$(systemNameInput).val()) {
        $(systemNameInput).val(await convertEnglishRomaji([$(ele).val()]));
        $(systemNameInput)[0].dataset.originalValue = $(systemNameInput).val();
    }
};

const handleEmptySystemName = async (ele, targetEle) => {
    const systemNameInput = $(ele)
        .parent()
        .siblings()
        .children(`input[name=${targetEle}]`);
    if (!$(systemNameInput).val()) {
        const removed = await convertNonAscii([$(ele).val()]);
        $(systemNameInput).val(removed);
    }
};

const englishAndMasterNameValidator = (
    englishNames = [],
    japaneseNames = [],
    localNames = [],
) => {
    if (isEmpty(englishNames)) return [];

    const nameErrors = new Set();
    const isEmptyEnglishName = []
        .concat(englishNames)
        .some((name) => isEmpty(name));
    if (isEmptyEnglishName) {
        nameErrors.add($(procModali18n.noEnglishName).text() || '');
    }

    // const isEmptyJpName = [].concat(japaneseNames).some(name => isEmpty(name)); no required
    // if (isEmptyJpName) {
    //     nameErrors.add($(procModali18n.noMasterName).text() || '');
    // }
    if (isArrayDuplicated(englishNames.filter((name) => !isEmpty(name)))) {
        nameErrors.add($(procModali18n.duplicatedSystemName).text() || '');
        // add red border to duplicated input
        showBorderRedForDuplicatedInput('systemName', englishNames);
    }

    if (isArrayDuplicated(japaneseNames.filter((name) => !isEmpty(name)))) {
        // duplicated Japanese name checking
        nameErrors.add($(procModali18n.duplicatedJapaneseName).text() || '');
        // add red border to duplicated input
        showBorderRedForDuplicatedInput('japaneseName', japaneseNames);
    }

    if (isArrayDuplicated(localNames.filter((name) => !isEmpty(name)))) {
        // duplicated local name checking
        nameErrors.add($(procModali18n.duplicatedLocalName).text() || '');
        // add red border to duplicated input
        showBorderRedForDuplicatedInput('localName', localNames);
    }

    nameErrors.delete('');
    return Array.from(nameErrors) || [];
};

const getDuplicatedValueInArray = (array) => {
    const duplicateValues = array.filter(
        (item, index) => array.indexOf(item) !== index,
    );
    return duplicateValues;
};

const showBorderRedForDuplicatedInput = (inputName, values) => {
    const duplicateValues = getDuplicatedValueInArray(values);
    const inputs = procModalElements.processColumnsTableBody.find(
        `input[name=${inputName}][checked=checked]`,
    );
    inputs.each((i, el) => {
        if ($(el).val() && duplicateValues.includes($(el).val())) {
            $(el).addClass('column-name-invalid');
        }
    });
};

const checkDuplicateProcessName = (
    attr = 'data-name-en',
    isShowMsg = true,
    errorMsg = $(procModalElements.msgProcNameAlreadyExist).text(),
) => {
    if (!checkOnFocus) return;
    // get current list of (process-mastername)
    const existingProcIdMasterNames = {};
    $('#tblProcConfig tr').each(function f() {
        const procId = $(this).data('proc-id');
        const rowId = $(this).attr('id');
        const currentProcId = currentProcItem.data('proc-id');
        if (Number(currentProcId) !== Number(procId)) {
            if (rowId) {
                const masterName =
                    $(`#${rowId} input[name=processName]`).attr(attr) || '';
                existingProcIdMasterNames[`${procId}`] = masterName;
            }
        }
    });

    let inputEl = procModalElements.proc;
    if (attr === 'data-name-jp') {
        inputEl = procModalElements.procJapaneseName;
    }
    if (attr === 'data-name-local') {
        inputEl = procModalElements.procLocalName;
    }
    // check for duplication
    const beingEditedProcName = inputEl.val();
    const existingMasterNames = Object.values(existingProcIdMasterNames);
    if (
        beingEditedProcName &&
        existingMasterNames.includes(beingEditedProcName)
    ) {
        if (isShowMsg) {
            // show warning message
            displayRegisterMessage(procModalElements.alertProcessNameErrorMsg, {
                message: errorMsg,
                is_error: true,
            });
        }
        inputEl.addClass('column-name-invalid');
        return true;
    } else {
        if (isShowMsg) {
            $(procModalElements.alertProcessNameErrorMsg).css(
                'display',
                'none',
            );
        }
        inputEl.removeClass('column-name-invalid');
    }
    return false;
};

const isDuplicatedProcessName = () => {
    const isNameEnDup = checkDuplicateProcessName('data-name-en', false);
    const isNameJpDup = checkDuplicateProcessName('data-name-jp', false);
    const isNameLocalDup = checkDuplicateProcessName('data-name-local', false);

    if (isNameEnDup || isNameJpDup || isNameLocalDup) {
        // show msg
        displayRegisterMessage(procModalElements.alertProcessNameErrorMsg, {
            message: $(procModalElements.msgProcNameAlreadyExist).text(),
            is_error: true,
        });
        return true;
    }

    return false;
};

const scrollTopProcModal = () => {
    $(procModalElements.procModal).animate({ scrollTop: 0 }, 'fast');
};

const scrollTopDataTableModal = () => {
    $(procModalElements.dataTableSettingModal).animate(
        { scrollTop: 0 },
        'fast',
    );
};

const validateProcName = () => {
    let notBlank = true;
    // get current list of (process-mastername)
    const masterName = procModalElements.proc.val();
    if (!masterName.trim()) {
        // show warning message
        displayRegisterMessage(procModalElements.alertProcessNameErrorMsg, {
            message: $(procModalElements.msgProcNameBlank).text(),
            is_error: true,
        });
        // scroll to top
        scrollTopProcModal();

        notBlank = false;
    } else {
        $(procModalElements.alertProcessNameErrorMsg).css('display', 'none');
    }

    const notDuplicated = !isDuplicatedProcessName();

    return notBlank && notDuplicated;
};

const autoFillShownNameToModal = () => {
    $('#processColumnsTable tbody tr').each(function f() {
        const shownName = $(this)
            .find(`input[name="${procModalElements.shownName}"]`)
            .val();
        const columnName = $(this)
            .find(`input[name="${procModalElements.columnName}"]`)
            .val();
        if (isEmpty(shownName)) {
            $(this)
                .find(`input[name="${procModalElements.shownName}"]`)
                .val(columnName);
        }
    });
};

/**
 *
 * @param getSelectedOnly
 * @param {?HTMLBodyElement} tableBodyElement - a table's body HTML object
 * @return {*}
 */
const getSelectedColumnsAsJson = (
    getSelectedOnly = true,
    tableBodyElement = null,
) => {
    // get Selected columns
    const getHtmlEleFunc = genJsonfromHTML(
        tableBodyElement
            ? $(tableBodyElement)
            : procModalElements.processColumnsTableBody,
        'selects',
        true,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('value'),
        procModalElements.dataType,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_serial_no'),
        procModalElements.serial,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_auto_increment'),
        procModalElements.auto_increment,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_get_date'),
        procModalElements.dateTime,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_main_date'),
        procModalElements.mainDate,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_main_time'),
        procModalElements.mainTime,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_main_serial_no'),
        procModalElements.mainSerial,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_line_name'),
        procModalElements.lineName,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_line_no'),
        procModalElements.lineNo,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_eq_name'),
        procModalElements.equiptName,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_eq_no'),
        procModalElements.equiptNo,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_part_name'),
        procModalElements.partName,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_part_no'),
        procModalElements.partNo,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_st_no'),
        procModalElements.partNo,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('column_type'),
        procModalElements.columnType,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('is_int_cat'),
        procModalElements.intCat,
    );
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('data-raw-data-type'),
        'rawDataType',
    );
    getHtmlEleFunc(procModalElements.columnName, (ele) => ele.value);
    getHtmlEleFunc(procModalElements.columnRawName, (ele) => ele.value);
    getHtmlEleFunc(procModalElements.systemName);
    getHtmlEleFunc(
        procModalElements.systemName,
        (ele) => ele.getAttribute('old-value'),
        'old_system_name',
    );
    getHtmlEleFunc(procModalElements.japaneseName);
    getHtmlEleFunc(procModalElements.localName);
    getHtmlEleFunc(procModalElements.unit);
    getHtmlEleFunc(procModalElements.operator);
    getHtmlEleFunc(procModalElements.isDummyDatetime);
    getHtmlEleFunc(
        procModalElements.columnName,
        (ele) => ele.checked,
        'isChecked',
    );
    getHtmlEleFunc(procModalElements.japaneseName);
    getHtmlEleFunc(
        procModalElements.japaneseName,
        (ele) => ele.getAttribute('old-value'),
        'old_name_jp',
    );
    getHtmlEleFunc(procModalElements.format);

    const selectJson = getHtmlEleFunc(procModalElements.coef);

    return selectJson;
};

const getHorizontalDataAsJson = () => {
    // get Selected columns
    const getHtmlEleFunc = genJsonfromTbl(
        procModalElements.processColumnsTable,
        'selects',
        true,
    );
    const prependSelector = 'tr:not(.d-none)';
    getHtmlEleFunc(procModalElements.columnName);
    getHtmlEleFunc(procModalElements.columnRawName);
    getHtmlEleFunc(procModalElements.systemName);
    getHtmlEleFunc(procModalElements.japaneseName);
    getHtmlEleFunc(procModalElements.localName);
    getHtmlEleFunc(
        procModalElements.dataType,
        (ele) => ele.getAttribute('value'),
        procModalElements.dataType,
    );
    // getHtmlEleFunc(procModalElements.format, e => e.value, prependSelector);
    const selectJson = getHtmlEleFunc(procModalElements.format);

    return selectJson;
};

// VALUE IS DIFFERENT IN BRIDGE STATION
const mappingDataGroupType = {
    is_get_date: 'DATA_TIME',
    is_serial_no: 'DATA_SERIAL',
    is_main_serial_no: 'DATA_SERIAL',
    is_auto_increment: 'AUTO_INCREMENTAL',
    is_line_name: 'LINE_NAME',
    is_line_no: 'LINE_NO',
    is_eq_name: 'EQ_NAME',
    is_eq_no: 'EQ_NO',
    is_part_name: 'PART_NAME',
    is_part_no: 'PART_NO',
    is_st_no: 'ST_NO',
    is_int_cat: 'INT_CATE',
    is_main_date: 'MAIN_DATE',
    is_main_time: 'MAIN_TIME',
};

const procColumnsData = (selectedJson, getAll = false) => {
    const columnsData = [];
    if (selectedJson.selects.columnName.length) {
        selectedJson.selects.columnName.forEach((v, k) => {
            const isChecked = selectedJson.selects.isChecked[k];
            const dataType = selectedJson.selects.dataType[k];
            const localName = selectedJson.selects.localName[k];
            const japaneseName = selectedJson.selects.japaneseName[k];
            const unit = selectedJson.selects.unit[k];
            // get old value to set after sort columns
            const oldJPName = selectedJson.selects['old_name_jp'][k];
            const oldSystemName = selectedJson.selects['old_system_name'][k];
            const columnType = Number(
                selectedJson.selects[procModalElements.columnType][k],
            );
            const rawDataType = selectedJson.selects.rawDataType[k];
            const format = selectedJson.selects.format[k];
            const column = {
                column_name: v,
                column_raw_name: selectedJson.selects.columnRawName[k],
                name_en: selectedJson.selects.systemName[k],
                // add system_name column
                data_type: dataType,
                // operator: selectedJson.selects.operator[k], // BS not show
                // coef: selectedJson.selects.coef[k], // BS not show
                column_type: columnType || 99, // data group type
                is_serial_no:
                    selectedJson.selects.serial[k] === 'true' ||
                    selectedJson.selects.mainSerial[k] === 'true',
                is_get_date: selectedJson.selects.dateTime[k] === 'true',
                is_auto_increment:
                    selectedJson.selects.auto_increment[k] === 'true',
                is_dummy_datetime:
                    selectedJson.selects.isDummyDatetime[k] === 'true',
                order: CfgProcess_CONST.CATEGORY_TYPES.includes(dataType)
                    ? 1
                    : 0,
                name_jp: japaneseName || null,
                name_local: localName || null,
                raw_data_type: rawDataType || null,
                format: format || null,
                unit: unit || null,
            };

            if (isChecked && !getAll) {
                columnsData.push(column);
            }
            if (getAll) {
                column.is_checked = isChecked;
                column.old_name_jp = oldJPName;
                column.old_system_name = oldSystemName;
                columnsData.push(column);
            }
        });
    }
    return columnsData;
};

const getSelectedOptionOfSelect = (selectEl) => {
    const selected1 = selectEl.find('option:selected');
    const selected2 = selectEl.find('option[selected=selected]');
    if (selected1.length > 0) {
        return selected1;
    }

    if (selected2.length > 0) {
        return selected2;
    }

    return selected1;
};

const collectProcCfgData = (columnDataRaws, getAllCol = false) => {
    const procID = procModalElements.procID.val() || null;
    const procEnName = procModalElements.proc.val();
    const procLocalName = procModalElements.procLocalName.val() || null;
    const procJapaneseName = procModalElements.procJapaneseName.val() || null;
    // const dataSourceId = getSelectedOptionOfSelect(procModalElements.databases).val() || '';
    const tableName =
        getSelectedOptionOfSelect(procModalElements.tables).val() || '';
    const comment = procModalElements.comment.val() || null;
    // preview data & data-type predict by file name
    // const fileName = procModalElements.fileName.val() || null;
    const procColumns = procColumnsData(columnDataRaws, getAllCol);

    // get uncheck column = all col - uncheck col
    const checkedProcessColumn = procColumns.map((col) => col.column_name);
    const unusedColumns = allProcessColumns.filter(
        (colName) => !checkedProcessColumn.includes(colName),
    );

    return [
        {
            id: procID,
            name_en: procEnName,
            name: procEnName,
            name_jp: procJapaneseName,
            name_local: procLocalName,
            comment,
            columns: procColumns,
        },
        {
            id: procID,
            name_en: procEnName,
            name: procEnName,
            name_jp: procJapaneseName,
            name_local: procLocalName,
            comment,
            columns: unusedColumns,
        },
    ];
};

const collectProcCfgData_New = () => {
    const procID = procModalElements.procID.val() || null;
    const procEnName = procModalElements.proc.val();
    const procLocalName = procModalElements.procLocalName.val() || null;
    const procJapaneseName = procModalElements.procJapaneseName.val() || null;
    const comment = procModalElements.comment.val();
    const isShowFileName = procModalElements.isShowFileName.not(':disabled')
        ? procModalElements.isShowFileName.is(':checked')
        : null;
    const columns = [];
    const uncheckColumns = [];
    procModalElements.processColumnsTableBody
        .children()
        .each((rowIndex, row) => {
            let rowDict = {};
            let isChecked = true;
            $(row)
                .children()
                .each((columnIndex, column) => {
                    const $column = $(column);
                    const columnTitle = $column.attr('title');
                    if (
                        [undefined, 'undefined', 'index'].includes(columnTitle)
                    ) {
                        return true;
                    }

                    const $input = $column.find(
                        'input:first-child, .config-data-type-dropdown>button>span',
                    );
                    let value = null;
                    if ($input.length === 0) {
                        // In case of index column or undefined column without value
                        value = $column.text();
                    } else {
                        // In case of data column
                        if ($input.is(':input')) {
                            if ($input.attr('type') === 'checkbox') {
                                value = $input.is(':checked');
                                if (columnTitle === 'order') {
                                    value = value ? 1 : 0;
                                }
                            } else {
                                value = $input.val();
                            }
                        } else {
                            // In case of multi-level-dropdown
                            value = $input.attr('data-raw-data-type');
                            rowDict['is_get_date'] =
                                $input
                                    .attr('is_get_date')
                                    ?.trim()
                                    ?.toLowerCase() === String(true);
                        }
                    }

                    if (columnTitle === 'is_checked') {
                        const rowData = $input.data();
                        delete rowData['type']; // no need this attribute due to already have data_type
                        delete rowData['name']; // no need this attribute to avoid sending redundant data to backend schema
                        if (!value) {
                            isChecked = false;
                        }

                        rowDict = {
                            ...rowDict,
                            ...rowData,
                        };
                    } else {
                        rowDict[columnTitle] = value;
                    }
                });

            // Convert to string for id because sometime it is int, sometime string
            rowDict['id'] = String(rowDict['id']);
            rowDict['order'] = rowIndex;
            if (isChecked || rowDict['column_name'] === 'FileName') {
                columns.push(rowDict);
            } else {
                uncheckColumns.push(rowDict);
            }
        });

    const datetimeFormat =
        procModalElements.procDateTimeFormatInput.val().trim() || null;

    return [
        {
            id: procID,
            name_en: procEnName,
            name: procEnName,
            name_jp: procJapaneseName,
            name_local: procLocalName,
            comment,
            is_show_file_name: isShowFileName,
            datetime_format: datetimeFormat,
            columns,
        },
        {
            id: procID,
            name_en: procEnName,
            name: procEnName,
            name_jp: procJapaneseName,
            name_local: procLocalName,
            comment,
            is_show_file_name: isShowFileName,
            datetime_format: datetimeFormat,
            columns: uncheckColumns,
        },
    ];
};

const saveProcCfg = (selectedJson, importData = true) => {
    clearWarning();
    const [procCfgData, uncheckProcCfgData] = collectProcCfgData_New();
    const data = {
        proc_config: procCfgData,
        uncheck_proc_config: uncheckProcCfgData,
        import_data: importData,
    };

    const handleErrorCases = (res) => {
        displayRegisterMessage(procModalElements.alertProcessNameErrorMsg, {
            message: res.message,
            is_error: true,
            is_warning: false,
        });

        if (res['errorType'] === 'CastError') {
            if (res.data) {
                // No need to show error message in process config modal because it will be shown in failed cast
                // data modal
                $(procModalElements.alertProcessNameErrorMsg).css(
                    'display',
                    'none',
                );

                // Show modal and list down all columns & data that cannot be converted
                const failedCastDataModal = new FailedCastDataModal();
                failedCastDataModal.init(res.data, res.message);
                failedCastDataModal.show();
            }

            // In case error be thrown when casting due to INTERNAL SERVER ERROR, revert to previous data type and
            // do not show distinct fail values
            revertDataTypeForErrorColumns();
        }
    };

    loadingShowImmediately();
    ajaxWithLog({
        url: 'api/setting/proc_config',
        type: 'POST',
        data: JSON.stringify(data),
        dataType: 'json',
        contentType: 'application/json',
    })
        .done((res) => {
            res = jsonParse(res);
            // sync Vis network
            reloadTraceConfigFromDB();

            // update GUI
            if (res.status !== HTTP_RESPONSE_CODE_500) {
                procModalElements.procModal.modal('hide');
                if (!currentProcItem.length) {
                    addProcToTable(res.data, true);
                } else {
                    $(currentProcItem)
                        .find('input[name="processName"]')
                        .val(res.data.shown_name)
                        .prop('disabled', true);
                    $(currentProcItem)
                        .find('input[name="processName"]')
                        .attr('data-name-en', res.data.name);
                    $(currentProcItem)
                        .find('select[name="datasourceName"]')
                        .val(res.data.data_source_name)
                        .prop('disabled', true);
                    $(currentProcItem)
                        .find('select[name="datatableName"]')
                        .val(res.data.data_table_name)
                        .prop('disabled', true);
                    $(currentProcItem)
                        .find('textarea[name="cfgProcComment"]')
                        .val(res.data.comment)
                        .prop('disabled', true);
                    $(currentProcItem).attr('id', `proc_${res.data.id}`);
                    $(currentProcItem).attr('data-proc-id', res.data.id);
                }
            } else {
                handleErrorCases(res);
            }
        })
        .fail((res) => {
            res = res['responseJSON'];
            handleErrorCases(res);
        })
        .always(() => {
            loadingHide();
        });

    $(`#tblProcConfig #${procModalCurrentProcId}`).data('type', '');
};

/**
 * Do revert to original Data type of changed columns
 */
function revertDataTypeForErrorColumns() {
    procModalElements.processColumnsTableBody
        .find(`td[title="raw_data_type"] div.multi-level-dropdown`)
        .each((i, e) => {
            const originRawDataType = $(e)
                .find('button.dropdown-toggle span')
                .data('origin-raw-data-type');
            const originRawDataTypeText =
                DataTypeDropdown_Controller.RawDataTypeTitle[originRawDataType];
            const rawDataType = $(e)
                .find('button.dropdown-toggle span')
                .data('raw-data-type');
            const idx = Number($(e).closest('tr')[0].rowIndex) - 1;
            if (originRawDataType !== rawDataType) {
                setTimeout(() => {
                    DataTypeDropdown_Controller.changeDataType(
                        e,
                        originRawDataType,
                        originRawDataTypeText,
                        '',
                        DataTypeDropdown_Controller.getOptionByAttrKey(
                            e,
                            originRawDataType,
                            '',
                        )[0],
                    );
                }, 1);
            }
        });
}

const getCheckedRowValues = () => {
    const selectJson = getSelectedColumnsAsJson();

    const SELECT_ROOT = Object.keys(selectJson)[0];
    const operators = [];
    const coefsRaw = [];
    const systemNames = [];
    const japaneseNames = [];
    const localNames = [];

    if (selectJson[SELECT_ROOT].columnName.length) {
        selectJson[SELECT_ROOT].columnName.forEach((v, k) => {
            const isChecked = selectJson[SELECT_ROOT].isChecked[k];
            if (isChecked) {
                operators.push('');
                // operators.push(selectJson[SELECT_ROOT][procModalElements.operator][k]);
                coefsRaw.push('');
                // coefsRaw.push(selectJson[SELECT_ROOT][procModalElements.coef][k]);
                systemNames.push(
                    selectJson[SELECT_ROOT][procModalElements.systemName][k],
                );
                japaneseNames.push(
                    selectJson[SELECT_ROOT][procModalElements.japaneseName][k],
                );
                localNames.push(
                    selectJson[SELECT_ROOT][procModalElements.localName][k],
                );
            }
        });
    }

    return [systemNames, japaneseNames, localNames, operators, coefsRaw];
};

const runRegisterProcConfigFlow = (edit = false) => {
    clearWarning();

    // validate proc name null
    const validateFlg = validateProcName();
    if (!validateFlg) {
        scrollTopProcModal();
        return;
    }

    // check if date is checked
    const getDateMsgs = [];
    const isMainDateSelected = $(
        'span[name=dataType][checked][is_main_date=true]',
    ).length;
    const isMainTimeSelected = $(
        'span[name=dataType][checked][is_main_time=true]',
    ).length;
    const isMainDateTimeSelected = $(
        'span[name=dataType][value=DATETIME][checked][is_get_date=true]',
    ).length;
    const isValidDatetime =
        isMainDateTimeSelected || (isMainDateSelected && isMainTimeSelected);
    if (!isValidDatetime) {
        getDateMsgs.push($(csvResourceElements.msgErrorNoGetdate).text());
    }

    const [systemNames, japaneseNames, localNames, operators, coefsRaw] =
        getCheckedRowValues();

    let coefs = [];
    if (coefsRaw) {
        coefs = coefsRaw.map((coef) => {
            if (coef === '') {
                return '';
            }
            return Number(coef);
        });
    }

    let nameMsgs = englishAndMasterNameValidator(
        systemNames,
        japaneseNames,
        localNames,
    );
    const coefMsgs = validateCoefOnSave(coefs, operators);

    const missingShownName = procModali18n.noMasterName;
    let hasError = true;
    if (nameMsgs.length && nameMsgs.includes(missingShownName)) {
        const emptyShownameMsg = $(procModali18n.emptyShownName).text();
        const useEnglnameMsg = $(procModali18n.useEnglishName).text();
        // show modal to confirm auto filling shown names
        $(procModalElements.msgContent).text(
            `${emptyShownameMsg}\n${useEnglnameMsg}`,
        );
        $(procModalElements.msgModal).modal('show');
    }
    if (getDateMsgs.length > 0 || nameMsgs.length > 0 || coefMsgs.length > 0) {
        const messageStr = Array.from(
            getDateMsgs.concat(nameMsgs).concat(coefMsgs),
        ).join('<br>');
        displayRegisterMessage(procModalElements.alertProcessNameErrorMsg, {
            message: messageStr,
            is_error: true,
        });
    } else {
        hasError = false;
        // show confirm modal if validation passed
        if (edit) {
            $(procModalElements.confirmReRegisterProcModal).modal('show');
        } else {
            $(procModalElements.confirmImportDataModal).modal('show');
        }
    }

    // scroll to where messages are shown
    // if (hasError) {
    //     const settingContentPos = procModalElements.procSettingContent.offset().top;
    //     const bodyPos = procModalElements.procModalBody.offset().top;
    //     procModalElements.procModal.animate({
    //         scrollTop: settingContentPos - bodyPos,
    //     }, 'slow');
    // }
};

const checkClearColumnsTable = (dsID, tableName) => {
    if (
        ((isEmpty(currentProcData.table_name) && isEmpty(tableName)) ||
            currentProcData.table_name === tableName) &&
        currentProcData.ds_id === dsID
    ) {
        return false;
    }
    return true;
};

const zip = (arr, ...arrs) =>
    arr.map((val, i) => arrs.reduce((a, arr) => [...a, arr[i]], [val]));

const extractSampleData = (sampleData) => {
    // extract sample_data to several columns
    const N_SAMPLE_DATA = 5;
    const samples = [];
    sampleData.forEach((value, i) => {
        var k = i % N_SAMPLE_DATA;
        samples[k] = [...(samples[k] || []), value];
    });
    return samples;
};

const getHorizontalSettingModeRows = () => {
    const selectJson = getHorizontalDataAsJson();
    const SELECT_ROOT = Object.keys(selectJson)[0]; // TODO use common function
    columnNames = selectJson[SELECT_ROOT][procModalElements.columnName] || [''];
    const sourceColNames = selectJson[SELECT_ROOT][
        procModalElements.columnRawName
    ] || [''];
    const englishName = selectJson[SELECT_ROOT][
        procModalElements.systemName
    ] || [''];
    const japaneseName = selectJson[SELECT_ROOT][
        procModalElements.japaneseName
    ] || [''];
    const localName = selectJson[SELECT_ROOT][procModalElements.localName] || [
        '',
    ];
    const dataTypes = selectJson[SELECT_ROOT][procModalElements.dataType] || [
        '',
    ];
    const formats = selectJson[SELECT_ROOT][procModalElements.format] || [''];
    const tmpSampleDatas = selectJson[SELECT_ROOT][
        procModalElements.sampleData
    ] || [''];
    const sampleDatas = extractSampleData(tmpSampleDatas);

    // let rowData = [columnNames, sourceColNames, shownNames, dataTypes, formats];
    let rowData = [
        sourceColNames,
        dataTypes,
        englishName,
        japaneseName,
        localName,
        formats,
    ];
    rowData = [...rowData, ...sampleDatas];
    return zip(...rowData);
};

const convertEnglishRomaji = async (englishNames = []) => {
    const result = await fetchWithLog('api/setting/list_to_english', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ english_names: englishNames }),
    }).then((response) => response.clone().json());

    return result.data || [];
};

const convertNonAscii = async (names = []) => {
    const result = await fetch('api/setting/list_normalize_ascii', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ names: names }),
    }).then((response) => response.clone().json());

    return result['data'] || [];
};

/**
 * Collect All Process Setting Info to text
 * @return {string} - a string contains all process setting info
 */
const collectAllProcSettingInfo = () => {
    const headerText = [
        ...procModalElements.processColumnsTable.find('thead th'),
        ...procModalElements.processColumnsSampleDataTable.find('thead th'),
    ]
        .map((th) => {
            const columnName = th.innerText.trim();
            if (th.getAttribute('colspan') == null) {
                return columnName;
            }
            const quantity = parseInt(th.getAttribute('colspan'), 10);
            return Array(quantity).fill(columnName).join(TAB_CHAR);
        })
        .join(TAB_CHAR);

    const bodyText = _.zip(
        [...procModalElements.processColumnsTableBody.find('tr')],
        [...procModalElements.processColumnsSampleDataTableBody.find('tr')],
    )
        .map((tr) => {
            const [trColumn, trSampleData] = tr;
            if (trColumn === undefined || trSampleData === undefined) {
                return undefined;
            }
            return [...trColumn.children, ...trSampleData.children]
                .map((td) => {
                    const inputEl = td.querySelector('input[type="text"]');
                    if (inputEl != null) {
                        return inputEl.value.trim();
                    }

                    return td.innerText.trim();
                })
                .join(TAB_CHAR);
        })
        .join(NEW_LINE_CHAR);

    return [headerText, bodyText].join(NEW_LINE_CHAR);
};

/**
 * Download All process setting info
 * @param {jQuery} e - JqueryEvent
 */
const downloadAllProcSettingInfo = (e) => {
    const text = collectAllProcSettingInfo();
    const processName = document.getElementById('processName').value.trim();
    const fileName = `${processName}_SampleData.tsv`;
    downloadText(fileName, text);
    showToastrMsg(
        document.getElementById('i18nStartedTSVDownload').textContent,
        MESSAGE_LEVEL.INFO,
    );
};

/**
 * Copy All process setting info
 * @param {jQuery} e - JqueryEvent
 */
const copyAllProcSettingInfo = (e) => {
    const text = collectAllProcSettingInfo();
    navigator.clipboard
        .writeText(text)
        .then(
            showToastCopyToClipboardSuccessful,
            showToastCopyToClipboardFailed,
        );
};

/**
 * parse clipboard string
 * @param {string} copiedText - clipboard string
 * @return {Array.<Array.<string>>}
 */
const transformCopiedTextToTable = (copiedText) => {
    const records = copiedText.replace(/\r\n+$/, '').split('\r\n');
    return records
        .map((rec) => rec.replace(/\t+$/, ''))
        .filter((row) => row !== '')
        .map((row) => row.split('\t'));
};

/**
 * Remove header and validate check
 * @param {Array.<Array.<string>>} table
 * @return {Array.<Array.<string>> | null}
 */
const transformCopiedTable = (table) => {
    if (table.length === 0) {
        return null;
    }

    const headerRow = procModalElements.processColumnsTable
        .find('thead>tr th')
        .toArray()
        .map((el) => el.innerText.trim());

    let newTable = table;

    // should off by one if we have order column
    const hasOrderColumn = table[0].length && table[0][0] === headerRow[0];
    if (hasOrderColumn) {
        newTable = table.map((row) => row.slice(1));
    }

    // user don't copy header rows
    let userHeaderRow = newTable[0];
    let expectedHeaderRow = headerRow.slice(1);
    const hasHeaderRow = _.isEqual(
        userHeaderRow.slice(0, expectedHeaderRow.length),
        expectedHeaderRow,
    );
    if (!hasHeaderRow) {
        showToastrMsg(
            'There is no header in copied text. Please also copy header!',
            MESSAGE_LEVEL.WARN,
        );
        return null;
    }

    return newTable.slice(1);
};

const copyDataToTableAt = (data, table) => (row, col) => {
    const ele = table[row][col];

    if (!ele) {
        return;
    }

    // input
    const input = ele.querySelector('input:enabled:not([type="hidden"])');
    const shouldChangeInput =
        input && !input.disabled && data[row][col] !== undefined;
    if (shouldChangeInput) {
        input.value = stringNormalization(data[row][col]);
    }

    // dropdown
    const dropdown = ele.querySelector('.config-data-type-dropdown');
    if (dropdown) {
        const dropdownButton = dropdown.querySelector('button');
        const shouldChangeDatatype =
            dropdownButton &&
            !dropdownButton.disabled &&
            data[row][col] !== undefined;
        if (!shouldChangeDatatype) {
            return;
        }

        const dropdownItems = dropdown.querySelectorAll(
            '.data-type-selection-box:not(.d-none) .dataTypeSelection:not(.d-none)',
        );
        const newDatatypeElement = Array.from(dropdownItems).find(
            (item) => item.innerText === data[row][col],
        );

        if (newDatatypeElement) {
            DataTypeDropdown_Controller.onClickDataType(newDatatypeElement);
        }
    }
};

/**
 * Paste All process setting info
 * @param {jQuery} e - JqueryEvent
 */
const pasteAllProcSettingInfo = (e) => {
    navigator.clipboard.readText().then(function (text) {
        const originalTable = transformCopiedTextToTable(text);
        const tableData = transformCopiedTable(originalTable);
        if (tableData === null) {
            return;
        }

        // get all <td> element but skip the order column
        const oldTableElement = procModalElements.processColumnsTableBody
            .find('tr')
            .toArray()
            .map((tr) => tr.querySelectorAll('td:not([title="index"])'))
            .map((tds) => Array.from(tds));

        // warning and abort if we don't have enough rows
        if (oldTableElement.length !== tableData.length) {
            showToastrMsg(
                'Number of records mismatch. Please check and copy again',
                MESSAGE_LEVEL.WARN,
            );
            return;
        }

        const totalRows = oldTableElement.length;
        const totalColumns = oldTableElement[0].length;
        const copyAt = copyDataToTableAt(tableData, oldTableElement);
        for (let row = 0; row < totalRows; ++row) {
            for (let col = 0; col < totalColumns; ++col) {
                copyAt(row, col);
            }
        }

        showToastPasteFromClipboardSuccessful();
    }, showToastPasteFromClipboardFailed);
};

const selectAllColsHandler = (e) => {
    e.preventDefault();
    e.stopPropagation();

    // show context menu when right click
    const menu = $(procModalElements.checkColsContextMenu);
    const menuHeight = menu.height();
    const windowHeight = $(window).height();
    const left = e.clientX;
    let top = e.clientY;
    if (windowHeight - top < menuHeight) {
        top -= menuHeight;
    }
    menu.css({
        left: `${left}px`,
        top: `${top}px`,
        display: 'block',
    });

    const targetCol = $(e.currentTarget).find('input').attr('data-col-index');
    if (targetCol !== '') {
        $(menu).attr('data-target-col', targetCol);
    }
    return false;
};

const hideCheckColMenu = (e) => {
    // later, not just mouse down, + mouseout of menu
    $(procModalElements.checkColsContextMenu).css({ display: 'none' });
};

const bindSelectColumnsHandler = () => {
    $('table[name=latestDataTable] thead th').each((i, th) => {
        th.addEventListener('contextmenu', selectAllColsHandler, false);
        th.addEventListener('mouseover', hideCheckColMenu, false);
    });
};

/**
 * Show or Hide Copy & Paste process config buttons base on secure context
 */
function showHideCopyPasteProcessConfigButtons() {
    if (window.isSecureContext) {
        $(procModalElements.procSettingModalCopyAllBtn).show();
        $(procModalElements.procSettingModalPasteAllBtn).show();
    } else {
        $(procModalElements.procSettingModalCopyAllBtn).hide();
        $(procModalElements.procSettingModalPasteAllBtn).hide();
    }
}

let renderedCols;

$(() => {
    // workaround to make multiple modal work
    $(document).on('hidden.bs.modal', '.modal', () => {
        if ($('.modal:visible').length) {
            $(document.body).addClass('modal-open');
        }
    });

    // confirm auto fill master name
    $(procModalElements.msgConfirmBtn).click(() => {
        autoFillShownNameToModal();

        $(procModalElements.msgModal).modal('hide');
    });

    // click Import Data
    procModalElements.okBtn.click((e) => {
        runRegisterProcConfigFlow((edit = false));
    });

    // click Import Data
    procModalElements.scanBtn.click(() => {
        runRegisterDataTableColumnConfigFlow((edit = false));
    });

    // confirm Import Data
    procModalElements.confirmImportDataBtn.click(() => {
        $(procModalElements.confirmImportDataModal).modal('hide');

        const selectJson = getSelectedColumnsAsJson();
        saveProcCfg(selectJson, true);

        // save order to local storage
        setTimeout(() => {
            dragDropRowInTable.setItemLocalStorage(
                $(procElements.tableProcList)[0],
            ); // set proc table order
        }, 2000);

        recentEdit(procModalCurrentProcId);

        // show toastr
        showToastr();
        showJobAsToastr();
    });

    // re-register process config
    procModalElements.reRegisterBtn.click((e) => {
        runRegisterProcConfigFlow((edit = true));
    });

    procModalElements.confirmReRegisterProcBtn.click(() => {
        $(procModalElements.confirmReRegisterProcModal).modal('hide');
        const selectJson = getSelectedColumnsAsJson();
        saveProcCfg(selectJson, true);

        // save order to local storage
        setTimeout(() => {
            dragDropRowInTable.setItemLocalStorage(
                $(procElements.tableProcList)[0],
            ); // set proc table order
        }, 2000);
        recentEdit(procModalCurrentProcId);
    });

    // load tables to modal combo box
    loadTables(getSelectedOptionOfSelect(procModalElements.databases).val());

    // Databases onchange
    procModalElements.databases.change(() => {
        hideAlertMessages();
        isClickPreview = true;
        const dsSelected = getSelectedOptionOfSelect(
            procModalElements.databases,
        ).val();
        loadTables(dsSelected);
    });
    // Tables onchange
    procModalElements.tables.change(() => {
        hideAlertMessages();
        isClickPreview = true;
        setProcessName();
    });

    procModalElements.proc.on('mouseup', () => {
        userEditedProcName = true;
    });

    // Show records button click event
    procModalElements.showRecordsBtn.click((event) => {
        event.preventDefault();
        const currentShownTableName =
            getSelectedOptionOfSelect(procModalElements.tables).val() || null;
        const currentShownDataSouce =
            getSelectedOptionOfSelect(procModalElements.databases).val() ||
            null;
        const clearDataFlg = checkClearColumnsTable(
            Number(currentShownDataSouce),
            currentShownTableName,
        );
        const procModalForm = $(procModalElements.formId);
        const formData = new FormData(procModalForm[0]);
        if (!formData.get('tableName') && currentShownTableName) {
            formData.set('tableName', currentShownTableName);
        }

        preventSelectAll(true);

        // reset select all checkbox when click showRecordsBtn
        // $(procModalElements.selectAllColumn).css('display', 'block');
        // $(procModalElements.autoSelectAllColumn).css('display', 'block');

        showLatestRecords(formData, clearDataFlg);
    });

    // Show records button click event
    procModalElements.showRecordsBtn.click((event) => {
        event.preventDefault();
        const currentShownTableName =
            procModalElements.tables.find(':selected').val() || null;
        const currentShownDataSouce =
            procModalElements.databases.find(':selected').val() || null;
        const clearDataFlg = checkClearColumnsTableCfgDataTable(
            Number(currentShownDataSouce),
            currentShownTableName,
        );
        const procModalForm = $(procModalElements.formId);
        const formData = new FormData(procModalForm[0]);

        preventSelectAll(true);

        // reset select all checkbox when click showRecordsBtn
        // $(procModalElements.selectAllColumn).css('display', 'block');
        // $(procModalElements.autoSelectAllColumn).css('display', 'block');

        // reset first datetime checked
        setDatetimeSelected = false;
        showLatestRecords(formData, clearDataFlg);
    });

    procModalElements.proc.on('focusout', () => {
        checkDuplicateProcessName('data-name-en');
    });
    procModalElements.procJapaneseName.on('focusout', () => {
        checkDuplicateProcessName('data-name-jp');
    });
    procModalElements.procLocalName.on('focusout', () => {
        checkDuplicateProcessName('data-name-local');
    });

    $(procModalElements.revertChangeAsLinkIdBtn).click(() => {
        currentAsLinkIdBox.prop('checked', !currentAsLinkIdBox.prop('checked'));
    });

    $(procModalElements.revertChangeAsLinkIdBtn).click(() => {
        currentAsLinkIdBoxDataTable.prop(
            'checked',
            !currentAsLinkIdBoxDataTable.prop('checked'),
        );
    });

    // download all columns in proc config table
    $(procModalElements.procSettingModalDownloadAllBtn)
        .off('click')
        .click(downloadAllProcSettingInfo);
    // copy all columns in proc config table
    $(procModalElements.procSettingModalCopyAllBtn)
        .off('click')
        .click(copyAllProcSettingInfo);
    // paste all columns to in proc config table
    $(procModalElements.procSettingModalPasteAllBtn)
        .off('click')
        .click(pasteAllProcSettingInfo);

    initSearchProcessColumnsTable();
    showHideCopyPasteProcessConfigButtons();
});

/**
 * A datatype default object
 * @type DataTypeObject
 */
const datatypeDefaultObject = {
    value: '',
    is_get_date: false,
    is_main_date: false,
    is_main_time: false,
    is_serial_no: false,
    is_main_serial_no: false,
    is_auto_increment: false,
    is_int_cat: false,
};

const fixedName = {
    is_get_date: {
        system: 'Datetime',
        japanese: '日時',
    },
    is_main_serial_no: {
        system: 'Serial',
        japanese: 'シリアル',
    },
    is_line_name: {
        system: 'LineName',
        japanese: 'ライン名',
    },
    is_line_no: {
        system: 'LineNo',
        japanese: 'ラインNo',
    },
    is_eq_name: {
        system: 'EqName',
        japanese: '設備名',
    },
    is_eq_no: {
        system: 'EqNo',
        japanese: '設備No',
    },
    is_part_name: {
        system: 'PartName',
        japanese: '品名',
    },
    is_part_no: {
        system: 'PartNo',
        japanese: '品番',
    },
    is_st_no: {
        system: 'StNo',
        japanese: 'StNo',
    },
};

const datatypeI18nText = {
    is_get_date: $(procModali18n.i18nMainDatetime).text(),
    is_main_date: $(procModali18n.i18nMainDate).text(),
    is_main_time: $(procModali18n.i18nMainTime).text(),
    is_main_serial_no: {
        TEXT: $(procModali18n.i18nMainSerialStr).text(),
        INTEGER: $(procModali18n.i18nMainSerialInt).text(),
    },
    is_serial_no: {
        TEXT: $(procModali18n.i18nSerialStr).text(),
        INTEGER: $(procModali18n.i18nSerialInt).text(),
    },
    is_line_name: $(procModali18n.i18nLineNameStr).text(),
    is_line_no: $(procModali18n.i18nLineNoInt).text(),
    is_eq_name: $(procModali18n.i18nEqNameStr).text(),
    is_eq_no: $(procModali18n.i18nEqNoInt).text(),
    is_part_name: $(procModali18n.i18nPartNameStr).text(),
    is_part_no: $(procModali18n.i18nPartNoInt).text(),
    is_st_no: $(procModali18n.i18nStNoInt).text(),
    is_auto_increment: $(procModali18n.i18nDatetimeKey).text(),
    is_int_cat: $(`#${DataTypes.INTEGER_CAT.i18nLabelID}`).text(),
};

const DataTypeAttrs = [
    'is_get_date',
    'is_main_date',
    'is_main_time',
    'is_serial_no',
    'is_main_serial_no',
    'is_auto_increment',
    'is_line_name',
    'is_line_no',
    'is_eq_name',
    'is_eq_no',
    'is_part_name',
    'is_part_no',
    'is_st_no',
    'is_int_cat',
];
const allowSelectOneAttrs = [
    'is_get_date',
    'is_main_date',
    'is_main_time',
    'is_main_serial_no',
    'is_line_name',
    'is_line_no',
    'is_eq_name',
    'is_eq_no',
    'is_part_name',
    'is_part_no',
    'is_st_no',
    'is_auto_increment',
];
const unableToReselectAttrs = [
    'is_get_date',
    'is_auto_increment',
    'is_main_date',
    'is_main_time',
];
const fixedNameAttrs = [
    'is_get_date',
    'is_main_serial_no',
    'is_line_name',
    'is_line_no',
    'is_eq_name',
    'is_eq_no',
    'is_part_name',
    'is_part_no',
    'is_st_no',
];
const highPriorityItems = [
    ...fixedNameAttrs,
    'is_auto_increment',
    'is_serial_no',
];

const sortProcessColumns = (force = false) => {
    const selectJson = getSelectedColumnsAsJson();
    const [procCfgData] = collectProcCfgData(selectJson, true);
    const columns = procCfgData.columns;

    generateProcessList(
        columns,
        prcPreviewData.rows,
        prcPreviewData.dummy_datetime_idx,
        true,
        force,
    );
};

const reCalculateCheckedColumn = (newColNumber) => {
    const currentTotalColumn = procModalElements.totalColumns.text();
    const currentCheckedColumn = procModalElements.totalCheckedColumns.text();
    showTotalCheckedColumns(
        Number(currentTotalColumn) + newColNumber,
        Number(currentCheckedColumn) + newColNumber,
    );
};

const initSearchProcessColumnsTable = () => {
    initCommonSearchInput(procModalElements.searchInput);
    procModalElements.searchInput.on('keypress input', function (event) {
        const keyCode = event.keyCode;
        let value = stringNormalization(this.value.toLowerCase());
        if (keyCode === KEY_CODE.ENTER) {
            const indexs = searchTableContent(
                procModalElements.processColumnsTableId,
                value,
                false,
            );
            procModalElements.processColumnsSampleDataTableBody
                .find('tr')
                .show();
            procModalElements.processColumnsSampleDataTableBody
                .find('tr')
                .addClass('gray');
            for (const index of indexs) {
                procModalElements.processColumnsSampleDataTableBody
                    .find(`tr:eq(${index})`)
                    .removeClass('gray');
            }
        } else {
            procModalElements.processColumnsSampleDataTableBody
                .find('tr')
                .removeClass('gray');
            procModalElements.processColumnsTableBody
                .find('tr')
                .removeClass('gray');
            const indexs = searchTableContent(
                procModalElements.processColumnsTableId,
                value,
                true,
            );
            procModalElements.processColumnsSampleDataTableBody
                .find('tr')
                .hide();
            for (const index of indexs) {
                procModalElements.processColumnsSampleDataTableBody
                    .find(`tr:eq(${index})`)
                    .show();
            }
        }
    });

    procModalElements.searchSetBtn.on('click', function () {
        procModalElements.processColumnsTableBody
            .find(
                'tr:not(.gray)[is-master-col="false"] input[name=columnName]:visible',
            )
            .prop('checked', true)
            .trigger('change');
    });

    procModalElements.searchResetBtn.on('click', function () {
        procModalElements.processColumnsTableBody
            .find(
                'tr:not(.gray)[is-master-col="false"] input[name=columnName]:visible',
            )
            .prop('checked', false)
            .trigger('change');
    });
};

const handleScrollSampleDataTable = () => {
    const $sampleDataTableBody =
        procModalElements.processColumnsSampleDataTableBody;
    const $scrollToLeftSpan = $('#sampleDataScrollToLeft');
    const $scrollToLeftOneStepSpan = $('#sampleDataScrollToLeftOneStep');
    const $scrollToRightSpan = $('#sampleDataScrollToRight');
    const $scrollToRightOneStepSpan = $('#sampleDataScrollToRightOneStep');
    const $procConfigContent = $('.proc-config-content');

    handleScrollSampleDataTableCore(
        $sampleDataTableBody,
        $scrollToLeftSpan,
        $scrollToLeftOneStepSpan,
        $scrollToRightSpan,
        $scrollToRightOneStepSpan,
        $procConfigContent,
    );
};

/**
 * handleScrollSampleDataTableCore
 * @param {jQuery} $sampleDataTableBody
 * @param {jQuery} $scrollToLeftSpan
 * @param {jQuery} $scrollToLeftOneStepSpan
 * @param {jQuery} $scrollToRightSpan
 * @param {jQuery} $scrollToRightOneStepSpan
 * @param {jQuery} $procConfigContent
 */
const handleScrollSampleDataTableCore = (
    $sampleDataTableBody,
    $scrollToLeftSpan,
    $scrollToLeftOneStepSpan,
    $scrollToRightSpan,
    $scrollToRightOneStepSpan,
    $procConfigContent,
) => {
    const sampleDataTableWidth = $sampleDataTableBody.width();
    const sampleDataTableTdWidth = $sampleDataTableBody.find('td').width();

    $scrollToLeftSpan.off('click');
    $scrollToLeftSpan.on('click', function () {
        gotoScroll(0);
    });

    $scrollToLeftOneStepSpan.off('click');
    $scrollToLeftOneStepSpan.on('click', function () {
        const currentScrollLeft = parentScrollLeft();
        let offset = currentScrollLeft - sampleDataTableTdWidth;
        gotoScroll(offset);
    });

    $scrollToRightOneStepSpan.off('click');
    $scrollToRightOneStepSpan.on('click', function () {
        const currentScrollLeft = parentScrollLeft();
        let offset = currentScrollLeft + sampleDataTableTdWidth;
        gotoScroll(offset);
    });

    $scrollToRightSpan.off('click');
    $scrollToRightSpan.on('click', function () {
        gotoScroll(sampleDataTableWidth);
    });

    const parentScrollLeft = () => {
        return $procConfigContent.scrollLeft();
    };

    const gotoScroll = (offset) => {
        $procConfigContent.animate(
            {
                scrollLeft: offset,
            },
            1000,
        );
    };
};

/**
 * Handle Hover Process Columns Table Row
 * @param {HTMLBodyElement?} processColumnsTableBody
 * @param {HTMLBodyElement?} processColumnsSampleDataTableBody
 */
const handleHoverProcessColumnsTableRow = (
    processColumnsTableBody = null,
    processColumnsSampleDataTableBody = null,
) => {
    const $processColumnsTableBody = processColumnsTableBody
        ? $(processColumnsTableBody)
        : procModalElements.processColumnsTableBody;
    const $processColumnsSampleDataTableBody = processColumnsSampleDataTableBody
        ? $(processColumnsSampleDataTableBody)
        : procModalElements.processColumnsSampleDataTableBody;

    $processColumnsTableBody.find('tr').off('mouseenter');
    $processColumnsTableBody.find('tr').on('mouseenter', function (e) {
        const tr = $(e.currentTarget);
        const index = tr.index();
        $processColumnsSampleDataTableBody.find('tr').removeClass('hovered');
        $processColumnsTableBody.find('tr').removeClass('hovered');
        $processColumnsSampleDataTableBody
            .find(`tr:eq(${index})`)
            .addClass('hovered');
    });

    $processColumnsTableBody.find('tr').off('mouseleave');
    $processColumnsTableBody.find('tr').on('mouseleave', function (e) {
        const tr = $(e.currentTarget);
        const index = tr.index();
        $processColumnsSampleDataTableBody
            .find(`tr:eq(${index})`)
            .removeClass('hovered');
    });

    $processColumnsSampleDataTableBody.find('tr').off('mouseenter');
    $processColumnsSampleDataTableBody
        .find('tr')
        .on('mouseenter', function (e) {
            const tr = $(e.currentTarget);
            const index = tr.index();
            $processColumnsTableBody.find('tr').removeClass('hovered');
            $processColumnsSampleDataTableBody
                .find('tr')
                .removeClass('hovered');
            $processColumnsTableBody
                .find(`tr:eq(${index})`)
                .addClass('hovered');
        });

    $processColumnsSampleDataTableBody.find('tr').off('mouseleave');
    $processColumnsSampleDataTableBody
        .find('tr')
        .on('mouseleave', function (e) {
            const tr = $(e.currentTarget);
            const index = tr.index();
            $processColumnsTableBody
                .find(`tr:eq(${index})`)
                .removeClass('hovered');
        });
};
