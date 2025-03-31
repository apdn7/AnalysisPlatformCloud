// state
let currentDSTR;
let latestRecords;
let useDummyDatetime;
let v2DataSources = null;
const MAX_NUMBER_OF_SENSOR = 100000000;
const MIN_NUMBER_OF_SENSOR = 0;
let isV2ProcessConfigOpening = false;
let v2ImportInterval = null;
const DUMMY_V2_PROCESS_NAME = 'DUMMY_V2_PROCESS_NAME';
let csvType = 'csv';
// data type
const originalTypes = {
    0: null,
    1: 'INTEGER',
    2: 'REAL',
    3: 'TEXT',
    4: 'DATETIME',
};

// Default config
const DEFAULT_CONFIGS = (DB_CONFIGS = {
    POSTGRESQL: {
        id: 'POSTGRESQL',
        name: 'postgresql',
        master_type: 'OTHERS',
        configs: {
            type: 'POSTGRESQL',
            port: 5432,
            schema: 'public',
            dbname: '',
            host: '',
            username: '',
            password: '',
            use_os_timezone: false,
        },
    },
    SQLITE: {
        id: 'SQLITE',
        name: 'sqlite',
        master_type: 'OTHERS',
        configs: {
            type: 'SQLITE',
            dbname: '',
            use_os_timezone: false,
        },
    },
    MSSQLSERVER: {
        id: 'MSSQLSERVER',
        name: 'mssqlserver',
        master_type: 'OTHERS',
        configs: {
            type: 'MSSQLSERVER',
            port: 1433,
            schema: 'dbo',
            dbname: '',
            host: '',
            username: '',
            password: '',
            use_os_timezone: false,
        },
    },
    MYSQL: {
        id: 'MYSQL',
        name: 'mysql',
        master_type: 'OTHERS',
        configs: {
            type: 'MYSQL',
            port: 3306,
            schema: null,
            dbname: '',
            host: '',
            username: '',
            password: '',
            use_os_timezone: false,
        },
    },
    ORACLE: {
        id: 'ORACLE',
        name: 'oracle',
        master_type: 'OTHERS',
        configs: {
            type: 'ORACLE',
            port: 1521,
            schema: null,
            dbname: '',
            host: '',
            username: '',
            password: '',
            use_os_timezone: false,
        },
    },
    CSV: {
        id: 'CSV',
        name: 'csv/tsv',
        master_type: 'OTHERS',
        configs: {
            type: 'CSV',
            directory: '',
            delimiter: 'Auto',
            use_os_timezone: false,
        },
    },
    ORACLE_EFA: {
        id: 'ORACLE_EFA',
        name: 'eFA',
        master_type: 'EFA',
        configs: {
            type: 'ORACLE',
            port: 1521,
            schema: null,
            dbname: '',
            host: '',
            username: '',
            password: '',
            use_os_timezone: false,
        },
    },
    CSV_V2: {
        id: 'CSV_V2',
        name: 'V2',
        master_type: 'V2',
        configs: {
            type: 'CSV',
            directory: '',
            delimiter: 'Auto',
            use_os_timezone: false,
        },
    },
    V2: {
        name: 'v2 csv',
        master_type: 'V2',
        configs: {
            type: 'V2',
            directory: '',
            delimiter: 'Auto',
            use_os_timezone: false,
        },
    },
    SOFTWARE_WORKSHOP: {
        id: 'SOFTWARE_WORKSHOP',
        name: 'software workshop',
        master_type: 'SOFTWARE_WORKSHOP',
        configs: {
            // type: 'POSTGRESQL', //'SOFTWARE_WORKSHOP',
            type: 'SOFTWARE_WORKSHOP', //'SOFTWARE_WORKSHOP',
            port: 5432,
            schema: 'public',
            dbname: '',
            host: '',
            username: '',
            password: '',
            use_os_timezone: false,
        },
    },
});

const HttpStatusCode = {
    isOk: 200,
    serverErr: 500,
};

const dbElements = {
    tblDbConfig: 'tblDbConfig',
    tblDbConfigID: '#tblDbConfig',
    divDbConfig: '#data_source',
    v2ProcessDiv: '#v2ProcessDiv',
    v2ProcessSelection: '#v2ProcessSelection',
    saveDataSourceModal: '#createV2ProcessDataSource',
    CSVTitle: $('#CSVSelectedLabel'),
};

const DATETIME = originalTypes[4];

const i18nDBCfg = {
    subFolderWrongFormat: $('#i18nSubFolderWrongFormat').text(),
    dirExist: $(`#${csvResourceElements.i18nDirExist}`).text(),
    dirNotExist: $(`#${csvResourceElements.i18nDirNotExist}`).text(),
    noDatetimeColMsg: $('#i18nNoDatetimeCol').text(),
    fileNotFound: $('#i18nFileNotFoundMsg'),
    couldNotReadData: $('#i18nCouldNotReadData'),
    dummyHeader: $('#i18nDummyHeader'),
    partialDummyHeader: $('#i18nPartialDummyHeader'),
};

const triggerEvents = {
    CHANGE: 'change',
    SELECT: 'select',
    ALL: 'all',
};

const checkFolderResources = async (folderUrl, originFolderUrl) => {
    ajaxWithLog({
        url: csvResourceElements.apiCheckFolderUrl,
        method: 'POST',
        data: JSON.stringify({
            url: folderUrl,
            isFile: await checkIsFilePath(folderUrl, originFolderUrl),
        }),
        contentType: 'application/json',
        success: (res) => {
            res = jsonParse(res);
            if (res.is_valid) {
                displayRegisterMessage(
                    csvResourceElements.alertMsgCheckFolder,
                    {
                        message: i18nDBCfg.dirExist,
                        is_error: false,
                    },
                );
                $(csvResourceElements.showResourcesBtnId).trigger('click');
            } else {
                displayRegisterMessage(
                    csvResourceElements.alertMsgCheckFolder,
                    {
                        message: res.err_msg,
                        is_error: true,
                    },
                );
                // hide loading
                $('#resourceLoading').hide();
                return;
            }
        },
    });
};

const showLatestRecordsFromDS = (
    res,
    hasDT = true,
    useSuffix = true,
    isV2 = false,
    v2Type = '',
) => {
    $(csvResourceElements.fileName).text(res.file_name);
    const v2HistoryTableContent = $(
        `${csvResourceElements.dataTbl} table#v2HistoryPreviewTable`,
    );
    const csvV2tableContent = $(
        `${csvResourceElements.dataTbl} table#v2CsvPreviewTable`,
    );
    v2HistoryTableContent.hide();
    csvV2tableContent.hide();
    let tableContent = csvV2tableContent;
    let directoryNo = 1;
    if (isV2 && v2Type === 'v2_history') {
        tableContent = v2HistoryTableContent;
        directoryNo = 2;
    }
    tableContent.show();

    const tableHeadTr = tableContent.find('thead tr');
    const tableBody = tableContent.find('tbody');

    tableHeadTr.html('');
    tableBody.html('');
    $('#resourceLoading').hide();
    if (res.is_dummy_header) {
        $(csvResourceElements.dummyHeaderModalMsg).text(
            $(i18nDBCfg.dummyHeader).text(),
        );
        $(csvResourceElements.dummyHeaderModal).modal('show');
    }
    if (res.partial_dummy_header) {
        $(csvResourceElements.dummyHeaderModalMsg).text(
            $(i18nDBCfg.partialDummyHeader).text(),
        );
        $(csvResourceElements.dummyHeaderModal).modal('show');
    }
    let hasDuplCols = false;
    const predictedDatatypes = res.dataType;
    const dummyDatetimeIdx = res.dummy_datetime_idx;
    const colsName = useSuffix ? res.header : res.org_headers;
    colsName.forEach((column, idx) => {
        let columnColor = dummyDatetimeIdx === idx ? ' dummy_datetime_col' : '';
        const isDuplCol = res.has_dupl_cols
            ? res.same_values[idx].is_dupl
            : false;
        if (isDuplCol) {
            columnColor += ' dupl_col';
            hasDuplCols = true;
        }
        const datatype = predictedDatatypes[idx];
        const tblHeader = `<th>
                    <input id="col-${idx}" class="data-type-predicted" ${directoryNo ? `data-directory-no=${directoryNo}` : ''} value="${datatype}" type="hidden">
                    <div class="">
                        <label class="column-name${columnColor}" for="">${column}</label>
                    </div>
                </th>`;
        tableHeadTr.append(tblHeader);
    });

    res.content.forEach((row) => {
        let rowContent = '<tr>';
        row.forEach((val, idx) => {
            const datatype = predictedDatatypes[idx];
            let columnColor =
                dummyDatetimeIdx === idx ? ' dummy_datetime_col' : '';
            const isDuplCol = res.has_dupl_cols
                ? res.same_values[idx].is_dupl
                : false;
            if (isDuplCol) {
                columnColor += ' dupl_col';
            }
            let formattedVal = val;
            formattedVal = trimBoth(String(formattedVal));
            if (datatype === DataTypes.DATETIME.value) {
                formattedVal = parseDatetimeStr(formattedVal);
            } else if (datatype === DataTypes.INTEGER.value) {
                formattedVal = parseIntData(formattedVal);
                if (isNaN(formattedVal)) {
                    formattedVal = '';
                }
            } else if (datatype === DataTypes.REAL.value) {
                formattedVal = parseFloatData(formattedVal);
            }
            rowContent += `<td data-original="${val}" class="${columnColor}">${formattedVal}</td>`;
        });
        rowContent += '</tr>';
        tableBody.append(rowContent);
    });
    csvResourceElements.csvFileName = res.file_name;
    $(csvResourceElements.skipHead).val(res.skip_head);
    $(csvResourceElements.skipTail).val(res.skip_tail);
    $(`${csvResourceElements.dataTbl}`).show();
    updateBtnStyleWithValidation($(csvResourceElements.csvSubmitBtn), false);
    if (
        res.file_name &&
        hasDT &&
        ((res.has_dupl_cols && useSuffix) || !res.has_dupl_cols)
    ) {
        // if show preview table ok, enable submit button
        $('button.saveDBInfoBtn[data-csv="1"]').removeAttr('disabled');
        updateBtnStyleWithValidation($(csvResourceElements.csvSubmitBtn), true);
        updateBtnStyleWithValidation(
            $(csvResourceElements.csvDirectImportBtn),
            true,
        );
    }
    // show message in case of has duplicated cols
    if (hasDuplCols) {
        const msgContent = '';
        showToastrMsg(msgContent);
    }

    // show encoding
    if (res.encoding) {
        $('#dbsEncoding').text(`Encoding: ${res.encoding}`);
    }
};

const addDummyDatetimeDS = (addCol = true) => {
    if (!addCol) {
        // update content of csv
        latestRecords.content.forEach((row) => row.shift());
        latestRecords.dataType.shift();
        latestRecords.header.shift();
        latestRecords.org_headers.shift();
        latestRecords.same_values.shift();
        latestRecords.dummy_datetime_idx = null;
    }
    useDummyDatetime = addCol;
    if (latestRecords.has_dupl_cols) {
        showDuplColModal();
    } else {
        showLatestRecordsFromDS(latestRecords, addCol);
    }
    if (addCol) {
        // clear old error message
        $(csvResourceElements.alertDSErrMsgContent).html('');
        $(csvResourceElements.alertMsgCheckFolder).hide();
        showToastrMsgNoCTCol(latestRecords.has_ct_col);
    } else {
        // clear old alert top msg
        $('.alert-top-fixed').hide();
        displayRegisterMessage(csvResourceElements.alertMsgCheckFolder, {
            message: i18nDBCfg.noDatetimeColMsg,
            is_error: true,
        });
    }
    updateBtnStyleWithValidation($(csvResourceElements.csvSubmitBtn), addCol);
    updateBtnStyleWithValidation(
        $(csvResourceElements.csvDirectImportBtn),
        addCol,
    );
};

const genColSuffix = (agree = true) => {
    const isV2 =
        $(csvResourceElements.showResourcesBtnId).attr('data-isV2') ===
            'true' || false;
    showLatestRecordsFromDS(
        latestRecords,
        useDummyDatetime,
        agree,
        isV2,
        csvType,
    );
};

/**
 * Check Url Is File Path or not
 * @param {string} folderUrl - an Url string
 * @param {string} originFolderUrl - an origin Url string
 * @return {Promise<boolean>} - true: is file path, false: is not file path
 */
async function checkIsFilePath(folderUrl, originFolderUrl) {
    const $filePathHiddenEl = $(csvResourceElements.isFilePathHidden);
    let folderInfoPromise;
    if ($filePathHiddenEl.val() === '') {
        // in case of new data source and first time checking, always call api to check
        folderInfoPromise = checkFolderOrFile(folderUrl);
    } else if (folderUrl !== originFolderUrl) {
        // in case of Url changed, call api to check
        folderInfoPromise = checkFolderOrFile(folderUrl);
    } else {
        // in other cases, get value of isFilePath hidden input
        folderInfoPromise = new Promise((resolve) =>
            resolve({
                isFile: $filePathHiddenEl.val().toLowerCase() === String(true),
            }),
        );
    }

    const folderInfo = await folderInfoPromise;
    $filePathHiddenEl.val(folderInfo.isFile);
    return folderInfo.isFile;
}

const showResources = async (selectedDatabaseType) => {
    csvType = selectedDatabaseType;
    let folderUrl;
    let originFolderUrl;
    if (selectedDatabaseType.toLowerCase() === 'csv_v2') {
        folderUrl = $(csvResourceElements.folderV2UrlInput).val();
        originFolderUrl = $(csvResourceElements.folderV2UrlInput).data(
            'originValue',
        );
        $(
            `${csvResourceElements.dataTbl} table#v2CsvPreviewTable thead tr`,
        ).empty();
        $(
            `${csvResourceElements.dataTbl} table#v2CsvPreviewTable tbody`,
        ).empty();
    } else if (selectedDatabaseType.toLowerCase() === 'v2_history') {
        $(
            `${csvResourceElements.dataTbl} table#v2HistoryPreviewTable thead tr`,
        ).empty();
        $(
            `${csvResourceElements.dataTbl} table#v2HistoryPreviewTable tbody`,
        ).empty();
        folderUrl = $(csvResourceElements.folderV2HistoryUrlInput).val();
        originFolderUrl = $(csvResourceElements.folderV2HistoryUrlInput).data(
            'originValue',
        );
    } else {
        folderUrl = $(csvResourceElements.folderUrlInput).val();
        originFolderUrl = $(csvResourceElements.folderUrlInput).data(
            'originValue',
        );
    }
    const isFilePath = await checkIsFilePath(folderUrl, originFolderUrl);

    const db_code = $(csvResourceElements.showResourcesBtnId).data('itemId');
    const isV2 =
        $(csvResourceElements.showResourcesBtnId).attr('data-isV2') ===
            'true' || false;
    const checkFolderAPI = '/ap/api/setting/check_folder';
    const checkFolderRes = await fetchData(
        checkFolderAPI,
        JSON.stringify({ url: folderUrl, isFile: isFilePath }),
        'POST',
    );
    if (checkFolderRes && !checkFolderRes.is_valid) {
        displayRegisterMessage(csvResourceElements.alertMsgCheckFolder, {
            message: checkFolderRes.err_msg,
            is_error: true,
        });
        // hide loading
        $('#resourceLoading').hide();
        return;
    }
    // get line skipping config
    const lineSkipping = $('input[name=line_skip]').val();
    const csvNRows = $(csvResourceElements.csvNRows).val() || null;
    const csvIsTranspose = $(csvResourceElements.csvIsTranspose).is(':checked');
    ajaxWithLog({
        url: csvResourceElements.apiUrl,
        method: 'POST',
        data: JSON.stringify({
            db_code,
            url: folderUrl,
            etl_func: $('[name=optionalFunction]').val(),
            delimiter: $(csvResourceElements.delimiter).val(),
            isV2,
            line_skip: lineSkipping,
            n_rows: csvNRows,
            is_transpose: csvIsTranspose,
            is_file: isFilePath,
        }),
        contentType: 'application/json',
        success: (res) => {
            $(csvResourceElements.alertInternalError).hide();
            res = jsonParse(res);
            showToastrMsgFailLimit(res);

            // save dummy header flag
            if (Object.keys(res).includes('is_dummy_header')) {
                $(csvResourceElements.isDummyHeader).val(res.is_dummy_header);
            }

            if (!res.has_ct_col) {
                showDummyDatetimeModal(res);
                latestRecords = res;
            } else if (res.has_dupl_cols) {
                showDuplColModal();
                latestRecords = res;
            } else {
                showLatestRecordsFromDS(
                    res,
                    true,
                    true,
                    isV2,
                    selectedDatabaseType,
                );
                // active button
                updateBtnStyleWithValidation(
                    $(csvResourceElements.csvSubmitBtn),
                    res.has_ct_col,
                );
                updateBtnStyleWithValidation(
                    $(csvResourceElements.csvDirectImportBtn),
                    res.has_ct_col,
                );
            }

            if (isV2 && res.is_process_null) {
                res.v2_processes = [DUMMY_V2_PROCESS_NAME];
                res.v2_processes_shown_name = [
                    $(dbConfigElements.csvDBSourceName).val(),
                ];
            }
            // update process of V2
            if (res.v2_processes && res.v2_processes.length) {
                const v2ProcessList = res.v2_processes;
                const v2ProcessShownNameList =
                    res.v2_processes_shown_name || res.v2_processes;
                addProcessList(v2ProcessList, v2ProcessShownNameList);
                $('input[name="v2Process"]').on('change', () => {
                    const selectedProcess = getCheckedV2Processes();
                    if (selectedProcess.length) {
                        // enable OK button
                        updateBtnStyleWithValidation(
                            $(csvResourceElements.csvSubmitBtn),
                            true,
                        );
                        $('button.saveDBInfoBtn[data-csv="1"]').prop(
                            'disabled',
                            false,
                        );
                    } else {
                        updateBtnStyleWithValidation(
                            $(csvResourceElements.csvSubmitBtn),
                            false,
                        );
                        $('button.saveDBInfoBtn[data-csv="1"]').prop(
                            'disabled',
                            true,
                        );
                    }
                });
            }
        },
        error: (error) => {
            $('#resourceLoading').hide();
            hideAlertMessages();
            displayRegisterMessage(csvResourceElements.alertInternalError, {
                message: i18nDBCfg.couldNotReadData.text(),
                is_error: true,
            });
        },
    });
};

const addProcessList = (
    procsIds,
    processList = [],
    checkedIds = [],
    parentId = 'v2ProcessSelection',
    name = 'v2Process',
) => {
    $(`#${parentId}`).empty();
    addGroupListCheckboxWithSearch(
        parentId,
        parentId + '__selection',
        '',
        procsIds,
        processList,
        {
            name: name,
            checkedIds: checkedIds,
        },
    );
};

const validateDBName = () => {
    let isOk = true;
    const dbnames = $(dbConfigElements.csvDBSourceName).val();

    if (!$.trim(dbnames)) {
        isOk = false;
    }

    return {
        isOk,
        message: $(dbConfigElements.i18nDbSourceEmpty).text(),
    };
};

const validateExistDBName = (dbName) => {
    let isOk = true;
    const dataSrcId = currentDSTR.attr(csvResourceElements.dataSrcId);
    const currentName = currentDSTR.find('input[name="name"]').val();

    const getFormData = genJsonfromHTML('#tblDbConfig', 'root', true);
    const dbNames = getFormData('name');

    if (dbNames.root.name != '') {
        // 既存レコードを修正案する場合。
        if (dataSrcId) {
            const index = dbNames.root.name.indexOf(currentName);
            if (index > -1) {
                dbNames.root.name.splice(index, 1);
            }
        }

        for (let i = 0; i < dbNames.root.name.length; i++) {
            if (dbNames.root.name[i] === dbName) {
                isOk = false;
                break;
            }
        }
    }

    return {
        isOk,
        message: $(dbConfigElements.i18nDbSourceExist).text(),
    };
};

let tmpResource;

// call backend API to save
const saveDataSource = (
    dsCode,
    dsConfig,
    isBackToCSVTSVDataSourceConfigModal = false,
) => {
    fetchWithLog('api/setting/data_source_save', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(dsConfig),
    })
        .then((response) => response.clone().json())
        .then((json) => {
            displayRegisterMessage('#alert-msg-db', json.flask_message);

            // 新規アイテムのattrを設定する。
            const itemName = 'name';
            if (!dsCode) {
                currentDSTR.attr('id', csvResourceElements.dsId + json.id);
                currentDSTR.attr(csvResourceElements.dataSrcId, json.id);
                // itemName = "master-name"
            }

            // データソース名とコメントの値を設定する。
            const dbType = currentDSTR
                .find('select[name="type"]')
                .val()
                .replace('_EFA', '')
                .replace('_V2', '');
            if (dbType === DB_CONFIGS.CSV.configs.type) {
                currentDSTR
                    .find(`input[name="${itemName}"]`)
                    .val($('#csvDBSourceName').val());
                currentDSTR
                    .find('textarea[name="comment"]')
                    .val($('#csvComment').val());
            } else {
                currentDSTR
                    .find(`input[name="${itemName}"]`)
                    .val($(`#${dbType.toLowerCase()}_dbsourcename`).val());
                currentDSTR
                    .find('textarea[name="comment"]')
                    .val($(`#${dbType.toLowerCase()}_comment`).val());
            }

            // show toastr message to guide user to proceed to Process config
            showToastrToProceedProcessConfig();
            // remove tmp resource
            tmpResource = {};
            recentEdit(dsCode);

            // add new data source
            if (json.data_source) {
                const newDataSrc = jsonParse(json.data_source);
                let isNew = true;
                for (let i = 0; i < cfgDS.length; i++) {
                    if (cfgDS[i].id === newDataSrc.id) {
                        cfgDS[i] = newDataSrc;
                        isNew = false;
                        break;
                    }
                }
                if (isNew) {
                    cfgDS.push(newDataSrc);
                }
            }
            loadingHide();
            if (dsConfig.is_direct_import) {
                $('.modal').modal('hide');
            }
            // if id efa direct
            if (dsConfig.is_direct_import) {
                if (dsConfig.master_type === 'EFA') {
                    $(csvResourceElements.partitionModal).modal('show');
                    $(csvResourceElements.partitionModal).attr(
                        'data-source-id',
                        `${json.id}`,
                    );
                    $(csvResourceElements.partitionModal).attr(
                        'is-save-data-table',
                        `${json.is_has_data_table}`,
                    );
                    let partitions = json.partitions;
                    const partition_from =
                        json.partition_from === null ? '' : json.partition_from;
                    const partition_to =
                        json.partition_to === null ? '' : json.partition_to;

                    showPartitions(partitions, partition_from, partition_to);
                } else if (dsConfig.master_type === 'OTHERS') {
                    console.log('OTHERS DIRECT');
                    $(csvResourceElements.directColumnAttributeModal).modal(
                        'show',
                    );
                    $(csvResourceElements.directColumnAttributeModal).attr(
                        'data-source-id',
                        `${json.id}`,
                    );
                    const serialCols = ['', ...json.serial_cols];
                    const datetimeCols = ['', ...json.datetime_cols];
                    const orderCols = ['', ...json.order_cols];
                    showColumnAttributeModal(
                        serialCols,
                        datetimeCols,
                        orderCols,
                    );
                }
            } else {
                const dataSourceIds = $(
                    '#tblDataTableConfig select[name=cfgProcDatabaseName]',
                )
                    .map(function () {
                        return $(this).val();
                    })
                    .get();

                const dataSourceName = $(
                    '#tblDataTableConfig input[name=databaseName]',
                )
                    .map(function () {
                        return $(this).val();
                    })
                    .get();
                if (
                    !dataSourceIds.includes(json.id) &&
                    !dataSourceName.includes(json.data_source_name)
                ) {
                    addDataTableToTable(json.id);
                } else {
                    let ele = $(
                        `#tblDataTableConfig input[name=databaseName][value=${json.data_source_name}]`,
                    );
                    if (ele.length === 0) {
                        ele = $(
                            `#tblDataTableConfig select[name=cfgProcDatabaseName] option[value=${json.id}]`,
                        );
                    }
                    const button = ele.closest('tr').find('button').first();
                    showDataTableSettingModal(
                        button,
                        isBackToCSVTSVDataSourceConfigModal,
                    );
                }
            }
        })
        .catch((json) => {
            displayRegisterMessage('#alert-msg-db', json.flask_message);

            if (dsConfig.is_direct_import) {
                loadingHide();
            }
        });
};

const saveV2DataSource = (dsConfig) => {
    loading.css('z-index', 9999);
    loading.show();
    fetchWithLog('api/setting/v2_data_source_save', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(dsConfig),
    })
        .then((response) => response.clone().json())
        .then((json) => {
            $('#modal-db-csv').modal('hide');
            displayRegisterMessage('#alert-msg-db', json.flask_message);
            if (json.flask_message.is_error) {
                loading.hide();
                return;
            }
            const dataSources = json.data.map((da) =>
                jsonParse(da.data_source),
            );
            let index = 0;
            for (const dbs of dataSources) {
                index += 1;
                const itemName = 'name';
                currentDSTR.attr('id', csvResourceElements.dsId + dbs.id);
                currentDSTR.attr(csvResourceElements.dataSrcId, dbs.id);

                // データソース名とコメントの値を設定する。
                currentDSTR.find(`input[name="${itemName}"]`).val(dbs.name);
                currentDSTR.find('textarea[name="comment"]').val(dbs.comment);
                currentDSTR.find('select[name="type"]').val(dbs.type);
                if (index < dataSources.length) {
                    currentDSTR = addDBConfigRow();
                }

                const allDSID = cfgDS.map((datasource) => datasource.id);
                if (allDSID.length && !allDSID.includes(dbs.id)) {
                    cfgDS.push(dbs);
                }
            }
            v2DataSources = null;
            // show toastr message to guide user to proceed to Process config
            showToastrToProceedProcessConfig();
            loading.hide();

            // show v2 process config automatically
            const dbsIds = json.data.map((da) => da.id);
            showAllV2ProcessConfigModal(dbsIds);
        })
        .catch((json) => {
            displayRegisterMessage('#alert-msg-db', json.flask_message);
            loading.hide();
        });
};

const showAllV2ProcessConfigModal = (dbsIds) => {
    let index = 1;
    v2ImportInterval = setInterval(() => {
        if (index > dbsIds.length) {
            clearInterval(v2ImportInterval);
            isV2ProcessConfigOpening = false;
        }

        if (!isV2ProcessConfigOpening) {
            resetIsShowFileName();
            showV2ProcessConfigModal(dbsIds[index - 1]);
            index++;
        }
    }, 1000);
};

const handleCloseProcConfigModal = () => {
    isV2ProcessConfigOpening = false;
};

const showV2ProcessConfigModal = (dbsId = null) => {
    if (!dbsId) return;
    showProcSettingModal(null, dbsId);
    isV2ProcessConfigOpening = true;
};

// get csv column infos
const getCsvColumns = () => {
    const columnNames = [];
    const columnTypes = [];
    const orders = [];
    const directoryNos = [];
    // $(csvResourceElements.dataTypeSelector).each((i, item) => {
    $(csvResourceElements.dataTypePredicted).each((i, item) => {
        const columnName = $(item)
            .parent()
            .find(csvResourceElements.columnName)
            .text();
        let dataType;
        let directoryNo = $(item).attr('data-directory-no') || 1;
        if (parseInt($(item).val(), 10) === DataTypes.NONE.value) {
            dataType = originalTypes[3];
        } else {
            dataType = originalTypes[$(item).val()];
        }
        columnTypes.push(dataType);
        columnNames.push(columnName);
        orders.push(i + 1);
        directoryNos.push(Number(directoryNo));
    });
    return [columnNames, columnTypes, orders, directoryNos];
};

const validateCsvInfo = (isV2) => {
    if (!validateDBName().isOk) {
        displayRegisterMessage('#alert-msg-csvDbname', {
            message: validateDBName().message,
            is_error: true,
        });
        loadingHide();
        return false;
    }

    // データベース名の存在をチェックする。
    if (
        !validateExistDBName($(dbConfigElements.csvDBSourceName).val()).isOk &&
        !isV2
    ) {
        displayRegisterMessage('#alert-msg-csvDbname', {
            message: validateExistDBName().message,
            is_error: true,
        });
        loadingHide();
        return false;
    }

    // const [columnNames, columnTypes, orders] = getCsvColumns();
    //
    // // Check if there is no get_date
    // const nbGetDateCol = columnTypes.filter(col => col === DATETIME).length;
    //
    // if (nbGetDateCol === 0) {
    //     const msgErrorNoGetdate = $(csvResourceElements.msgErrorNoGetdate).text();
    //     displayRegisterMessage(
    //         csvResourceElements.alertMsgCheckFolder,
    //         { message: msgErrorNoGetdate, is_error: true },
    //     );
    //     return false;
    // }

    const isWarning = false;
    // let msgWarnManyGetdate = '';
    // if (nbGetDateCol > 1) {
    //     msgWarnManyGetdate = $(csvResourceElements.msgWarnManyGetdate).text();
    //     const firstGetdateIdx = columnTypes.indexOf(DATETIME);
    //     const firstGetdate = columnNames[firstGetdateIdx] || '';
    //     msgWarnManyGetdate = `${msgWarnManyGetdate.replace('{col}', firstGetdate)}\n`;
    //     isWarning = true;
    // }

    if (isWarning) {
        $(csvResourceElements.csvConfirmModalMsg).text(
            `${msgWarnManyGetdate}${$(csvResourceElements.msgConfirm).text()}`,
        );

        $(csvResourceElements.csvConfirmModal).modal('show');
        return false;
    }

    return true;
};

// gen csv info
const genCsvInfo = async () => {
    const dbItemId = currentDSTR.attr(csvResourceElements.dataSrcId);
    const dbType = currentDSTR
        .find('select[name="type"]')
        .val()
        .replace('_EFA', '')
        .replace('_V2', '');
    const masterDbType = $(csvResourceElements.masterDbType).val();
    let directory = '';
    let secondDirectory = '';
    if (masterDbType === DB_CONFIGS.V2.master_type) {
        directory = $(csvResourceElements.folderV2UrlInput).val();
        secondDirectory = $(csvResourceElements.folderV2HistoryUrlInput).val();
    } else {
        directory = $(csvResourceElements.folderUrlInput).val();
    }
    const skipHead = $(csvResourceElements.skipHead).val() || null;
    const skipTail = $(csvResourceElements.skipTail).val() || null;
    const csvNRows = $(csvResourceElements.csvNRows).val() || null;
    const csvIsTranspose = $(csvResourceElements.csvIsTranspose).is(':checked');
    const delimiter = $(csvResourceElements.delimiter).val();
    const optionalFunction = $(csvResourceElements.optionalFunction).val();
    const [columnNames, columnTypes, orders, directoryNos] = getCsvColumns();
    const isDummyHeader = $(csvResourceElements.isDummyHeader).val();
    const isFilePath =
        $(csvResourceElements.isFilePathHidden).val().toLowerCase() === 'true';

    // Get csv Information
    const csvColumns = [];

    const dictCsvDetail = {
        id: dbItemId,
        directory: directory,
        second_directory: secondDirectory,
        skip_head: skipHead,
        skip_tail: skipTail,
        n_rows: csvNRows,
        is_transpose: csvIsTranspose,
        etl_func: optionalFunction,
        delimiter,
        csv_columns: csvColumns,
        dummy_header: isDummyHeader,
        is_file_path: isFilePath,
    };

    const dictDataSrc = {
        id: dbItemId,
        name: $('#csvDBSourceName').val(),
        type: dbType,
        comment: $('#csvComment').val(),
        csv_detail: dictCsvDetail,
        master_type: masterDbType,
    };

    for (let i = 0; i < columnNames.length; i++) {
        csvColumns.push({
            data_source_id: dbItemId,
            column_name: columnNames[i],
            data_type: columnTypes[i],
            order: orders[i],
            directory_no: directoryNos[i],
        });
    }

    return dictDataSrc;
};

const validateV2CSVDBSources = (dbs) => {
    // check duplicated db name
    let isDuplicatedDBName = false;
    for (const v2Dbs of dbs) {
        const dbName = v2Dbs.name;
        if (!validateExistDBName(dbName).isOk) {
            displayRegisterMessage('#alert-msg-csvDbname', {
                message: validateExistDBName().message,
                is_error: true,
            });
            isDuplicatedDBName = true;
        }
    }
    return isDuplicatedDBName;
};

const saveCSVDataSource = async (isDirectImport = fasle, isV2 = false) => {
    const dataSrcId = currentDSTR.attr(csvResourceElements.dataSrcId);
    const dictDataSrc = await genCsvInfo();

    // save
    // if (isV2) {
    //     // update V2 process data to dictDataSrc
    //     const v2DatasourceByProcess = getV2ProcessData(dictDataSrc);
    //     if (v2DatasourceByProcess.length > 1) {
    //         // show modal messenger
    //         $(dbElements.saveDataSourceModal).modal('show');
    //         v2DataSources = v2DatasourceByProcess;
    //
    //     } else {
    //         saveV2DataSource(v2DatasourceByProcess);
    //     }
    // } else {
    //     saveDataSource(dataSrcId, dictDataSrc);
    //     $('#modal-db-csv').modal('hide');
    // }

    if (isDirectImport) {
        dictDataSrc['is_direct_import'] = true;
    }
    // loadingShow();
    saveDataSource(dataSrcId, dictDataSrc, true);

    // show toast after save
    let dataTypes = $(csvResourceElements.dataTypeSelector).find(
        'option:checked',
    );
    dataTypes = dataTypes
        .toArray()
        .filter(
            (dataType) => Number(dataType.value) === DataTypes.STRING.value,
        );
    if (dataTypes.length > 1) {
        const warningMsg = `${dataTypes.length} ${$(
            '#i18nTooManyString',
        ).text()}`;
        showToastrMsg(warningMsg);
    }
    if (!dictDataSrc['is_direct_import']) {
        $('.modal').modal('hide');
    }
};

const handleSaveV2DataSources = () => {
    if (v2DataSources) {
        saveV2DataSource(v2DataSources);
    }
};

// save csv
const saveCSVInfo = async (isDirectImport = false) => {
    const hasCTCols =
        $(csvResourceElements.csvSubmitBtn).attr('data-has-ct') === 'true';
    if (!hasCTCols) {
        return;
    }
    if (validateCsvInfo()) {
        // const isV2 = $(csvResourceElements.csvSubmitBtn).attr('data-isV2') === 'true';
        // saveCSVDataSource(isV2);
        await saveCSVDataSource(isDirectImport);
    }
};

// direct import
const directImportBtn = () => {
    const hasCTCols =
        $(csvResourceElements.csvDirectImportBtn).attr('data-has-ct') ===
        'true';
    if (!hasCTCols) {
        return;
    }
    loadingShow();
    saveCSVInfo(true).then(() => {});
};

// const saveDBInfo
// const validateDBInfo
// const saveDBDataSource

// DBInfoの入力をチェックする
const validateDBInfo = () => {
    const dbItemId = currentDSTR.attr(csvResourceElements.dataSrcId);
    const dbType = currentDSTR
        .find('select[name="type"]')
        .val()
        .replace('_EFA', '')
        .replace('_V2', '');
    const dbTypePrefix = dbType.toLowerCase();
    const dbItem = {};
    // Get DB Information
    const itemValues = {
        id: dbItemId,
        name: $(`#${dbTypePrefix}_dbsourcename`).val(),
        type: dbType,
        host: $(`#${dbTypePrefix}_host`).val(),
        port: $(`#${dbTypePrefix}_port`).val(),
        dbname: $(`#${dbTypePrefix}_dbname`).val(),
        schema: $(`#${dbTypePrefix}_schema`).val(),
        username: $(`#${dbTypePrefix}_username`).val(),
        password: $(`#${dbTypePrefix}_password`).val(),
        comment: $(`#${dbTypePrefix}_comment`).val(),
        use_os_timezone: $(`#${dbTypePrefix}_use_os_timezone`).val() === 'true',
    };
    dbItem[dbItemId] = itemValues;
    const validated = DB.validate(dbItem);
    if (!validated.isValid) {
        displayRegisterMessage(`#alert-${dbTypePrefix}-validation`, {
            message: $('#i18nDbSourceEmpty').text(),
            is_error: true,
        });
        return false;
    }

    // データベース名の存在をチェックする。
    if (!validateExistDBName($(`#${dbTypePrefix}_dbsourcename`).val()).isOk) {
        displayRegisterMessage(`#alert-${dbTypePrefix}-validation`, {
            message: validateExistDBName().message,
            is_error: true,
        });
        return false;
    }

    return true;
};

// DBInfoを生成する。
const genDBInfo = (isDirectImport = false) => {
    const dbItemId = currentDSTR.attr(csvResourceElements.dataSrcId);
    let dbType = currentDSTR
        .find('select[name="type"]')
        .val()
        .replace('_EFA', '')
        .replace('_V2', '');
    const domDBPrefix = dbType.toLowerCase();

    const masterDbType = $(`#${domDBPrefix}_master_db_type`).val();

    // Get DB Information
    const dictDBDetail = {
        id: dbItemId,
        name: $(`#${domDBPrefix}_dbsourcename`).val(),
        type: dbType,
        host: $(`#${domDBPrefix}_host`).val(),
        port: $(`#${domDBPrefix}_port`).val(),
        dbname: $(`#${domDBPrefix}_dbname`).val(),
        schema: $(`#${domDBPrefix}_schema`).val(),
        username: $(`#${domDBPrefix}_username`).val(),
        password: $(`#${domDBPrefix}_password`).val(),
        comment: $(`#${domDBPrefix}_comment`).val(),
        use_os_timezone: $(`#${domDBPrefix}_use_os_timezone`).val() === 'true',
    };

    const dictDataSrc = {
        id: dbItemId,
        name: $(`#${domDBPrefix}_dbsourcename`).val(),
        type: dbType,
        master_type: masterDbType ? masterDbType : 'OTHERS',
        comment: $(`#${domDBPrefix}_comment`).val(),
        db_detail: dictDBDetail,
        is_direct_import: isDirectImport,
    };

    return dictDataSrc;
};

// DBInfoの保存を実施する。
const saveDBDataSource = (isDirectImport = false) => {
    const dataSrcId = currentDSTR.attr(csvResourceElements.dataSrcId);
    const dictDataSrc = genDBInfo(isDirectImport);

    // save
    saveDataSource(dataSrcId, dictDataSrc);

    if (isDirectImport) {
        // not close modal until request done
        return;
    }

    // show toast after save
    $('.modal').modal('hide');
};

const saveDBInfo = (element, isDirectImport = false) => {
    if (validateDBInfo()) {
        if (isDirectImport) {
            loadingShow();
        }

        saveDBDataSource(isDirectImport);
    }
};

const savePartitionDataTable = (isCancel = false) => {
    // const isSaveDateTable = $(csvResourceElements.partitionModal).attr('is-save-data-table');
    if (isCancel) {
        $(csvResourceElements.partitionModal).modal('hide');
        return;
    }
    const dataSourceId = $(csvResourceElements.partitionModal).attr(
        'data-source-id',
    );
    const partitionFrom =
        $('#partitionModal').find('[name=partitionFrom] ').val() || null;
    const partitionTo =
        $('#partitionModal').find('[name=partitionTo] ').val() || null;
    const data = {
        data_source_id: dataSourceId,
        partition_from: partitionFrom,
        partition_to: partitionTo,
    };
    $(csvResourceElements.partitionModal).modal('hide');
    fetchData(
        'api/setting/save_data_table_config_direct',
        JSON.stringify(data),
        'POST',
    ).then((res) => {
        if (res.data && res.data.length) {
            res.data.forEach((dataTable) => {
                const cfgDataTableRow = $(
                    dataTableElements.tblDataTableID,
                ).find(`tr[data-datatable-id=${dataTable.id}]`);
                if (cfgDataTableRow.length === 0) {
                    addColumnAttrToTable(dataTable, true);
                }

                // After data table created, process id still unidentified because it will be generated after SCAN_MASTER & SCAN DATA TYPE jobs done
                // const cfgProcessRow = $(procElements.tblProcConfigID).find(`tr[data-proc-id=${dataTable.id}]`);
                // if (cfgProcessRow.length === 0) {
                //     addProcToTable(dataTable.id, dataTable.name, true);
                // }
            });
        }
    });
};

const saveColumnAttribute = () => {
    const dataSourceId = $(csvResourceElements.directColumnAttributeModal).attr(
        'data-source-id',
    );
    const serialCol = dataTableSettingModalElements.serialColAttr.val();
    const datetimeCol = dataTableSettingModalElements.datetimeColAttr.val();
    const orderCol = dataTableSettingModalElements.orderColAttr.val();
    // const dataSourceId = '2102784111400000001';
    const data = {
        data_source_id: dataSourceId,
        partition_from: null,
        partition_to: null,
        serial: serialCol,
        datetime: datetimeCol,
        order: orderCol,
    };
    $(dataTableSettingModalElements.confirmDirectColumnAttribute).modal('hide');
    $(csvResourceElements.directColumnAttributeModal).modal('hide');

    loadingShow(undefined, undefined, LOADING_TIMEOUT_FOR_COLUMN_ATTR);

    fetchData(
        'api/setting/save_data_table_config_direct',
        JSON.stringify(data),
        'POST',
    ).then((res) => {
        if (res.data && res.data.length) {
            res.data.forEach((dataTable) => {
                const cfgDataTableRow = $(
                    dataTableElements.tblDataTableID,
                ).find(`tr[data-datatable-id=${dataTable.id}]`);
                if (cfgDataTableRow.length === 0) {
                    addColumnAttrToTable(dataTable, true);
                }

                // After data table created, process id still unidentified because it will be generated after SCAN_MASTER & SCAN DATA TYPE jobs done
                // const cfgProcessRow = $(procElements.tblProcConfigID).find(`tr[data-proc-id=${dataTable.id}]`);
                // if (cfgProcessRow.length === 0) {
                //     addProcToTable(dataTable.id, dataTable.name, true);
                // }
            });
        }
    });
};

// save db
const saveDBInfo_old = () => {
    const dbItemId = $(e).data('itemId');
    const dbType = $(e).data('dbType');
    const dbItem = {};
    // Get DB Information
    const itemValues = {
        id: dbItemId,
        name: $(`#${dbType}_dbsourcename`).val(),
        type: dbType,
        host: $(`#${dbType}_host`).val(),
        port: $(`#${dbType}_port`).val(),
        dbname: $(`#${dbType}_dbname`).val(),
        schema: $(`#${dbType}_schema`).val(),
        username: $(`#${dbType}_username`).val(),
        password: $(`#${dbType}_password`).val(),
        comment: $(`#${dbType}_comment`).val(),
        use_os_timezone: $(`#${dbType}_use_os_timezone`).val() === 'true',
    };
    dbItem[dbItemId] = itemValues;
    const validated = DB.validate(dbItem);
    // Update instances
    if (validated.isValid) {
        DB.add(dbItem);
        if (dbType === DB_CONFIGS.SQLITE.configs.type) {
            dbItem[dbItemId].dbname = $(`#${dbType}_dbname`).val();
            DB.delete(dbItemId, [
                'host',
                'port',
                'schema',
                'username',
                'password',
            ]);
        }

        // call API here TODO test
        saveDataSource(dbItemId, dbItem);
        $(`#modal-db-${dbType}`).modal('hide');
        return true;
    }
    displayRegisterMessage(`#alert-${dbType}-validation`, {
        message: $('#i18nDbSourceEmpty').text(),
        is_error: true,
    });
    // return false;

    // TODO: Error handle if validate is failed
    // TODO: clean modal's data
};

const updateDBTable = (trId) => {
    const tableData = $(dbElements.tblDbConfigID).DataTable();
    const currentRow = $(dbElements.tblDbConfigID).find(`tr[id='${trId}']`);
    const input = currentRow.find("input[name='master-name']");
    const rowData = tableData.row(currentRow).data();
    input.attr('value', input.val());
    rowData[0] = input.get(0).outerHTML;
    return tableData.row(currentRow).data(rowData).draw();
};

const addDBConfigRow = () => {
    // function to create db_id
    const generateDbID = () => `db_${moment().format('YYMMDDHHmmssSSS')}`;

    // Todo: Refactor i18n
    const dbConfigTextByLang = {
        Setting: $('#i18nSetting').text(),
        DSName: $('#i18nDataSourceName').text(),
        Comment: $('#i18nComment').text(),
    };

    const rowNumber = $(`${dbElements.tblDbConfigID} tbody tr`).length;
    // [FIRST RELEASE] Hide unstable functions
    const limitFeature = !isRunningInWindow ? 'disabled="disabled"' : '';

    // const trID = generateDbID();
    const row = `<tr name="db-info">
        <td class="col-number">${rowNumber + 1}</td>
        <td>
            <input name="name" class="form-control"
             type="text" placeholder="${dbConfigTextByLang.DSName}" value="" ${dragDropRowInTable.DATA_ORDER_ATTR} 
             disabled="disabled">
        </td>
        <td class="text-center">
        <select name="type" class="form-control">
                <option ${limitFeature} value="${DEFAULT_CONFIGS.CSV.id}">${DEFAULT_CONFIGS.CSV.name}</option>
                <option ${limitFeature} value="${DEFAULT_CONFIGS.SQLITE.id}">${DEFAULT_CONFIGS.SQLITE.name}</option>
                <option value="${DEFAULT_CONFIGS.POSTGRESQL.id}">${DEFAULT_CONFIGS.POSTGRESQL.name}</option>
                <option value="${DEFAULT_CONFIGS.MSSQLSERVER.id}">${DEFAULT_CONFIGS.MSSQLSERVER.name}</option>
                <option value="${DEFAULT_CONFIGS.ORACLE.id}">${DEFAULT_CONFIGS.ORACLE.name}</option>
                <option value="${DEFAULT_CONFIGS.MYSQL.id}">${DEFAULT_CONFIGS.MYSQL.name}</option>
                <option value="${DEFAULT_CONFIGS.ORACLE_EFA.id}">${DEFAULT_CONFIGS.ORACLE_EFA.name}</option>
                <option ${limitFeature} value="${DEFAULT_CONFIGS.CSV_V2.id}">${DEFAULT_CONFIGS.CSV_V2.name}</option>
                <option value="${DEFAULT_CONFIGS.SOFTWARE_WORKSHOP.id}">${DEFAULT_CONFIGS.SOFTWARE_WORKSHOP.name}</option>
            </select>
        </td>
        <td class="text-center">
            <button type="button" class="btn btn-secondary db-file icon-btn" onclick="loadDetail(this)"
                data-toggle="modal">
                <i class="fas fa-edit icon-secondary"></i>
            </button>
        </td>
        <td>
            <textarea name="comment"
                class="form-control" rows="1"
                disabled="disabled" placeholder="${dbConfigTextByLang.Comment}"></textarea>
        </td>
        <td class="text-center">
            <button onclick="deleteRow(this,null)" type="button" class="btn btn-secondary icon-btn">
                <i class="fas fa-trash-alt icon-secondary"></i>
            </button>
        </td>
    </tr>`;
    $(`#${dbElements.tblDbConfig} > tbody:last-child`).append(row);
    setTimeout(() => {
        scrollToBottom(`${dbElements.tblDbConfig}_wrap`);
    }, 200);
    return $(`#${dbElements.tblDbConfig} > tbody tr:last-child`);
    // filter code
    // resetDataTable(dbElements.tblDbConfigID, {}, [0, 1, 3], row);
    // updateTableRowNumber(dbElements.tblDbConfig);
};

const deleteRow = (self) => {
    $('#deleteDSModal').modal('show');
    currentDSTR = $(self).closest('tr');
};

const confirmDeleteDS = async () => {
    // save current data source tr element
    const dsCode = currentDSTR.attr(csvResourceElements.dataSrcId);
    $(currentDSTR).remove();

    // update row number
    // updateTableRowNumber(dbElements.tblDbConfig);

    if (!dsCode) {
        return;
    }

    // call backend API to delete
    const deleteDataSource = async (dsCode) => {
        try {
            let result;
            await ajaxWithLog({
                url: 'api/setting/delete_datasource_cfg',
                data: JSON.stringify({ db_code: dsCode }),
                dataType: 'json',
                type: 'POST',
                contentType: false,
                processData: false,
                success: (res) => {
                    res = jsonParse(res);
                    result = getNode(res, ['result', 'deleted_procs']);

                    // Delete record from DataSource
                    $(`#tblDbConfig tr[data-ds-id=${dsCode}]`).remove();

                    // Delete all DataTable in DataSource parent
                    $(`#tblDataTableConfig tr[data-ds-id=${dsCode}]`).remove();

                    // Delete in drop down data source
                    $(
                        `select[name='cfgDataSourceName'] > option[value=${dsCode}]`,
                    ).remove();

                    // Delete in list datasource
                    cfgDS = cfgDS.filter(function (obj) {
                        return obj.id !== Number(dsCode);
                    });

                    // // Delete all Process in DataSource parent
                    // $(`#tblProcConfig tr[data-ds-id=${dsCode}]`)
                    //     .remove();

                    // refresh Vis network
                    // reloadTraceConfigFromDB();
                },
                error: () => {
                    result = null;
                },
            });
            return result;
        } catch (error) {
            return null;
        }
    };

    const deletedProcs = await deleteDataSource(dsCode);

    // delete from UI on success
    if (deletedProcs) {
        $(`#${dsCode}`).remove();

        // remove the deleted DS config in global variable
        DB.delete(dsCode);

        // remove relevant processes in UI
        deletedProcs.forEach((procCode) => {
            $(`#tblProcConfig tr[id=${procCode}]`).remove();
        });
    }
};

const showToastrToProceedProcessConfig = () => {
    const msgContent = `<p>${$('#i18nProceedToProcessConfig').text()}</p>`;
    showToastrMsg(msgContent, MESSAGE_LEVEL.INFO);
};

const getDataTypeFromID = (dataTypeID) => {
    let currentDatType = '';
    Object.keys(DataTypes).forEach((key) => {
        if (DataTypes[key].value === Number(dataTypeID)) {
            currentDatType = DataTypes[key].name;
        }
    });
    return currentDatType;
};

const changeSelectedColumnRaw = (ele, idx) => {
    const defaultOptions = {
        dateTime: {
            checked: false,
            disabled: true,
        },
        auto_increment: {
            checked: false,
            disabled: false,
        },
        serial: {
            checked: false,
            disabled: false,
        },
    };
    const operators = {
        DEFAULT: {
            regex: 'Valid-like',
        },
        REAL: {
            '+': '+',
            '-': '-',
            '*': '*',
            '/': '/',
        },
    };
    // find column in setting table
    const columnName = $(
        `table[name="latestDataTable"] thead th:eq(${idx})`,
    ).find('input')[0];
    const columnInSelectedTable = columnName
        ? $('#processColumnsTable').find(`tr[uid="${columnName.value}"]`)[0]
        : null;

    if (columnInSelectedTable) {
        Object.keys(defaultOptions).forEach((key) => {
            const activeDOM = $(columnInSelectedTable).find(
                `input[name="${key}"]`,
            );
            activeDOM.prop('checked', defaultOptions[key].checked);

            if (Number(ele.value) === DataTypes.DATETIME.value) {
                activeDOM.prop('disabled', false);
            } else {
                activeDOM.prop('disabled', defaultOptions[key].disabled);
            }
        });
        let operatorOpt = '';
        if (
            Number(ele.value) === DataTypes.REAL.value ||
            Number(ele.value) === DataTypes.INTEGER.value
        ) {
            operatorOpt = operators[DataTypes.REAL.name];
            const coefNumber =
                $(columnInSelectedTable).find('input[name="coef"]')[0] || '';
            if (!Number(coefNumber.value)) {
                console.log('operator with string');
            }
        } else {
            operatorOpt = operators.DEFAULT;
        }
        const operatorEle = Object.keys(operatorOpt)
            .map((opt) => `<option value="${opt}">${operatorOpt[opt]}</option>`)
            .join('');
        $(columnInSelectedTable).find('select[name="operator"]').html('');
        $(columnInSelectedTable)
            .find('select[name="operator"]')
            .append('<option>---</option>');
        $(columnInSelectedTable)
            .find('select[name="operator"]')
            .append(operatorEle);

        // update data type
        $(columnInSelectedTable)
            .find('input[name="columnName"]')
            .attr('data-type', getDataTypeFromID(ele.value));
    }

    // update data type
    $(`table[name="latestDataTable"] thead th:eq(${idx})`)
        .find('input')
        .attr('data-type', getDataTypeFromID(ele.value));
};

const getValueByIdOfYoyakugoList = (id, key) => {
    const currentYoyaku = yoyakugoRows.filter((el) => el.id === id)[0];
    return currentYoyaku && currentYoyaku[key];
};

const updateSelectedRows = (disableOnly = false) => {
    const selectedRows = $('#selectedColumnsTable').find('tbody tr');
    let hasDateTimeCheck = false;
    let hasSerialCheck = false;

    // check hasTimeCheck and hasSerialCheck
    selectedRows.each((i, row) => {
        const columnName = $(row).find('input[name=columnName]');
        const yoyakuId = columnName.attr('data-m-data-group-id');
        const isSerial = getValueByIdOfYoyakugoList(yoyakuId, 'is_serial');
        const isDate = getValueByIdOfYoyakugoList(yoyakuId, 'is_datetime');
        hasDateTimeCheck = hasDateTimeCheck || isDate;
        hasSerialCheck = hasSerialCheck || isSerial;
    });

    selectedRows.each((i, row) => {
        const columnName = $(row).find('input[name=columnName]');
        const enNameEl = $(row).find('input[name=englishName]');
        const shownNameEl = $(row).find('input[name=shownName]');
        const dateTimeEl = $(row).find('input[name=dateTime]');
        const dataSerialEl = $(row).find('input[name=dataSerial]');
        const coefEl = $(row).find('input[name=coef]');
        const operatorEl = $(row).find('select[name=operator]');

        const yoyakuId = columnName.attr('data-m-data-group-id');
        const shownName = getValueByIdOfYoyakugoList(yoyakuId, 'name');
        const isDataSerial = getValueByIdOfYoyakugoList(yoyakuId, 'is_serial');
        const isDateTime = getValueByIdOfYoyakugoList(yoyakuId, 'is_datetime');

        if (!disableOnly) {
            $('#selectedColumnsTable')
                .find('input[name=dateTime]')
                .prop('checked', false);
            $('#selectedColumnsTable')
                .find('input[name=dataSerial]')
                .prop('checked', false);

            if (hasDateTimeCheck) {
                // $('#selectedColumnsTable').find('input[name=dateTime]').attr('disabled', true);
            } else {
                // $('#selectedColumnsTable').find('input[name=dateTime]').attr('disabled', false);
            }

            if (hasSerialCheck) {
                $('#selectedColumnsTable')
                    .find('input[name=dataSerial]')
                    .attr('disabled', true);
            } else {
                $('#selectedColumnsTable')
                    .find('input[name=dataSerial]')
                    .attr('disabled', false);
            }
        }

        if (hasDateTimeCheck && isDateTime && !disableOnly) {
            setTimeout(() => {
                dateTimeEl.prop('checked', true);
            }, 50);
        }
        if (hasSerialCheck && isDataSerial && !disableOnly) {
            setTimeout(() => {
                dataSerialEl.prop('checked', true);
            }, 50);
        }

        // disabel all input in row
        if (!yoyakuId || yoyakuId === 'undefined') {
            // $(row).find('input, select').attr('disabled', false);
            shownNameEl.attr('disabled', false);
            enNameEl.attr('disabled', false);
            coefEl.prop('disabled', false);
            operatorEl.prop('disabled', false);
        } else {
            // $(row).find('input, select').attr('disabled', true);
            if (!disableOnly) {
                shownNameEl.val(
                    Number(yoyakuId) === masterDataGroup.HORIZONTAL_DATA
                        ? columnName.val()
                        : shownName,
                ); // In case of Horizontal Data, get truly column name instead of column definition
            }
            shownNameEl.attr('disabled', true);
            enNameEl.attr('disabled', true);
            coefEl.prop('disabled', true);
            operatorEl.prop('disabled', true);
        }
    });
};

const changeSelectedYoyakugoRaw = (ele) => {
    // find column in setting table
    const dataType = getValueByIdOfYoyakugoList($(ele).val(), 'data_type');
    const columnName = $(ele).parent().parent().find('input[type=checkbox]');
    const columnInSelectedTable =
        columnName &&
        $('#selectedColumnsTable').find(`input[value='${columnName.val()}']`);
    const dataTypeColumn = $(ele)
        .parent()
        .parent()
        .find('select.csv-datatype-selection');

    if (columnInSelectedTable) {
        $(columnInSelectedTable).attr('data-m-data-group-id', ele.value);
        $(columnInSelectedTable).attr('data-type', dataType);
    }

    // update data type
    columnName.attr('data-m-data-group-id', ele.value);

    if (ele.value) {
        if (Number(ele.value) !== masterDataGroup.HORIZONTAL_DATA) {
            // In case not Horizontal Data Column, allow choice data type. See in class DataGroupType.HORIZONTAL_DATA
            dataTypeColumn.val((DataTypes[dataType] ?? DataTypes.NONE).value); // Keep current data type for Horizontal Data Column
            dataTypeColumn.prop('disabled', true);
        } else {
            dataTypeColumn.val((DataTypes[dataType] ?? DataTypes.NONE).value); // Keep current data type for Horizontal Data Column
            dataTypeColumn.prop('disabled', false);
        }

        dataTypeColumn.trigger('change');
    } else {
        dataTypeColumn.prop('disabled', false);
    }

    // auto check when select yoyakugo
    if (ele.value && !columnName.prop('checked')) {
        columnName.prop('checked', true).trigger('change');
    }

    updateSelectedRows();
    updateSelectedItems();
};

const updateSubmitBtn = () => {
    let btnClass = ['btn-primary', 'btn-secondary'];
    procModalElements.createOrUpdateProcCfgBtn.attr('data-has-ct', true);
    procModalElements.createOrUpdateProcCfgBtn.removeClass(btnClass[1]);
    procModalElements.createOrUpdateProcCfgBtn.addClass(btnClass[0]);
    // hide err msg
    $(procModalElements.alertProcessNameErrorMsg).css('display', 'none');
};
const parseDataType = (ele, idx, isCfgProcess = false) => {
    // change background color
    changeBackgroundColor(ele);

    // const vals = [...procModalElements.processColumnsSampleDataTableBody.find(`tr:eq(${idx}) .sample-data`)].map(el => $(el));
    const rows = $(ele).parent().parent().find('td[data-original]');
    const dataColumn = $(ele).closest('tr').find('input[type=checkbox]');
    const vals = [];
    for (const row of rows) {
        vals.push($(row));
    }

    const attrName = 'data-original';

    // get data type in case of change all cols from here
    const changeColsTo = $(ele.options[ele.selectedIndex]).data(
        triggerEvents.ALL,
    );
    if (changeColsTo) {
        const seletedValue = $(`#dataTypeTemp-${idx}`).val();
        const allCols = $(ele)
            .parent()
            .parent()
            .parent()
            .find(triggerEvents.SELECT);
        for (let i = idx; i < allCols.length; i++) {
            const selectEle = $(`select#col-${i}`);
            if (selectEle.attr('data-master-column') === 'false') {
                selectEle.val(seletedValue).trigger(triggerEvents.CHANGE);
            }
        }
    } else {
        $(`#dataTypeTemp-${idx}`).val(ele.value);
    }
    let value;
    if (isCfgProcess) {
        value = $(ele).find(`option[value=${ele.value}]`).attr('raw-data-type');
    } else {
        value = ele.getAttribute('value');
    }

    switch (Number(value)) {
        case DataTypes.INTEGER.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseIntData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.INTEGER.name);
            break;
        case DataTypes.SMALL_INT.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseIntData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.SMALL_INT.name);
            break;
        case DataTypes.BIG_INT.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseIntData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.BIG_INT.name);
            break;
        // case DataTypes.CATEGORY_INTEGER.value:
        //     for (const e of vals) {
        //         let val = e.attr(attrName);
        //         val = parseIntData(val);
        //         e.html(val);
        //     }
        //     dataColumn.attr('raw-data-type', DataTypes.CATEGORY_INTEGER.name)
        //     break;
        case DataTypes.BOOLEAN.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseBooleanData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.BOOLEAN.name);
            break;
        case DataTypes.REAL.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseFloatData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.REAL.name);
            break;
        case DataTypes.DATETIME.value: {
            // --- B-Sprint36+80 #5 ---
            // Validate value before do convert
            let formats = [
                new RegExp(/^(\d{4})(\d\d)(\d\d)(\d\d)(\d\d)(\d\d)$/), // gui format
                new RegExp(
                    /^(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d) \+(\d\d):(\d\d)$/,
                ), // with tz
                new RegExp(
                    /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}).(\d{6})$/,
                ), // wo tz
                new RegExp(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z)$/), // sample: 2023-07-11T10:52:10.930000Z
                new RegExp(
                    /^(\d{4})[/\-\s](\d{2})[/\-\s](\d{2})[Tt\s]?(\d{2}):(\d{2}):(\d{2})[Zz]?$/,
                ),
            ];

            for (const e of vals) {
                let val = e.attr(attrName);
                val = trimBoth(String(val));

                // Check lenght of value are valid or not
                //   sample int/real/real value:
                //       - format YYYYMMDDHHMSSS (14 characters): 19000101000000 -> 1900/01/01 00:00:00 +09:00
                //       - format YYYY-MM-DD HH:MM:SS +MM:SS (26 characters): 1900/01/01 00:00:00 +09:00
                // if all formats not matched
                if (formats.every((format) => !(val.match(format) != null))) {
                    if (ele.previousValue == null) {
                        ele.previousValue = $(ele).find('option:selected')[0];
                    }
                    changeBackgroundColor(ele.previousValue);
                    ele.value = ele.previousValue;
                    showToastrMsg(
                        'Can not convert to DATETIME type!!!<br>Please check data format',
                        'Error',
                        (level = 'ERROR'),
                    );
                    return;
                }
            }
            // --- B-Sprint36+80 #5 ---

            let datetimeFormat = formats[0];
            for (const e of vals) {
                let val = e.attr(attrName);
                val = trimBoth(String(val));
                // --- B-Sprint36+80 #5 ---
                val = new Date(
                    val.replace(datetimeFormat, '$4:$5:$6 $2/$3/$1'),
                );
                // --- B-Sprint36+80 #5 ---
                val = moment(val).format(DATE_FORMAT_TZ);
                if (val === 'Invalid date') {
                    val = '';
                }
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.DATETIME.name);
            break;
        }
        case DataTypes.DATE.name:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseDatetimeStr(val, true);
                e.html(val);
            }
            break;
        case DataTypes.TIME.name:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseTimeStr(val);
                e.html(val);
            }
            break;
        case DataTypes.REAL_SEP.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = val.replaceAll(',', '');
                val = parseFloatData(val);
                e.html(val);
            }
            break;
        case DataTypes.INTEGER_SEP.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = val.replaceAll(',', '');
                val = parseIntData(val);
                e.html(val);
            }
            break;
        case DataTypes.EU_REAL_SEP.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = val.replaceAll('.', '');
                val = val.replaceAll(',', '.');
                val = parseFloatData(val);
                e.html(val);
            }
            break;
        case DataTypes.EU_INTEGER_SEP.value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = val.replaceAll('.', '');
                val = val.replaceAll(',', '.');
                val = parseIntData(val);
                e.html(val);
            }
            break;
        default:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = trimBoth(String(val));
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.TEXT.name);
            break;
    }
    // changeSelectedColumnRaw(ele, idx);
    updateSubmitBtn();
    ele.previousValue = value; // --- B-Sprint36+80 #5 ---
};

const bindDBItemToModal = (selectedDatabaseType, dictDataSrc) => {
    // Clear message
    clearOldValue();

    // change databaseType to reused other modal's code but keep original one for setting label
    const originalDatabaseType = selectedDatabaseType;
    switch (selectedDatabaseType) {
        case DEFAULT_CONFIGS.ORACLE_EFA.id: {
            selectedDatabaseType = DEFAULT_CONFIGS.ORACLE.id;
            break;
        }
        case DEFAULT_CONFIGS.CSV_V2.id: {
            selectedDatabaseType = DEFAULT_CONFIGS.CSV.id;
            break;
        }
    }

    let domModalPrefix = selectedDatabaseType.toLowerCase();

    // show upon db types
    switch (selectedDatabaseType) {
        case DEFAULT_CONFIGS.SQLITE.configs.type: {
            $(`#${domModalPrefix}_master_db_type`).val(dictDataSrc.master_type);
            if (!dictDataSrc.db_detail) {
                $(`#${domModalPrefix}_dbname`).val('');
                $(`#${domModalPrefix}_dbsourcename`).val('');
                $(`#${domModalPrefix}_comment`).val('');
            } else {
                // load to modal
                $(`#${domModalPrefix}_dbname`).val(
                    dictDataSrc.db_detail.dbname,
                );
                $(`#${domModalPrefix}_dbsourcename`).val(dictDataSrc.name);
                $(`#${domModalPrefix}_comment`).val(dictDataSrc.comment);
            }
            break;
        }
        case DEFAULT_CONFIGS.CSV.configs.type: {
            // Default selected
            $(csvResourceElements.alertMsgCheckFolder).hide();
            $(csvResourceElements.dataTbl).hide();

            // reset table content
            $(`${csvResourceElements.dataTbl} table thead tr`).empty();

            $(`${dbConfigElements.csvModal} #okBtn`).data(
                'itemId',
                dictDataSrc.id,
            );
            $(`${dbConfigElements.csvModal} #showResources`).data(
                'itemId',
                dictDataSrc.id,
            );

            // Clear old input data
            $(csvResourceElements.isFilePathHidden).val('');
            $(csvResourceElements.folderUrlInput).val('');
            $(csvResourceElements.folderUrlInput).data('originValue', '');
            $(csvResourceElements.folderV2UrlInput).val('');
            $(csvResourceElements.folderV2UrlInput).data('originValue', '');
            $(csvResourceElements.folderV2HistoryUrlInput).val('');
            $(csvResourceElements.folderV2HistoryUrlInput).data(
                'originValue',
                '',
            );

            $(csvResourceElements.fileName).text('');

            $(csvResourceElements.masterDbType).val(dictDataSrc.master_type);

            if (originalDatabaseType === DEFAULT_CONFIGS.CSV_V2.id) {
                $(`#modal-db-${domModalPrefix} .saveDBInfoBtn`).attr(
                    'data-isV2',
                    true,
                );
                $(`.saveDBInfoBtn`).attr('data-isV2', true);
                $(csvResourceElements.showResourcesBtnId).attr(
                    'data-isV2',
                    true,
                );
            }

            if (dictDataSrc.csv_detail) {
                if (
                    dictDataSrc.master_type === DEFAULT_CONFIGS.V2.master_type
                ) {
                    $(csvResourceElements.folderV2UrlInput).val(
                        dictDataSrc.csv_detail.directory,
                    );
                    $(csvResourceElements.folderV2UrlInput).data(
                        'originValue',
                        dictDataSrc.csv_detail.directory,
                    );
                    $(csvResourceElements.folderV2HistoryUrlInput).val(
                        dictDataSrc.csv_detail.second_directory,
                    );
                    $(csvResourceElements.folderV2HistoryUrlInput).data(
                        'originValue',
                        dictDataSrc.csv_detail.directory,
                    );
                }
                if (dictDataSrc.csv_detail.directory) {
                    $(csvResourceElements.folderUrlInput).val(
                        dictDataSrc.csv_detail.directory,
                    );
                    $(csvResourceElements.folderUrlInput).data(
                        'originValue',
                        dictDataSrc.csv_detail.directory,
                    );
                }
                $(csvResourceElements.isFilePathHidden).val(
                    dictDataSrc.csv_detail.is_file_path,
                );

                // Update default delimiter radio button by DEFAULT_CONFIGS
                if (dictDataSrc.csv_detail.delimiter === 'CSV') {
                    $(csvResourceElements.csv)[0].checked = true;
                } else if (dictDataSrc.csv_detail.delimiter === 'TSV') {
                    $(csvResourceElements.tsv)[0].checked = true;
                } else if (dictDataSrc.csv_detail.delimiter === 'SMC') {
                    $(csvResourceElements.smc)[0].checked = true;
                } else {
                    $(csvResourceElements.fileTypeAuto)[0].checked = true;
                }
                // line skipping
                const skipHead = dictDataSrc.csv_detail.skip_head;
                const isDummyHeader = dictDataSrc.csv_detail.dummy_header;
                const lineSkip =
                    skipHead == 0 && !isDummyHeader ? '' : skipHead;
                $(csvResourceElements.skipHead).val(lineSkip);

                $(csvResourceElements.csvNRows).val(
                    dictDataSrc.csv_detail.n_rows,
                );
                $(csvResourceElements.csvIsTranspose).prop(
                    'checked',
                    !!dictDataSrc.csv_detail.is_transpose,
                );

                // load optional function
                if (dictDataSrc.csv_detail.etl_func) {
                    $(csvResourceElements.optionalFunction)
                        .select2()
                        .val(dictDataSrc.csv_detail.etl_func)
                        .trigger('change');
                }
            }

            // load master name + comment
            $('#csvDBSourceName').val(dictDataSrc.name);
            $('#csvComment').val(dictDataSrc.comment);
            if (dictDataSrc.id) {
                if (originalDatabaseType !== DEFAULT_CONFIGS.CSV_V2.id) {
                    $('#resourceLoading').show();
                    showResources(selectedDatabaseType);
                }
            }
            break;
        }
        default: {
            if (!dictDataSrc.db_detail) {
                break;
            }

            // Todo: Refactor Modal's inputs ID
            $(`#${domModalPrefix}_dbsourcename`).val(dictDataSrc.name);
            $(`#${domModalPrefix}_comment`).val(dictDataSrc.comment);
            $(`#${domModalPrefix}_host`).val(dictDataSrc.db_detail.host);
            $(`#${domModalPrefix}_port`).val(dictDataSrc.db_detail.port);
            $(`#${domModalPrefix}_dbname`).val(dictDataSrc.db_detail.dbname);
            $(`#${domModalPrefix}_schema`).val(dictDataSrc.db_detail.schema);
            $(`#${domModalPrefix}_username`).val(
                dictDataSrc.db_detail.username,
            );
            $(`#${domModalPrefix}_password`).val(
                dictDataSrc.db_detail.password,
            );
            $(`#${domModalPrefix}_use_os_timezone`).val(
                dictDataSrc.db_detail.use_os_timezone,
            );
            $(eles.useOSTZOption).prop(
                'checked',
                dictDataSrc.db_detail.use_os_timezone,
            );
            $(eles.useOSTZOption).data(
                'previous-value',
                dictDataSrc.db_detail.use_os_timezone,
            );
            $(`#${domModalPrefix}_master_db_type`).val(dictDataSrc.master_type);

            const dbTypeLower = dictDataSrc.type
                ? dictDataSrc.type.toLowerCase()
                : '';
            $(eles.useOSTZConfirmBtn).data('dbType', dbTypeLower);
            $('#btn-test-db-conn-psql').attr('data-dsid', dictDataSrc.id);
            if (
                dictDataSrc.master_type == DEFAULT_CONFIGS.ORACLE.master_type &&
                dictDataSrc.db_detail.type ==
                    DEFAULT_CONFIGS.ORACLE.configs.type
            ) {
                $('#btnDirectDB').hide();
                $('#btnSaveDB').show();
            } else if (
                dictDataSrc.master_type ==
                DEFAULT_CONFIGS.ORACLE_EFA.master_type
            ) {
                $('#btnDirectDB').show();
                $('#btnSaveDB').show();
            } else {
                $('#btnSaveDB').show();
            }
            break;
        }
    }

    //  TODO: refactor modal ID
    $(`#modal-db-${domModalPrefix} input`).data('itemId', dictDataSrc.id);
    $(`#modal-db-${domModalPrefix} select`).data('itemId', dictDataSrc.id);
    $(`#modal-db-${domModalPrefix} .saveDBInfoBtn`).data(
        'itemId',
        dictDataSrc.id,
    );
    $(`#modal-db-${domModalPrefix} .saveDBInfoBtn`).data(
        'dbType',
        dictDataSrc.type,
    );
    $(`#modal-db-${domModalPrefix}`).modal('show');

    // change label for eFA and V2
    const modalTitleEle = $(`#modal-db-${domModalPrefix} h4.modal-title`);
    switch (originalDatabaseType) {
        case DEFAULT_CONFIGS.ORACLE_EFA.id: {
            const newTextLabel = `${$('#i18nSetting').text()}: eFA(Oracle)`;
            modalTitleEle.text(newTextLabel);
            break;
        }
        case DEFAULT_CONFIGS.CSV_V2.id: {
            dbElements.CSVTitle.text(
                dbElements.CSVTitle.text().replace('CSV/TSV', 'V2 CSV'),
            );
            $(csvResourceElements.csvDiv).hide();
            $(csvResourceElements.v2SourceDiv).show();
            $(csvResourceElements.v2HistoryDiv).show();
            $(csvResourceElements.skipHead).parent().hide();
            $(csvResourceElements.csvNRows).parent().hide();
            $(csvResourceElements.csvIsTranspose).parent().hide();
            break;
        }
        case DEFAULT_CONFIGS.CSV.id: {
            dbElements.CSVTitle.text(
                dbElements.CSVTitle.text().replace('V2 CSV', 'CSV/TSV'),
            );
            $(csvResourceElements.csvDiv).show();
            $(csvResourceElements.v2SourceDiv).hide();
            $(csvResourceElements.v2HistoryDiv).hide();
            $(csvResourceElements.skipHead).parent().show();
            $(csvResourceElements.csvNRows).parent().show();
            $(csvResourceElements.csvIsTranspose).parent().show();
            break;
        }
        case DEFAULT_CONFIGS.ORACLE.id: {
            const newTextLabel = `${$('#i18nSetting').text()}: (Oracle)`;
            modalTitleEle.text(newTextLabel);
            break;
        }
    }

    addAttributeToElement();
};

// preview data automatically
$(csvResourceElements.folderUrlInput).on('change', function () {
    $('#resourceLoading').show();
    csvType = 'csv';
    showResources('csv');
});

$(csvResourceElements.folderV2UrlInput).on('change', function () {
    $('#resourceLoading').show();
    csvType = 'csv_v2';
    showResources('csv_v2');
});

$(csvResourceElements.folderV2HistoryUrlInput).on('change', function () {
    $('#resourceLoading').show();
    csvType = 'v2_history';
    showResources('v2_history');
});

const checkDBConnection = (dbType, html, msgID) => {
    // reset connection status text
    $(`#${msgID}`).html('');
    $(`#${msgID}`).addClass('spinner-grow');
    $(`#${msgID}`).removeClass('text-danger');
    $(`#${msgID}`).removeClass('text-success');

    // get form data
    const data = {
        db: {},
    };

    let dbName = '';
    if (dbType === DB_CONFIGS.SQLITE.configs.type) {
        const filePath = $(`#${dbType}_dbname`).val();
        dbName = filePath;
    } else {
        dbName = $(`#modal-db-${dbType} input[name="${dbType}_dbname"]`).val();
    }
    Object.assign(data.db, {
        id: $('#btn-test-db-conn-psql').attr('data-dsid') || null,
        host: $(`#modal-db-${dbType} input[name="${dbType}_host"]`).val(),
        port: $(`#modal-db-${dbType} input[name="${dbType}_port"]`).val(),
        schema: $(`#modal-db-${dbType} input[name="${dbType}_schema"]`).val(),
        username: $(
            `#modal-db-${dbType} input[name="${dbType}_username"]`,
        ).val(),
        password: $(
            `#modal-db-${dbType} input[name="${dbType}_password"]`,
        ).val(),
        dbname: dbName,
        db_type: dbType,
    });

    fetchWithLog('api/setting/check_db_connection', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
    })
        .then((response) => response.clone().json())
        .then((json) => {
            displayTestDBConnectionMessage(msgID, json.flask_message);
        });
};

const loadDetail = (self) => {
    // save current data source tr element
    currentDSTR = $(self).closest('tr');
    const dataSrcId = currentDSTR.attr(csvResourceElements.dataSrcId);
    let dsType = currentDSTR.find('select[name="type"]').val();

    let jsonDictDataSrc = {};
    switch (dsType) {
        case DEFAULT_CONFIGS.SQLITE.id: {
            jsonDictDataSrc = {
                id: null,
                comment: '',
                db_detail: DEFAULT_CONFIGS.SQLITE.configs,
                master_type: DEFAULT_CONFIGS.SQLITE.master_type,
            };
            break;
        }
        case DEFAULT_CONFIGS.POSTGRESQL.id: {
            jsonDictDataSrc = {
                id: null,
                comment: '',
                db_detail: DEFAULT_CONFIGS.POSTGRESQL.configs,
                master_type: DEFAULT_CONFIGS.POSTGRESQL.master_type,
            };
            break;
        }
        case DEFAULT_CONFIGS.MSSQLSERVER.id: {
            jsonDictDataSrc = {
                id: null,
                comment: '',
                db_detail: DEFAULT_CONFIGS.MSSQLSERVER.configs,
                master_type: DEFAULT_CONFIGS.MSSQLSERVER.master_type,
            };
            break;
        }
        case DEFAULT_CONFIGS.ORACLE.id: {
            jsonDictDataSrc = {
                id: null,
                comment: '',
                db_detail: DEFAULT_CONFIGS.ORACLE.configs,
                master_type: DEFAULT_CONFIGS.ORACLE.master_type,
            };
            break;
        }
        case DEFAULT_CONFIGS.MYSQL.id: {
            jsonDictDataSrc = {
                id: null,
                comment: '',
                db_detail: DEFAULT_CONFIGS.MYSQL.configs,
                master_type: DEFAULT_CONFIGS.MYSQL.master_type,
            };
            break;
        }
        case DEFAULT_CONFIGS.CSV.id: {
            jsonDictDataSrc = {
                id: null,
                comment: '',
                csv_detail: DEFAULT_CONFIGS.CSV.configs,
                master_type: DEFAULT_CONFIGS.CSV.master_type,
            };
            break;
        }
        case DEFAULT_CONFIGS.ORACLE_EFA.id: {
            dsType = DEFAULT_CONFIGS.ORACLE_EFA.id;
            jsonDictDataSrc = {
                id: null,
                comment: '',
                db_detail: DEFAULT_CONFIGS.ORACLE_EFA.configs,
                master_type: DEFAULT_CONFIGS.ORACLE_EFA.master_type,
            };
            break;
        }
        case DEFAULT_CONFIGS.CSV_V2.id: {
            dsType = DEFAULT_CONFIGS.CSV_V2.id;
            jsonDictDataSrc = {
                id: null,
                comment: '',
                master_type: DEFAULT_CONFIGS.CSV_V2.master_type,
                csv_detail: DEFAULT_CONFIGS.CSV_V2.configs,
            };
            break;
        }
        case DEFAULT_CONFIGS.SOFTWARE_WORKSHOP.id: {
            dsType = DEFAULT_CONFIGS.SOFTWARE_WORKSHOP.id;
            jsonDictDataSrc = {
                id: null,
                comment: '',
                master_type: DEFAULT_CONFIGS.SOFTWARE_WORKSHOP.master_type,
                db_detail: DEFAULT_CONFIGS.SOFTWARE_WORKSHOP.configs,
            };
            break;
        }
    }

    // When click (+) to create blank item
    if (dataSrcId === null || dataSrcId === undefined) {
        bindDBItemToModal(dsType, jsonDictDataSrc);
    } else {
        const url = new URL(
            `${csvResourceElements.apiLoadDetail}/${dataSrcId}`,
            window.location.href,
        ).href;
        fetchWithLog(url, {
            method: 'GET',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
        })
            .then((response) => response.clone().json())
            .then((json) => {
                let sameDataSourceType = dsType === json.type;
                if (json) {
                    switch (dsType) {
                        case DEFAULT_CONFIGS.ORACLE_EFA.id: {
                            dsType = DEFAULT_CONFIGS.ORACLE_EFA.id;
                            sameDataSourceType = json.master_type === 'EFA';
                            break;
                        }
                        case DEFAULT_CONFIGS.CSV_V2.id: {
                            dsType = DEFAULT_CONFIGS.CSV_V2.id;
                            sameDataSourceType = json.master_type === 'V2';
                            break;
                        }
                    }

                    if (!sameDataSourceType) {
                        // clear all json dictionary
                        json = jsonDictDataSrc;
                    }

                    bindDBItemToModal(dsType, json);
                }
            });
    }
};

// handle searching data source name
const searchDataSourceName = (element) => {
    const inputDataSourceName = element.currentTarget.value.trim();

    // when input nothing or only white space characters, show all data source in list
    if (inputDataSourceName.length === 0) {
        $('input[name="name"]').each(function () {
            $(this.closest('tr[name="db-info"]')).show();
        });

        return;
    }

    // find and show data source who's name is same with user input
    $('input[name="name"]').each(function () {
        const currentRow = $(this.closest('tr[name="db-info"]'));
        if (this.value.match(inputDataSourceName)) currentRow.show();
        else currentRow.hide();
    });
};

const getCheckedV2Processes = (name = 'v2Process') => {
    return [
        ...$(`input[name=${name}]:checked`).map((i, el) => $(el).val()),
    ].filter((val) => val !== 'All');
};

const getV2ProcessData = (dictDataSrc) => {
    const v2Datasources = [];
    const v2SelectedProcess = getCheckedV2Processes();
    if (v2SelectedProcess.length) {
        v2SelectedProcess.forEach((processName) => {
            const subDatasourceByProcess = jsonParse(
                JSON.stringify(dictDataSrc),
            );
            const suffix =
                processName === DUMMY_V2_PROCESS_NAME ? '' : `_${processName}`;
            subDatasourceByProcess.name = `${subDatasourceByProcess.name}${suffix}`;
            subDatasourceByProcess.csv_detail.process_name = processName;
            subDatasourceByProcess.csv_detail.auto_link = false;
            v2Datasources.push(subDatasourceByProcess);
        });
    }
    return v2Datasources;
};

function autoSetNameForDataSource(fileName) {
    const dbsName = $(dbConfigElements.csvDBSourceName).val();
    if (!userEditedDSName || !dbsName) {
        const fullPath = fileName.replace(/\\/g, '//');
        const lastFolderName = fullPath.match(/([^/]*)\/*$/)[1];
        // autogen datasource name by latest folder name
        $(dbConfigElements.csvDBSourceName).val(lastFolderName);
    }
}

$(() => {
    // drag & drop for tables
    $(`#${dbElements.tblDbConfig} tbody`).sortable({
        helper: dragDropRowInTable.fixHelper,
        update: dragDropRowInTable.updateOrder,
    });

    // resort table
    dragDropRowInTable.sortRowInTable(dbElements.tblDbConfig);

    $(csvResourceElements.connectResourceBtn).on('click', () => {
        $(csvResourceElements.alertInternalError).hide();
        const folderUrl = $(csvResourceElements.folderUrlInput).val();
        const originFolderUrl = $(csvResourceElements.folderUrlInput).data(
            'originValue',
        );
        csvType = 'csv';
        checkFolderResources(folderUrl, originFolderUrl).then(() => {});
    });

    $(csvResourceElements.connectV2ResourceBtn).on('click', () => {
        const folderUrl = $(csvResourceElements.folderV2UrlInput).val();
        const originFolderUrl = $(csvResourceElements.folderV2UrlInput).data(
            'originValue',
        );
        csvType = 'csv_v2';
        checkFolderResources(folderUrl, originFolderUrl).then(() => {});
    });

    $(csvResourceElements.connectV2HistoryResourceBtn).on('click', () => {
        const folderUrl = $(csvResourceElements.folderV2HistoryUrlInput).val();
        const originFolderUrl = $(
            csvResourceElements.folderV2HistoryUrlInput,
        ).data('originValue');
        csvType = 'v2_history';
        checkFolderResources(folderUrl, originFolderUrl).then(() => {});
    });
    $(csvResourceElements.showResourcesBtnId).on('click', () => {
        $('#resourceLoading').show();
        showResources(csvType);
    });

    $(csvResourceElements.csvConfirmRegister).on('click', (e) => {
        saveCSVDataSource().then(() => {});
    });

    // Multiple modal
    $(document).on('show.bs.modal', '.modal', (e) => {
        const activedModalsNum = $('.modal:visible').length;
        // zindex of multiple modals start at 1040
        let zIndex = 1040;
        if (activedModalsNum) {
            // get zindex of last modal
            const highestZIndex =
                $('.modal:visible')[activedModalsNum - 1].style.zIndex || 0;
            zIndex = Number(highestZIndex) + 10;
        }
        const backdropZIndex = zIndex - 1;

        $(e.currentTarget).css('z-index', zIndex);
        setTimeout(() => {
            $('.modal-backdrop')
                .not('.modal-stack')
                .css('z-index', backdropZIndex)
                .addClass('modal-stack');
            // change zindex for last backdrop
            // backdrop 1: 1039, backdrop 2: 1049, ...
            $('.modal-backdrop:eq(-1)').css('z-index', backdropZIndex);
        }, 0);
    });

    // add an empty db row if there is no db config
    setTimeout(() => {
        const countDataSource = $(
            `${dbElements.tblDbConfigID} tbody tr[name=db-info]`,
        ).length;
        if (!countDataSource) {
            addDBConfigRow();
        }
    }, 500);
    $(dbElements.divDbConfig)[0].addEventListener(
        'mouseup',
        handleMouseUp,
        false,
    );

    $(dbConfigElements.csvDBSourceName).on('mouseup', () => {
        userEditedDSName = true;
    });
    $(csvResourceElements.folderUrlId).on('change', (e) => {
        const fileName = $(e.currentTarget).val().replace(/"/g, '');
        e.target.value = fileName;
        autoSetNameForDataSource(fileName);
    });

    $(csvResourceElements.folderV2UrlId).on('change', (e) => {
        const fileName = $(e.currentTarget).val().replace(/"/g, '');
        e.target.value = fileName;
        autoSetNameForDataSource(fileName);
    });

    $(csvResourceElements.folderV2HistoryUrlId).on('change', (e) => {
        if ($(csvResourceElements.folderV2UrlId).val() !== '') {
            return;
        }

        const fileName = $(e.currentTarget).val().replace(/"/g, '');
        e.target.value = fileName;
        autoSetNameForDataSource(fileName);
    });

    // add event to dbElements.txbSearchDataSourceName
    $(dbElements.txbSearchDataSourceName).keyup(searchDataSourceName);

    // convert skipHead to blank if value is out of range
    $(csvResourceElements.skipHead).on('change', (ele) => {
        const skipHead = $(ele.currentTarget);
        if (skipHead.is(':out-of-range')) {
            skipHead.val('');
        }
    });

    // searchDataSource
    onSearchTableContent('searchDataSource', 'tblDbConfig');
    onSearchTableContent('searchProcConfig', 'tblProcConfig');
    sortableTable('tblDbConfig', [0, 1, 2, 4], 510, true);
    sortableTable('tblProcConfig', [0, 1, 2, 3, 5], 510, true);
    onSearchTableContent('searchCfgDataTable', 'tblDataTableConfig');
    sortableTable('tblDataTableConfig', [0, 1, 2, 4], 510, true);
});
