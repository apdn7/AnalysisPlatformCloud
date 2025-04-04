let currentProcItem;
const currentProcData = {};
const IS_CONFIG_PAGE = true;
let dicOriginDataType = {};
let dicProcessCols = {};

const procElements = {
    tblProcConfig: 'tblProcConfig',
    tblProcConfigID: '#tblProcConfig',
    tableProcList: '#tblProcConfig tbody',
    procListChild: '#tblProcConfig tbody tr',
    divProcConfig: '#accordionPC',
    fileName: 'input[name=fileName]',
    fileNameInput: '#fileNameInput',
    fileNameBtn: '#fileNameBtn',
    dbTableList: '#dbTableList',
    fileInputPreview: '#fileInputPreview',
};

const i18n = {
    statusDone: $('#i18nStatusDone').text(),
    statusImporting: $('#i18nStatusImporting').text(),
    statusFailed: $('#i18nStatusFailed').text(),
    statusPending: $('#i18nStatusPending').text(),
    validLike: $('#i18nValidLike').text(),
    reachFailLimit: $('#i18nReachFailLimit').text(),
    noCTCol: $('#i18nNoCTCol').text(),
    noCTColProc: $('#i18nNoCTColPrc').text(),
};
const JOB_STATUS = {
    DONE: {
        title: i18n.statusDone,
        class: 'check green',
    },
    FAILED: {
        title: i18n.statusFailed,
        class: 'exclamation-triangle yellow',
    },
    KILLED: {
        title: i18n.statusFailed,
        class: 'exclamation-triangle yellow',
    },
    PROCESSING: {
        title: i18n.statusImporting,
        class: 'spinner fa-spin',
    },
    PENDING: {
        title: i18n.statusPending,
        class: 'spinner fa-spin',
    },
};

const CATEGORY_ERROR_TEXT = {
    OLD_UNIQUE_VALUE_EXCEED: 'OLD UNIQUE VALUE EXCEED',
    NEW_UNIQUE_VALUE_EXCEED: 'NEW UNIQUE VALUE EXCEED',
};

const updateBackgroundJobs = (json) => {
    if (_.isEmpty(json)) {
        return;
    }

    Object.values(json).forEach((row) => {
        const statusClass =
            JOB_STATUS[row.status].class || JOB_STATUS.FAILED.class;
        let statusTooltip =
            JOB_STATUS[row.status].title || JOB_STATUS.FAILED.title;
        if (row.data_type_error) {
            statusTooltip = $(baseEles.i18nJobStatusMsg).text();
            statusTooltip = statusTooltip.replace(
                '__param__',
                row.db_master_name,
            );
        }
        const updatedStatus = `<div class="align-middle text-center" data-st="${statusClass}">
            <div class="" data-toggle="tooltip" data-placement="top" title="${statusTooltip}">
                <i class="fas fa-${statusClass} status-i"></i>
            </div>
        </div>`;

        const jobStatusEle = $(`#proc_${row.proc_id} .process-status`).first();
        if (
            jobStatusEle &&
            jobStatusEle.html() &&
            jobStatusEle.html().trim() !== ''
        ) {
            if (jobStatusEle.attr('data-status') !== row.status) {
                jobStatusEle.html(updatedStatus);
            }
        } else {
            jobStatusEle.html(updatedStatus);
        }
        jobStatusEle.attr('data-status', row.status);
    });
};

const deleteProcess = (procItem) => {
    currentProcItem = $(procItem).closest('tr');
    const procId = currentProcItem.data('proc-id');
    if (procId) {
        $('#btnDeleteProc').attr('data-item-id', procId);
        $('#deleteProcModal').modal('show');
    } else {
        // remove empty row
        $(currentProcItem).remove();
        updateTableRowNumber(procElements.tblProcConfig);
    }
};

const removeProcessConfigRow = (procId) => {
    $(`#proc_${procId}`).remove();
};

const confirmDelProc = () => {
    const procId = $('#btnDeleteProc').attr('data-item-id');
    fetchWithLog('api/setting/delete_process', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ proc_id: procId }), // example: { proc_id: 3 }
    })
        .then((response) => response.clone().json())
        .catch((e) => {
            console.error(e);
        });
};

const disableDatatime = (data_type, isAddNew) => {
    if (!isAddNew) return ' disabled';
    return data_type === DataTypes.DATETIME.name ? '' : ' disabled';
};

const genColConfigHTML = (col, isAddNew = true) => {
    const isYoyakugo = col.column_type && col.column_type !== 99;
    const isDateTime = col.is_get_date ? ' checked' : '';
    let isSerial = col.is_serial_no ? ' checked' : '';
    const isAutoIncrement = col.is_auto_increment ? ' checked' : '';
    const disabled = isYoyakugo ? 'disabled' : '';
    const isNumeric = isNumericDatatype(col.data_type);
    const [numericOperators, textOperators, coefHTML] = createOptCoefHTML(
        col.operator,
        col.coef,
        isNumeric,
        disabled,
    );
    const disableDatetime = disableDatatime(col.data_type, isAddNew);
    const isDummyDatetime = col.is_dummy_datetime ? true : false;
    const disableSerial = ['d', 'r'].includes(col.data_type) ? 'disabled' : '';

    // if v2 col_name is シリアルNo -> auto check
    if (!isSerial && isAddNew) {
        isSerial = /^.*シリアル|serial.*$/.test(
            col.column_name.toString().toLowerCase(),
        )
            ? 'checked'
            : '';
    }

    return `<tr name="selectedColumn" id="selectedColumn${col.column_name}" uid="${col.column_name}">
        <td class="col-number"></td>
        <td class="pr-row">
            <input data-type="${col.data_type}" name="${procModalElements.columnName}"
                class="form-control" value="${col.column_name}" disabled>
        </td>
        <td>
            <div class="custom-control custom-checkbox" style="text-align:center; vertical-align:middle">
                <input id="datetimeColumn${col.column_name}"
                    class="custom-control-input" is-dummy-datetime="${isDummyDatetime}" 
                    type="checkbox" name="${procModalElements.dateTime}" ${isDateTime} ${disableDatetime}>
                <label class="custom-control-label" for="datetimeColumn${col.column_name}"></label>
                <input id="isDummyDatetime${col.column_name}" type="hidden" name="${procModalElements.isDummyDatetime}"
                    value="${isDummyDatetime}">
            </div>
        </td>
        <td>
            <div class="custom-control custom-checkbox" style="text-align:center; vertical-align:middle">
                <input id="serialColumn${col.column_name}"
                    class="custom-control-input" type="checkbox" name="${procModalElements.serial}" ${isSerial}>
                <label class="custom-control-label" for="serialColumn${col.column_name}"></label>
            </div>
        </td>
        <td class="pr-row"><input name="${procModalElements.englishName}" class="form-control" type="text" value="${isDateTime && !isDummyDatetime && isAddNew ? 'Datetime' : col.name_en}"></td>
        <td class="pr-row"><input name="${procModalElements.japaneseName}" data-shown-name="1" class="form-control" type="text" value="${col.name_jp || ''}"></td>
        <td class="pr-row"><input name="${procModalElements.localName}" data-shown-name="1" class="form-control" type="text" value="${col.name_local || ''}"></td>
        <td class="pr-row">
            <select name="${procModalElements.operator}" class="form-control" type="text" ${disabled}>
                <option value="">---</option>
                ${isNumeric ? numericOperators : textOperators}
            </select>
        </td>
        <td class="pr-row-sm pr-row">
            ${coefHTML}
        </td>
    </tr>`;
};

const getProcInfo = (procId, dataTypeErrorColIds = []) => {
    ajaxWithLog({
        url: `api/setting/proc_config/${procId}`,
        type: 'GET',
        cache: false,
        success(res) {
            loading.hide();

            res = jsonParse(res);
            procModalElements.proc.val(res.data.name_en);
            procModalElements.procJapaneseName.val(res.data.name_jp || '');
            procModalElements.procLocalName.val(res.data.name_local || '');
            procModalElements.procID.val(res.data.id);
            procModalElements.comment.val(res.data.comment);
            procModalElements.tables.val(res.data.table_name);
            procModalElements.dsID.val(res.data.data_source_id);
            procModalElements.fileName.val(res.data.file_name);
            procModalElements.isShowFileName.prop(
                'checked',
                !!res.data.is_show_file_name,
            );
            procModalElements.procDateTimeFormatInput.val(
                res.data.datetime_format || '',
            );
            procModalElements.isShowFileName.prop('disabled', false);
            if (!res.data.is_csv) {
                procModalElements.isShowFileName.prop('checked', false);
                procModalElements.isShowFileName.prop('disabled', true);
            } else {
                if (res.data.is_show_file_name === null) {
                    // default uncheck
                    procModalElements.isShowFileName.prop('checked', false);
                } else if (res.data.is_show_file_name === true) {
                    procModalElements.isShowFileName.prop('checked', true);
                    procModalElements.isShowFileName.prop('disabled', true);
                } else {
                    procModalElements.isShowFileName.prop('checked', false);
                    procModalElements.isShowFileName.prop('disabled', true);
                }
            }

            currentProcData.ds_id = res.data.data_source_id;
            currentProcData.table_name = res.data.table_name;

            const dsLength = $(
                '#procSettingModal select[name=cfgProcDatabaseName] option',
            ).length;
            if (dsLength > 0) {
                $(
                    `#procSettingModal select[name=cfgProcDatabaseName] option[value="${res.data.data_source_id}"]`,
                )
                    .prop('selected', true)
                    .change();
            }
            $('#procSettingModal').modal('show');
            resetDicOriginData();
            // let rowHtml = '';
            // // TODO: use raw_data_type
            // const convertDataType = {
            //     i: 'INTEGER',
            //     d: 'DATETIME',
            //     r: 'REAL',
            //     t: 'TEXT',
            //     date: 'DATE',
            //     time: 'TIME',
            // };
            // res.data.columns.forEach((row) => {
            //     row['data_type'] = convertDataType[row.data_type];
            // });

            validateSelectedColumnInput();

            // validate coef when showing selected columns
            validateAllCoefs();

            // handling english name onchange
            handleEnglishNameChange(procModalElements.proc);
            handleEnglishNameChange($(procModalElements.systemNameInput));

            // disable datetime + as key columns
            validateFixedColumns();

            // show warning to reset data link config when change as link id
            validateCheckBoxesAll();

            // update row number
            updateTableRowNumber(null, $('table[name=processColumnsTable]'));
            setTimeout(() => {
                generateProcessList(
                    res.data.columns,
                    res.rows,
                    [],
                    undefined,
                    undefined,
                    undefined,
                    res.data.is_imported,
                );
                document
                    .querySelectorAll('div.config-data-type-dropdown')
                    .forEach((dataTypeDropdownElement) =>
                        DataTypeDropdown_Controller.addEvents(
                            dataTypeDropdownElement,
                        ),
                    );
                handleShowFileNameColumn(procModalElements.isShowFileName[0]);
                showHideReRegisterBtn(res.data.is_imported);
                FunctionInfo.getAllFunctionInfosApi(
                    procId,
                    res.col_id_in_funcs,
                ).then(FunctionInfo.loadFunctionListTableAndInitDropDown);
            }, 200);
            // generateProcessList(res.data.columns, res.rows, []);
            $(procModalElements.autoSelectAllColumn).attr('checked', true);

            // $('#procSettingModal').modal('show');
            currentProcColumns = res.col;
            currentProcDataCols = res.data.columns;
            currentProcess = res.data;
            currentProcessId = res.data.id;

            // date time format
            initDatetimeFormatCheckboxAndInput();
        },
    });
};

const showHideReRegisterBtn = (isNewProcess) => {
    if (isNewProcess) {
        procModalElements.createOrUpdateProcCfgBtn.css('display', 'none');
        procModalElements.reRegisterBtn.css('display', 'block');
    } else {
        procModalElements.createOrUpdateProcCfgBtn.css('display', 'block');
        procModalElements.reRegisterBtn.css('display', 'none');
    }
};

const isAddNewMode = () => isEmpty(procModalElements.procID.val() || null);

const showProcSettingModal = (
    procItem,
    dbsId = null,
    dataTypeErrorColIds = [],
) => {
    $(functionConfigElements.collapseFunctionConfig).collapse('hide');
    FunctionInfo.resetInputFunctionInfo().then(
        FunctionInfo.removeAllFunctionRows,
    );
    clearWarning();
    cleanOldData();
    showHideReRegisterBtn();

    // clear user editted input flag
    userEditedProcName = false;

    // clear old procInfo
    currentProcColumns = null;
    $(procModalElements.prcSM).html('');
    $(procModalElements.settingContent).removeClass('hide');
    $(procModalElements.prcSM).parent().addClass('hide');

    currentProcItem = $(procItem).closest('tr');
    const procId = currentProcItem.data('proc-id');
    const loading = $('.loading');

    loading.show();
    handleEnglishNameChange(procModalElements.proc);

    if (procId) {
        // getPullSampleData(procId);
        getProcInfo(procId, dataTypeErrorColIds);
    } else {
        resetDicOriginData();
        procModalElements.dsID.val('');

        $('#procSettingModal').modal('show');
        loading.hide();
    }
    // $('#processGeneralInfo #processName').prop('disabled', true);
    const dataRowID = $(procItem).parent().parent().data('rowid');
    loadProcModal(procId, dataRowID, dbsId);

    $('#processGeneralInfo select[name="cfgProcTableName"]').select2(
        select2ConfigI18n,
    );

    // clear error message
    $(procModalElements.alertProcessNameErrorMsg).css('display', 'none');

    // hide selection checkbox
    // $(procModalElements.autoSelectAllColumn).hide();

    // reset select all checkbox to uncheck when showing modal
    changeSelectionCheckbox((autoSect = true), (selectAll = true));
    // disable original column name
    $(procModalElements.columnNameInput).each(function f() {
        $(this).attr('disabled', true);
    });

    // clear attr on buttons
    procModalElements.okBtn.removeAttr('data-has-ct');
};

const resetDicOriginData = () => {
    dicOriginDataType = {};
    dicProcessCols = {};
    currentProcess = null;
};

const changeDataSource = (e) => {
    const dsType = $(e).find(':selected').data('ds-type');
    if (dsType === 'CSV' || dsType === 'V2') {
        const tableDOM = $(e)
            .parent()
            .parent()
            .find('select[name="cfgProcTableName"]')[0];
        if (tableDOM) {
            $(tableDOM).hide();
        }
    } else {
        const databaseId = $(e).val();
        $.get(
            `api/setting/database_table/${databaseId}`,
            { _: $.now() },
            (res) => {
                res = jsonParse(res);
                const tables = res.tables.map(
                    (tblName) =>
                        `<option value="${tblName}">${tblName}</option>`,
                );
                const tableOptions = [
                    '<option value="">---</option>',
                    ...tables,
                ].join('');
                const tableDOM = $(e)
                    .parent()
                    .parent()
                    .find('select[name="cfgProcTableName"]')[0];
                if (tableDOM) {
                    $(tableDOM).show();
                    $(tableDOM).html(tableOptions);
                }
            },
        );
    }
};

const addProcToTable = (cfgProcess = null, disabled = false) => {
    // function to create proc_id

    let processId,
        procShownName,
        procName,
        dbsId,
        datatableName,
        datasourceName,
        comment = null;
    if (cfgProcess) {
        processId = cfgProcess.id;
        procShownName = cfgProcess.shown_name;
        procName = cfgProcess.name;
        dbsId = (cfgProcess.data_source ?? {}).id;
        datatableName = (cfgProcess['data_tables'] ?? [])
            .map((e) => e.name)
            .join(' | ');
        datasourceName = (cfgProcess['data_sources'] ?? [])
            .map((e) => e.name)
            .join(' | ');
        comment = cfgProcess.comment;
    }

    removeEmptyConfigRow(procElements.tableProcList);

    const procConfigTextByLang = {
        procName: $('#i18nProcName').text(),
        dbName: $('#i18nDataSourceName').text(),
        tbName: $('#i18nTableName').text(),
        setting: $('#i18nSetting').text(),
        comment: $('#i18nComment').text(),
    };
    const allDS = cfgDS || [];
    // const DSselection = allDS.map(ds => `<option data-ds-type="${ds.type}" ${dbsId && Number(dbsId) === Number(ds.id) ? 'selected' : ''} value="${ds.id}">${ds.name}</option>`);
    // const DSSelectionWithDefaultVal = ['<option value="">---</option>', ...DSselection].join('');
    const dummyRowID = new Date().getTime().toString(36);
    const rowNumber = $(`${procElements.tblProcConfigID} tbody tr`).length;
    const disabledEle = disabled ? ' disabled' : '';

    const newRecord = `
    <tr name="procInfo" ${processId ? `data-proc-id=${processId} id=proc_${processId}` : ''} ${dbsId ? `data-ds-id=${dbsId}` : ''} data-rowid="${dummyRowID}">
        <td class="col-number">${rowNumber + 1}</td>
        <td>
            <input data-name-en="${procName}" name="processName" class="form-control" type="text"
                placeholder="${procConfigTextByLang.procName}"
                value="${procShownName || ''}"
                ${procName ? 'disabled' : ''} 
                ${dragDropRowInTable.DATA_ORDER_ATTR}
                ${disabledEle}>
        </td>
        <td>
            <input name="datasourceName" class="form-control" type="text"
                placeholder="${procConfigTextByLang.dbName}" value="${datasourceName || ''}"
                ${disabledEle}>
        </td>
        <td>
            <input name="datatableName" class="form-control" type="text"
                placeholder="${procConfigTextByLang.tbName}" value="${datatableName || ''}"
                ${disabledEle}>
        </td>
        <td class="text-center button-column">
            <button type="button" class="btn btn-secondary icon-btn"
                onclick="showProcSettingModal(this)">
                <i class="fas fa-edit icon-secondary"></i></button>
        </td>
        <td>
            <textarea name="cfgProcComment" class="form-control form-data"
                rows="1" placeholder="${procConfigTextByLang.comment}"
                value="${comment || ''}"
                disabled></textarea>
        </td>
        <td class="process-status" id="jobStatus-${processId}"></td>
        <td class="text-center button-column">
            <button onclick="deleteProcess(this)" type="button"
                class="btn btn-secondary icon-btn">
                <i class="fas fa-trash-alt icon-secondary"></i>
            </button>
        </td>
    </tr>`;

    $(procElements.tableProcList).append(newRecord);
    if (procName && dbsId) {
        dragDropRowInTable.setItemLocalStorage(
            $(procElements.tableProcList)[0],
        ); // set proc table order
    }
    setTimeout(() => {
        scrollToBottom(`${procElements.tblProcConfig}_wrap`);
    }, 200);

    updateTableRowNumber(procElements.tblProcConfig);
};

$(() => {
    procModalElements.procModal.on('hidden.bs.modal', () => {
        // $(procModalElements.selectAllColumn).css('display', 'none');
    });

    // BRIDGE STATION NO NEED TO ADD A EMPTY PROCESS ANYMORE
    // add an empty process config when there is no process config
    // setTimeout(() => {
    //     const countProcConfig = $(`${procElements.tableProcList} tr[name=procInfo]`).length;
    //     if (!countProcConfig) {
    //         addProcToTable();
    //     }
    // }, 500);

    // drag & drop for tables
    $(`#${procElements.tblProcConfig} tbody`).sortable({
        helper: dragDropRowInTable.fixHelper,
        update: dragDropRowInTable.updateOrder,
    });

    // resort table
    dragDropRowInTable.sortRowInTable(procElements.tblProcConfig);

    // set table order
    $(procElements.divProcConfig)[0].addEventListener(
        'mouseup',
        handleMouseUp,
        false,
    );

    // File name input by explorer
    const $fileName = $(procElements.fileName);
    const $selectFileBtn = $(procElements.fileNameBtn);
    const $selectFileInput = $(procElements.fileNameInput);

    $selectFileBtn.on('click', () => {
        $selectFileInput.click();
    });

    $selectFileInput.on('change', function () {
        const file = this.files[0];
        console.log(file);
        if (file) {
            $fileName.val(file.name);
        }
    });
});

const handleCategoryError = (json) => {
    if (json.errors.length === 0) {
        return;
    }

    const processId = json.process_id;
    const errorColumnsData = [];

    for (const error of json.errors) {
        const { data_id: columnId, error_type: errorType } = error;
        const errorColumns = processes[processId].columns.filter(
            (column) => column.id === columnId,
        );
        if (errorColumns.length !== 1) {
            throw Error(`Invalid category error: ${error}`);
        }

        const errorColumn = errorColumns[0];
        errorColumnsData.push({
            id: columnId,
            shownName: errorColumn.shown_name,
            errorType: errorType,
        });
    }

    const modalEle = $(procModalElements.procConfigCategoryErrorModal);
    const processErrorInfo = modalEle.find('.process-error-info');
    processErrorInfo.html(
        `<p>Process Name: ${processes[processId].shown_name}</p>`,
    );

    const columnsErrorInfo = modalEle.find('.columns-error-info');
    // clear text
    columnsErrorInfo.html('');
    columnsErrorInfo.append('<p>Column:</p>');
    for (const [index, errorData] of errorColumnsData.entries()) {
        columnsErrorInfo.append(`<p>${index + 1}. ${errorData.shownName}</p>`);
    }

    const openProcessConfigButton = modalEle.find('#openProcessConfig');
    openProcessConfigButton.on('click', () => {
        const errorColumnIds = errorColumnsData.map((col) => col.id);
        const buttonShowProcess = $(procElements.tblProcConfigID)
            .find(`tr[data-proc-id=${processId}] td.text-center > button`)
            .first();
        showProcSettingModal(buttonShowProcess, null, errorColumnIds);
        fetchWithLog(
            `/ap/api/setting/abort_transaction_import_job/${processId}`,
            {
                method: 'POST',
            },
        ).then((response) => {
            console.log(response.text());
        });
    });

    modalEle.modal('show');

    const countDownEle = modalEle.find('.count-down');
    // clear text
    countDownEle.text('');

    const countDownText = 'This dialogue is automatically closed in';
    let startTime = 60;
    countDownEle.text(`${countDownText}: ${startTime}`);
    const countDownHandler = setInterval(() => {
        if (startTime > 0) {
            startTime -= 1;
            countDownEle.text(`${countDownText}: ${startTime}`);
        }
    }, 1000);

    setTimeout(
        () => {
            clearInterval(countDownHandler);
            modalEle.modal('hide');
        },
        (startTime + 2) * 1000,
    );
};
