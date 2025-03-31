let currentDataTableItem;
const currentCfgDataTable = {};

const dataTableElements = {
    tblDataTableID: '#tblDataTableConfig',
    tblDataTableList: '#tblDataTableConfig tbody',
    tblDataTableConfig: 'tblDataTableConfig',
    tblDataTableConfigID: '#tblDataTableConfig',
    tableDataTableList: '#tblDataTableConfig tbody',
    dataTableListChild: '#tblDataTableConfig tbody tr',
    divDataTableConfig: '#accordionDataTable',
};

// todo refactor. duplicated
const i18nCfgDataTable = {
    statusDone: $('#i18nStatusDone').text(),
    statusImporting: $('#i18nStatusImporting').text(),
    statusFailed: $('#i18nStatusFailed').text(),
    statusPending: $('#i18nStatusPending').text(),
    validLike: $('#i18nValidLike').text(),
    reachFailLimit: $('#i18nReachFailLimit').text(),
    V2MeasurementData: $('#i18nV2MeasurementData').text(),
    V2HistoryData: $('#i18nV2HistoryData').text(),
};

const isAddNewDataTableMode = () =>
    isEmpty(dataTableSettingModalElements.dataTableID.val() || null);

const showPartitions = (partitions, from = '', to = '') => {
    if (partitions) {
        // add option empty partitions
        partitions = ['', ...partitions];
        dataTableSettingModalElements.partitionFromDiv.show();
        dataTableSettingModalElements.partitionToDiv.show();

        const fromOptionsHtml = partitions.map((tbl) => {
            const isSelected = tbl === from ? 'selected' : '';
            return `<option value="${tbl}" ${isSelected}>${tbl}</option>`;
        });

        const toOptionsHtml = partitions.map((tbl) => {
            const isSelected = tbl === to ? 'selected' : '';
            return `<option value="${tbl}" ${isSelected}>${tbl}</option>`;
        });

        dataTableSettingModalElements.partitionFrom.html(
            fromOptionsHtml.join(''),
        );
        dataTableSettingModalElements.partitionTo.html(toOptionsHtml.join(''));
        dataTableSettingModalElements.isLogicalTable.val('true');
    } else {
        dataTableSettingModalElements.partitionFromDiv.hide();
        dataTableSettingModalElements.partitionToDiv.hide();
        dataTableSettingModalElements.isLogicalTable.val('');
    }

    // Set max-height for scroll table
    document.documentElement.style.setProperty(
        '--cad-from-to-total-height',
        partitions ? '90px' : '0px',
    );
};

const showColumnAttributeModal = (serialCols, datetimeCols, orderCols) => {
    const serialHtml = serialCols.map((serialCol) => {
        return `<option value="${serialCol}" >${serialCol}</option>`;
    });

    const datetimeHtml = datetimeCols.map((datetimeCol) => {
        return `<option value="${datetimeCol}" >${datetimeCol}</option>`;
    });

    const orderHtml = orderCols.map((orderCol) => {
        return `<option value="${orderCol}" >${orderCol}</option>`;
    });

    $('#orderAttribute').hover(
        function () {
            $(this).attr('title', i18nCommon.orderAttributeHover);
        },
        function () {},
    );

    dataTableSettingModalElements.serialColAttr.html(serialHtml.join(''));
    dataTableSettingModalElements.datetimeColAttr.html(datetimeHtml.join(''));
    dataTableSettingModalElements.orderColAttr.html(orderHtml.join(''));
};

const getDataTableColumnConfigHTML = (col, isAddNew = true) => {
    const isNumeric = isNumericDatatype(col.data_type);
    const [numericOperators, textOperators, coefHTML] =
        createOptCoefHTMLCfgDataTable(col.operator, col.coef, isNumeric);

    return `<tr name="selectedColumn" id="selectedColumn${col.column_name}" uid="${col.column_name}">
        <td class="col-number"></td>
        <td class="pr-row">
            <input data-type="${col.data_type}" data-m-data-group-id="${col.data_group_type}" name="${dataTableSettingModalElements.columnName}" 
                class="form-control" value="${col.column_name}" disabled>
        </td>
        <td class="pr-row"><input name="${dataTableSettingModalElements.shownName}" class="form-control" type="text" value="${col.name}"></td>
        <td class="pr-row">
            <select name="${dataTableSettingModalElements.operator}" class="form-control" type="text">
                <option value="">---</option>
                ${isNumeric ? numericOperators : textOperators}
            </select>
        </td>
        <td class="pr-row-sm pr-row">
            ${coefHTML}
        </td>
    </tr>`;
};

const getDataTableInfo = (cfgDataTableId) => {
    ajaxWithLog({
        url: `api/setting/cfg_data_table/${cfgDataTableId}`,
        type: 'GET',
        cache: false,
        success(res) {
            res = jsonParse(res);
            // loading.hide();

            dataTableSettingModalElements.databases.html('');
            const options = {
                type: res.data.data_source.type,
                master_type: res.data.data_source.master_type,
                value: res.data.data_source.id,
                text: res.data.data_source.name,
                disabled: true,
                selected: 'selected',
            };
            dataTableSettingModalElements.databases.append(
                $('<option/>', options),
            );
            dataTableSettingModalElements.tables.empty();
            dataTableSettingModalElements.tables.prop('disabled', false);
            dataTableSettingModalElements.dataTableName.val(res.data.name);
            dataTableSettingModalElements.dataTableID.val(res.data.id);
            dataTableSettingModalElements.comment.val(res.data.comment);
            dataTableSettingModalElements.tables.val(res.data.table_name);
            if (
                res.data.data_source.master_type ===
                DEFAULT_CONFIGS.V2.master_type
            ) {
                dataTableSettingModalElements.detailMasterTypeDiv.show();
                dataTableSettingModalElements.detailMasterType.val(
                    res.data.detail_master_type,
                );
                dataTableSettingModalElements.detailMasterType
                    .children()
                    .remove();
                const textOption =
                    res.data.detail_master_type ===
                    DEFAULT_CONFIGS.V2.master_type
                        ? i18nCfgDataTable.V2MeasurementData
                        : i18nCfgDataTable.V2HistoryData;
                dataTableSettingModalElements.detailMasterType.append(
                    $('<option/>', {
                        value: res.data.detail_master_type,
                        text: textOption,
                        selected: true,
                    }),
                );
                // dataTableSettingModalElements.detailMasterTypes.prop('disabled', true);
            } else {
                dataTableSettingModalElements.detailMasterTypeDiv.hide();
            }
            dataTableSettingModalElements.dsID.val(res.data.data_source_id);
            currentCfgDataTable.ds_id = res.data.data_source_id;
            currentCfgDataTable.table_name = res.data.table_name;
            showPartitions(
                res.data.partitions,
                res.data.partition_from,
                res.data.partition_to,
            );

            const dsLength = $(
                '#dataTableSettingModal select[name=databaseName] option',
            ).length;
            if (dsLength > 0) {
                $(`#dataTableSettingModal select[name=databaseName]`)
                    .val(res.data.data_source_id)
                    .trigger('change');
            }

            res.data.columns.forEach((row) => {
                dataTableSettingModalElements.seletedColumnsBody.append(
                    getDataTableColumnConfigHTML(row, false),
                );
                updateSelectedRows(true);
            });

            dataTableSettingModalElements.tables.append(
                $('<option/>', {
                    value: res.data.table_name || '',
                    text: res.data.table_name || '---',
                }),
            );
            dataTableSettingModalElements.tables.prop('disabled', true);

            // showPartitions(res.tables.partitions, res.data.partition_from, res.data.partition_to);

            // validate coef when showing selected columns
            // validateAllCoefs(); BS not use

            // handling english name onchange
            // handleEnglishNameChange(); BS not use

            // disable datetime + as key columns
            validateFixedColumnsDataTable();

            // show warning to reset data link config when change as link id
            validateCheckBoxesAllDataTable();

            // update row number
            updateTableRowNumber(null, $('table[name=selectedColumnsTable]'));

            // $(dataTableSettingModalElements.dataTableSettingModal).modal('show');
        },
    });
};

const showDataTableSettingModal = async (
    procItem,
    isBackToCSVTSVDataSourceConfigModal = false,
) => {
    checkCountClick();
    clearWarning();
    $(dataTableSettingModalElements.dataSM).html('');
    $(dataTableSettingModalElements.cfgDataTableContent).removeClass('hide');
    $(dataTableSettingModalElements.dataSM).parent().addClass('hide');

    currentDataTableItem = $(procItem).closest('tr');
    let cfgDataTableId = currentDataTableItem.data('datatable-id');
    const cfgDataSourceId =
        currentDataTableItem.data('ds-id') ||
        parseInt(
            currentDataTableItem.find('select[name=cfgDataSourceName]').val(),
        );
    const loading = $('.loading');
    loading.show();

    // TODO: clear old input from form
    $(dataTableSettingModalElements.latestDataHeader).empty();
    $(dataTableSettingModalElements.latestDataBody).empty();
    $(dataTableSettingModalElements.seletedColumnsBody).empty();
    dataTableSettingModalElements.comment.val('');
    dataTableSettingModalElements.dataTableName.val('');
    dataTableSettingModalElements.dataTableID.val('');
    dataTableSettingModalElements.comment.val('');
    dataTableSettingModalElements.databases.html('');
    dataTableSettingModalElements.tables.html('');
    dataTableSettingModalElements.tables.prop('disabled', false);
    dataTableSettingModalElements.cancelBtn.isBackToCSVTSVDataSourceConfigModal =
        isBackToCSVTSVDataSourceConfigModal;
    let isAutoPreview = false;
    let isReadOnlyMode = false;
    let isCsv = false;
    if (cfgDataTableId) {
        // set current proc
        dataTableModalCurrentProcId = cfgDataTableId;
        // userEditedDSNameCfgDataTable = true;
        getDataTableInfo(cfgDataTableId);
        isAutoPreview = true;
    } else {
        dataTableModalCurrentProcId = null;
        [isCsv, isReadOnlyMode] = await loadDataTableModal(cfgDataSourceId);
        if (isCsv == null) {
            // In case no data source in db, not show modal
            showToastrMsg('There are no data source in DB, please check!!!');
            console.warn('There are no data source in DB, please check!!!');
        }
    }

    $('#dataTableGeneralInfo select[name="tablename"]').select2(
        select2ConfigI18n,
    );

    // clear error message
    $(dataTableSettingModalElements.alertDataTableNameErrorMsg).css(
        'display',
        'none',
    );

    // hide selection checkbox
    $(dataTableSettingModalElements.autoSelectAllColumn).hide();

    // reset select all checkbox to uncheck when showing modal
    changeSelectionCheckbox((autoSect = false), (selectAll = false));
    // disable original column name
    $(dataTableSettingModalElements.columnNameInput).each(function f() {
        $(this).attr('disabled', true);
    });

    // show setting mode when loading proc config
    showHideModesDataTable(false, isReadOnlyMode);

    dataTableSettingModalElements.dataTableSettingModal.modal('show');
    loading.hide();

    // check table_name = null
    setTimeout(() => {
        // if (!cfgDataTableId){
        //     setDataTableName();
        //     return;
        // }
        if (isAutoPreview || !isCsv) {
            // only show data when table is selected
            let selectedTable = dataTableSettingModalElements.tables
                .find('option:selected')
                .text();
            if (selectedTable) {
                dataTableSettingModalElements.showRecordsBtn.trigger('click');
            }
        }
    }, 500);
};

const addDataTableToTable = (dataSourceId = null) => {
    // function to create proc_id
    removeEmptyConfigRow(dataTableElements.tableDataTableList);

    const dataTableConfigTextByLang = {
        // todo change text
        dataTableName: $('#i18nProcName').text(),
        dbName: $('#i18nDBName').text(),
        tbName: $('#i18nTableName').text(),
        setting: $('#i18nSetting').text(),
        comment: $('#i18nComment').text(),
    };
    const allDS = cfgDS || [];
    const DSselection = allDS.map(
        (ds, i) =>
            `<option data-ds-type="${ds.type}" value="${ds.id}" >${ds.name}</option>`,
    );
    const DSSelectionWithDefaultVal = [
        '<option value="">---</option>',
        ...DSselection,
    ].join('');
    const dummyRowID = new Date().getTime().toString(36);

    const newRecord = `
    <tr name="tableInfo" data-rowid="${dummyRowID}">
        <td class="col-number"></td>
        <td>
            <input name="dataTableName" class="form-control" type="text"
                placeholder="${dataTableConfigTextByLang.tbName}" value="" ${dragDropRowInTable.DATA_ORDER_ATTR} disabled>
        </td>
        <td class="text-center">
            <select class="form-control" name="cfgDataSourceName" 
                onchange="changeDataSource(this);">${DSSelectionWithDefaultVal}</select>
        </td>
        <td class="text-center text-center th-title-other button-column">
            <button type="button" class="btn btn-secondary icon-btn"
                onclick="showDataTableSettingModal(this)">
                <i class="fas fa-edit icon-secondary"></i></button>
        </td>
        <td>
            <textarea name="comment" class="form-control form-data" rows="1" 
                placeholder="${dataTableConfigTextByLang.comment}"
                value=""
                disabled></textarea>
        </td>
        <td class="data-table-status" id=""></td>
        <!-- BRIDGE STATION - Refactor DN & OSS version -->
        ${
            isAppSourceDN
                ? `
        <td class="text-center">
            <button
                    onclick="goToMappingPage(this);"
                    type="button"
                    class="btn btn-secondary icon-btn data-table-to-mapping-page blink-btn-fast hide"
            >
                <i class="fas fa-sitemap icon-secondary"></i>
            </button>
        </td>
        `
                : ''
        }
        <td class="col-btn button-column">
            <button onclick="deleteCfgDataTable(this)" type="button"
                class="btn btn-secondary icon-btn">
                <i class="fas fa-trash-alt icon-secondary"></i>
            </button>
        </td>
    </tr>`;

    $(dataTableElements.tableDataTableList).append(newRecord);
    updateTableRowNumber(dataTableElements.tblDataTableConfig);
    setTimeout(() => {
        scrollToBottom(`${dataTableElements.tblDataTableConfig}_wrap`);
    }, 200);
    if (dataSourceId) {
        const $targetDataTableConfigRow = $(
            '#tblDataTableConfig select[name=cfgDataSourceName]',
        ).last();
        $targetDataTableConfigRow.val(dataSourceId).change();
        const button = $targetDataTableConfigRow
            .closest('tr')
            .find('td.text-center > button');
        showDataTableSettingModal(button).then();
    }
};

/**
 * @param {HTMLButtonElement} btn
 */
const goToMappingPage = (btn) => {
    const btnElement = $(btn).closest('tr[name=tableInfo]');
    const dataTableId = btnElement.attr('data-datatable-id');
    if (dataTableId) {
        const url = `/ap/mapping_config/master_line?data_table_id=${dataTableId}`;
        window.open(url, '_blank');
    }
};

const deleteCfgDataTable = (procItem) => {
    currentDataTableItem = $(procItem).closest('tr');
    const cfgDataTableId = currentDataTableItem.data('datatable-id');
    if (cfgDataTableId) {
        $('#btnDeleteDataTable').attr('datatable-id', cfgDataTableId);
        $('#deleteDataTableModal').modal('show');
    } else {
        // remove empty row
        $(currentDataTableItem).remove();
        updateTableRowNumber(dataTableElements.tblDataTableConfig);
    }
};

const confirmDelDataTable = () => {
    const dataTableId = $('#btnDeleteDataTable').attr('datatable-id');
    fetchWithLog('api/setting/delete_data_table', {
        method: 'POST',
        headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ data_table_id: dataTableId }), // example: { data_table_id: 20700000000000000001 }
    })
        .then((response) => response.clone().json())
        .then(() => {
            // remove proc from HTML table
            removeDataTableConfigRow(dataTableId);

            // update row number
            updateTableRowNumber(dataTableElements.tblDataTableConfig);
        })
        .catch(() => {});
};

const removeDataTableConfigRow = (dataTableId) => {
    $(`#data_table_${dataTableId}`).remove();
};

const addColumnAttrToTable = (cfgDataTable, disabled = false) => {
    removeEmptyConfigRow(dataTableElements.tblDataTableList);
    const procConfigTextByLang = {
        dbName: $('#i18nDataSourceName').text(),
        tbName: $('#i18nTableName').text(),
        setting: $('#i18nSetting').text(),
        comment: $('#i18nComment').text(),
    };
    const rowNumber = $(`${dataTableElements.tblDataTableID} tbody tr`).length;
    const disabledEle = disabled ? ' disabled' : '';

    const newRecord = `
    <tr name="tableInfo" id="data_table_${cfgDataTable.id}" data-ds-id="${cfgDataTable.data_source.id}" data-datatable-id="${cfgDataTable.id}">
        <td class="col-number">${rowNumber + 1}</td>
        <td>
            <input name="dataTableName" class="form-control" type="text"
                placeholder="${procConfigTextByLang.tbName}" value="${cfgDataTable.name}"
                ${disabledEle} data-order>
        </td>
        <td class="text-center">
            <input name="databaseName" class="form-control" type="text"
                placeholder="${procConfigTextByLang.dbName}" value="${cfgDataTable.data_source.name}"
                ${disabledEle}>
        </td>
        <td class="text-center th-title-other button-column">
            <button type="button" class="btn btn-secondary icon-btn"
                onclick="showDataTableSettingModal(this);">
                <i class="fas fa-edit icon-secondary"></i></button>
        </td>
        <td>
            <textarea name="comment" class="form-control form-data" rows="1"
                value="${cfgDataTable.comment ?? ''}"
                disabled>${cfgDataTable.comment ?? ''}</textarea>
        </td>
        <td class="data-table-status" id="jobStatus-${cfgDataTable.id}"></td>
        <!-- BRIDGE STATION - Refactor DN & OSS version -->
        ${
            isAppSourceDN
                ? `
        <td class="text-center">
            <button
                    onclick="goToMappingPage(this);"
                    type="button"
                    class="btn btn-secondary icon-btn data-table-to-mapping-page blink-btn-fast hide"
            >
                <i class="fas fa-sitemap icon-secondary"></i>
            </button>
        </td>
        `
                : ''
        }
        <td class="col-btn button-column">
            <button onclick="deleteCfgDataTable(this);" type="button"
                class="btn btn-secondary icon-btn">
                <i class="fas fa-trash-alt icon-secondary"></i>
            </button>
        </td>
    </tr>`;

    $(dataTableElements.tblDataTableList).append(newRecord);
    setTimeout(() => {
        scrollToBottom(`${dataTableElements.tblDataTableConfig}_wrap`);
    }, 200);
};

$(() => {
    dataTableSettingModalElements.dataTableSettingModal.on(
        'hidden.bs.modal',
        () => {
            $(dataTableSettingModalElements.latestDataTableTopAction).css(
                'display',
                'none',
            );
        },
    );

    // add an empty process config when there is no process config
    setTimeout(() => {
        const countDataTableConfig = $(
            `${dataTableElements.tableDataTableList} tr[name=tableInfo]`,
        ).length;
        if (!countDataTableConfig) {
            addDataTableToTable();
        }
    }, 500);

    // drag & drop for tables
    $(`#${dataTableElements.tblDataTableConfig} tbody`).sortable({
        helper: dragDropRowInTable.fixHelper,
        update: dragDropRowInTable.updateOrder,
    });

    // resort table
    dragDropRowInTable.sortRowInTable(dataTableElements.tblDataTableConfig);

    // set table order
    $(dataTableElements.divDataTableConfig)[0].addEventListener(
        'contextmenu',
        baseRightClickHandler,
        false,
    );
    $(dataTableElements.divDataTableConfig)[0].addEventListener(
        'mouseup',
        handleMouseUp,
        false,
    );
});

/* We depend on SCAN_MASTER and SCAN_DATA_TYPE to update status for data table.
 * if job type is not those jobs, we will skip updating
 * status is done if and only if job_type is SCAN_DATA_TYPE and status is DONE
 * otherwise, it should never be done
 *
 * To know if this data table is scanned or not, we use the attribute data-scan-done
 * we don't check those data tables which have already finished scanning
 */
const updateStatusForDataTable = (row) => {
    const validJobs = ['SCAN_MASTER', 'SCAN_DATA_TYPE'];
    if (!validJobs.includes(row.job_type)) {
        return;
    }

    const dataTable = $(`#data_table_${row.data_table_id}`);
    const scanned = dataTable.attr('data-scan-done');
    if (typeof scanned !== 'undefined' && scanned !== false) {
        return;
    }

    let rowStatus = row.status;
    // do not allow job done if we are not scanning data type
    if (row.job_type !== 'SCAN_DATA_TYPE' && rowStatus === 'DONE') {
        rowStatus = 'PROCESSING';
    }

    const jobStatusEle = dataTable.find('.data-table-status').first();
    const status = jobStatusEle.attr('data-status');

    // do not update status if it's done already
    if (status !== 'DONE') {
        const statusClass =
            JOB_STATUS[rowStatus].class || JOB_STATUS.FAILED.class;
        let statusTooltip =
            JOB_STATUS[rowStatus].title || JOB_STATUS.FAILED.title;
        const updatedStatus = `<div class="align-middle text-center" data-st="${statusClass}">
                <div class="" data-toggle="tooltip" data-placement="top" title="${statusTooltip}">
                    <i class="fas fa-${statusClass} status-i"></i>
                </div>
            </div>`;

        if (
            jobStatusEle &&
            jobStatusEle.html() &&
            jobStatusEle.html().trim() !== ''
        ) {
            if (status !== rowStatus) {
                jobStatusEle.html(updatedStatus);
            }
        } else {
            jobStatusEle.html(updatedStatus);
        }
        jobStatusEle.attr('data-status', rowStatus);
    }
};

/* Update status for mapping config page. Status is updated if and only if:
 *  - SCAN_DATA_TYPE or SCAN_UNKNOWN_DATA_TYPE jobs are sent
 *  - job has done
 *  - mapping config page is enabled
 */
const shouldUpdateMappingPageStatus = (row) => {
    const validJobs = ['SCAN_DATA_TYPE', 'SCAN_UNKNOWN_DATA_TYPE'];
    if (!validJobs.includes(row.job_type)) {
        return false;
    }

    if (row.status !== 'DONE') {
        return false;
    }

    const dataTable = $(`#data_table_${row.data_table_id}`);
    const mappingPageEnabled = dataTable.attr('data-mapping-page-enabled');
    if (
        typeof mappingPageEnabled === 'undefined' ||
        mappingPageEnabled === false
    ) {
        return false;
    }

    return true;
};

/**
 * @param {number | string} option.data_table_id
 * @param {boolean} option.enabled
 */
const updateDataTableMappingConfigStatus = (option) => {
    const dataTable = $(`#data_table_${option.data_table_id}`);
    const toMappingPageButton = dataTable.find(
        'button.data-table-to-mapping-page',
    );
    if (option.enabled) {
        toMappingPageButton.removeClass('hide');
    } else {
        toMappingPageButton.addClass('hide');
    }
};

const updateMappingConfigStatus = (json) => {
    if (_.isEmpty(json)) {
        return;
    }
    Object.values(json).forEach((option) => {
        const { has_new_master: hasNewMaster, data_table_id: dataTableId } =
            option;
        updateDataTableMappingConfigStatus({
            data_table_id: dataTableId,
            enabled: hasNewMaster,
        });
    });
};

const notifyUserUnknownMasterDataFound = (row) => {
    showNewFoundMasterToastr(row.data_table_name, row.data_table_id);
};

const updateBackgroundJobsForDataTable = (json) => {
    if (_.isEmpty(json)) {
        return;
    }

    Object.values(json)
        .filter((row) => row.data_table_id)
        .forEach((row) => {
            updateStatusForDataTable(row);

            if (shouldUpdateMappingPageStatus(row)) {
                fetchWithLog(
                    `/ap/mapping_config/has_new_master?data_table_id=${row.data_table_id}`,
                )
                    .then((response) => response.json())
                    .then((data) => {
                        const { has_new_master: hasNewMaster } = data;
                        updateDataTableMappingConfigStatus({
                            data_table_id: row.data_table_id,
                            enabled: hasNewMaster,
                        });
                        if (hasNewMaster) {
                            notifyUserUnknownMasterDataFound(row);
                        }
                    });
            }
        });
};
