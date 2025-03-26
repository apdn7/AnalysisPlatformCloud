let dataTableModalCurrentProcId = null;
let currentAsLinkIdBoxDataTable = null;
let userEditedDSNameCfgDataTable = false;
const EFA_TABLES = ['XR_PLOT', 'PART_LOG'];

const formElements = {
    endProcSelectedItem: '#yoyakuSelects-parent select', // tunghh  ?
};

// todo (procModalElements_cfgDataTable) check lại mấy cái item trong. cái nào k dùng thì xoá
const dataTableSettingModalElements = {
    dataTableSettingModal: $('#dataTableSettingModal'),
    dataTableSettingModalBody: $('#dataTableSettingModalBody'),
    dataTableSettingContent: $('#dataTableSettingContent'),
    dataTableName: $('#dataTableSettingModal input[name=dataTableName]'),
    comment: $('#dataTableSettingModal input[name=comment]'),
    databases: $('#dataTableSettingModal select[name=databaseName]'),
    tables: $('#dataTableSettingModal select[name=tableName]'),
    showRecordsBtn: $('#dataTableSettingModal button[name=showRecords]'),
    latestDataHeader: $(
        '#dataTableSettingModal table[name=latestDataTable] thead',
    ),
    latestDataBody: $(
        '#dataTableSettingModal table[name=latestDataTable] tbody',
    ),
    seletedColumnsBody: $(
        '#dataTableSettingModal table[name=selectedColumnsTable] tbody',
    ),
    seletedColumns: $(
        '#dataTableSettingModal table[name=selectedColumnsTable] tbody tr[name=selectedColumn]',
    ),
    // okBtn: $('#dataTableSettingModal button[name=scanBtn]'),
    scanBtn: $('#dataTableSettingModal button[name=scanBtn]'),
    cancelBtn: $(
        '#dataTableSettingModal button[id=cancelColumnAttributeModalBtn]',
    ),
    reRegisterBtn: $('#procSettingModal button[name=reRegisterBtn]'),
    confirmImportDataBtn: $('#confirmImportDataBtn'),
    confirmScanMasterDataBtn: $('#confirmScanMasterDataBtn'),
    pullDataBtn: $('#pullDataBtn'),
    confirmPullDataModal: $('#confirmPullDataModal'),
    confirmReRegisterProcBtn: $('#confirmReRegisterProcBtn'),
    revertChangeAsLinkIdBtn: $('#revertChangeAsLinkIdBtnDataTable'),
    confirmImportDataModal: $('#confirmImportDataModal'),
    confirmPullDataBtn: $('#confirmPullDataBtn'),
    confirmScanMasterDataModal: $('#confirmScanMasterDataModal'),
    confirmReRegisterProcModal: $('#confirmReRegisterProcModal'),
    warningResetDataLinkModal: $('#warningResetDataLinkModal'),
    warningResetDataLinkModalDataTable: $(
        '#warningResetDataLinkModalDataTable',
    ),
    procConfigConfirmSwitchModal: $('#procConfigConfirmSwitchModal'),
    dateTime: 'dateTime',
    serial: 'serial',
    order: 'order',
    dataSerial: 'dataSerial',
    auto_increment: 'auto_increment',
    columnName: 'columnName',
    englishName: 'englishName',
    shownName: 'shownName',
    operator: 'operator',
    coef: 'coef',
    procsComment: 'comment',
    procsdbName: 'databaseName',
    procsTableName: 'tableName',
    yoyakuselectName: 'yoyakuSelects',
    coefInput: $(
        '#procSettingModal table[name=selectedColumnsTable] input[name="coef"]',
    ),
    columnNameInput:
        '#dataTableSettingModal table[name=selectedColumnsTable] input[name="columnName"]',
    englishNameInput:
        '#dataTableSettingModal table[name=selectedColumnsTable] input[name="englishName"]',
    masterNameInput:
        '#dataTableSettingModal table[name=selectedColumnsTable] input[name="shownName"]',
    latestDataTable: $('#dataTableSettingModal table[name=latestDataTable]'),
    msgProcNameAlreadyExist: '#i18nAlreadyExist',
    msgDataTableNameBlank: '#i18nDataTableNameBlank',
    msgColumnAttributeOnlyHorizontalDataOrDataValue:
        '#i18nColumnAttributeOnlyHorizontalDataOrDataValue',
    msgHorizontalData: '#i18nHorizontalData',
    msg0CannotBeSelectedForHorizontalDataSource:
        '#i18n0CannotBeSelectedForHorizontalDataSource',
    msgDataValue: '#i18nDataValue',
    msgFactoryID: '#i18nFactoryID',
    msgFactoryName: '#i18nFactoryName',
    msgPlantID: '#i18nPlantID',
    msgPlantNO: '#i18nPlantNO',
    msgDeptID: '#i18nDeptID',
    msgDeptName: '#i18nDeptName',
    msgLineGroupID: '#i18nLineGroupID',
    msgLineGroupName: '#i18nLineGroupName',
    msgLineID: '#i18nLineID',
    msgLineName: '#i18nLineName',
    msgProcessID: '#i18nProcessID',
    msgProcessName: '#i18nProcessName',
    msgMachineID: '#i18nMachineID',
    msgMachineName: '#i18nMachineName',
    msgPartNoFull: '#i18nPartNoFull',
    msgPartNo: '#i18nPartNo',
    msgDatetime: '#i18nDatetime',
    msgOrder: '#i18nOrder',
    msgSerial: '#i18nSerial',
    msgSubPartNo: '#i18nSubPartNo',
    msgSubTrayNo: '#i18nSubTrayNo',
    msgSubLotNo: '#i18nSubLotNo',
    msgSubSerial: '#i18nSubSerial',
    msgQualityName: '#i18nQualityName',
    msgQualityID: '#i18nQualityID',
    msgModal: '#msgModal',
    msgContent: '#msgContent',
    msgConfirmBtn: '#msgConfirmBtn',
    // selectAllSensor: '#selectAllSensor',
    selectAllSensor: '#selectAllSensorChkBox',
    alertMessage: '#alertMsgSelectedColumnsTable',
    alertDataTableNameErrorMsg: '#alertDataTableNameErrorMsg',
    selectAllColumn: '#selectAllDataTableColumn',
    procID: $('#dataTableSettingModal input[name=dataTableId]'),
    dataTableID: $('#dataTableSettingModal input[name=dataTableId]'),
    dsID: $('#dataTableSettingModal input[name=dataTableDsID]'),
    isLogicalTable: $('#dataTableSettingModal input[name=isLogicalTable]'),
    formId: '#cfgDataTableForm',
    changeModeBtn: '#dataTableEditMode',
    prSMChangeModeBtn: '#prcEditMode',
    confirmSwitchButton: '#confirmSwitch',
    settingContent: '#dataTableSettingContent',
    cfgDataTableContent: '#dataTableSettingContent',
    selectedColumnsTable: 'selectedColumnsTable',
    prcSM: '#prcSM',
    dataSM: '#dataSM',
    autoSelectAllColumn: '#autoSelectDataTableAllColumn',
    autoSelect: '#autoSelectChkBox',
    checkColsContextMenu: '#checkColsContextMenu',
    latestDataTableTopAction: $('#latestDatatTableTopAction'),
    dataTableLoading: $('#dataTableLoading'),
    yoyakugoSelect: $('[name=yoyakuSelects]'),
    partitionFromDiv: $('[name=partitionFromDiv]'),
    partitionToDiv: $('[name=partitionToDiv]'),
    partitionFrom: $('[name=partitionFrom]'),
    partitionTo: $('[name=partitionTo]'),
    detailMasterTypeDiv: $('[name=detailMasterTypeDiv]'),
    detailMasterType: $('[name=detailMasterType]'),
    detailMasterTypes: $(
        '#dataTableSettingModal select[name=detailMasterType]',
    ),
    confirmDirectColumnAttribute: '#confirmDirectColumnAttribute',
    serialColAttr: $('[name=serialAttrDefCols]'),
    datetimeColAttr: $('[name=datetimeAttrDefCols]'),
    orderColAttr: $('[name=orderAttrDefCols]'),
    procCfgTbl: '#procCfgTbl',
};

const setDataTableName = (dataRowID = null) => {
    const procDOM = $(`#tblDataTableConfig tr[data-rowid=${dataRowID}]`);

    const dataTableNameInput =
        dataTableSettingModalElements.dataTableName.val();

    // if (userEditedDSNameCfgDataTable && dataTableNameInput) {
    //     return;
    // }

    const dsNameSelection = $(
        '#dataTableSettingModal select[name=databaseName] option:selected',
    ).text();
    const sourceType = $(
        '#dataTableSettingModal select[name=databaseName] option:selected',
    ).attr('type');
    const masterType = $(
        '#dataTableSettingModal select[name=databaseName] option:selected',
    ).attr('master_type');
    let tableNameSelection = $(
        '#dataTableSettingModal select[name=tableName] option:selected',
    ).text();
    let selectDetailMasterType =
        dataTableSettingModalElements.detailMasterType.val() || '';
    if (tableNameSelection === '---') {
        tableNameSelection = null;
    }

    // get setting information from outside card
    const settingDataTableName = procDOM.find('input[name="dataTableName"]')[0];
    // const settingDSName = procDOM.find('select[name="databaseName"]')[0];
    const settingTableName = procDOM.find('select[name="tableName"]')[0];

    // add new proc row
    let firstGenerated = false;
    if (dataRowID && settingDataTableName && !dataTableNameInput) {
        dataTableSettingModalElements.dataTableName.val(
            $(settingDataTableName).val(),
        );
        firstGenerated = true;
    }

    if (
        (!firstGenerated && !userEditedDSNameCfgDataTable) ||
        !dataTableSettingModalElements.dataTableName.val()
    ) {
        let firstTableName = '';
        if (tableNameSelection) {
            firstTableName = tableNameSelection;
        } else if (settingTableName) {
            firstTableName = $(settingTableName).val();
        }
        const combineProcName = firstTableName
            ? `${dsNameSelection}_${firstTableName}`
            : selectDetailMasterType
              ? `${selectDetailMasterType}_${dsNameSelection}`
              : dsNameSelection;
        dataTableSettingModalElements.dataTableName.val(combineProcName);
        if (
            firstTableName.length > 0 ||
            selectDetailMasterType.length > 0 ||
            (masterType === DEFAULT_CONFIGS.CSV.master_type &&
                sourceType === DEFAULT_CONFIGS.CSV.id)
        ) {
            dataTableSettingModalElements.showRecordsBtn.trigger('click');
        }
    }
};

// reload tables after change
const loadTablesCfgDataTable = (
    databaseId,
    dataRowID = null,
    selectedTbl = null,
) => {
    // if new row have process name, set new process name in modal
    if (!databaseId) {
        setDataTableName(dataRowID);
        return;
    }
    if (isEmpty(databaseId)) return;

    ajaxWithLog({
        url: `api/setting/database_table/${databaseId}`,
        method: 'GET',
        cache: false,
    }).done((res) => {
        res = jsonParse(res);
        dataTableSettingModalElements.tables.empty();
        dataTableSettingModalElements.tables.prop('disabled', false);
        // TODO: use const for data type
        if (res.ds_type === DEFAULT_CONFIGS.CSV.id) {
            dataTableSettingModalElements.tables.append(
                $('<option/>', {
                    value: '',
                    text: '---',
                }),
            );
            dataTableSettingModalElements.tables.prop('disabled', true);
            if (res.master_type === DEFAULT_CONFIGS.V2.master_type) {
                dataTableSettingModalElements.detailMasterTypeDiv.show();
                const masterTypes = ['', ...res.detail_master_types];
                const masterTypesHtml = masterTypes.map((masterType) => {
                    const textOption =
                        masterType === DEFAULT_CONFIGS.V2.master_type
                            ? i18nCfgDataTable.V2MeasurementData
                            : i18nCfgDataTable.V2HistoryData;
                    return `<option value="${masterType}">${textOption}</option>`;
                });
                dataTableSettingModalElements.detailMasterType.html(
                    masterTypesHtml.join(''),
                );
            } else {
                dataTableSettingModalElements.detailMasterTypeDiv.hide();
            }
        } else if (res.tables) {
            let tables = res.tables;
            tables.unshift('');
            tables.forEach((tbl) => {
                const options = {
                    value: tbl,
                    text: tbl,
                };
                if (selectedTbl && tbl === selectedTbl) {
                    options.selected = 'selected';
                }
                dataTableSettingModalElements.tables.append(
                    $('<option/>', options),
                );
            });
            dataTableSettingModalElements.detailMasterTypeDiv.hide();
        }
        // if (!dataTableModalCurrentProcId) {
        //     setDataTableName(dataRowID);
        // }
        setDataTableName(dataRowID);
        showPartitions(res.partitions);
    });
};

// load current proc name, database name and tables name

const loadDataTableModal = async (cfgDataSourceId = null) => {
    let isCsv = false;
    let isReadOnlyMode = true;
    // load databases
    const dbInfo = await getAllDatabaseConfig();
    if (dbInfo != null && Array.isArray(dbInfo) && dbInfo.length > 0) {
        let dicSelectedDataSource;
        dataTableSettingModalElements.databases.html('');
        const selectedDataSourceId = cfgDataSourceId
            ? cfgDataSourceId
            : dbInfo[dbInfo.length - 1].id;
        dbInfo.forEach((ds) => {
            const options = {
                type: ds.type,
                master_type: ds.master_type,
                value: ds.id,
                text: ds.name,
                disabled: false,
            };
            if (ds.id === selectedDataSourceId) {
                options.selected = 'selected';
                dicSelectedDataSource = options;
            }
            dataTableSettingModalElements.databases.append(
                $('<option/>', options),
            );
        });

        // load default tables
        let dbNotDirectInfo = [];
        for (let i = 0; i < dbInfo.length; i++) {
            dbNotDirectInfo.push(dbInfo[i]);
        }
        if (dbNotDirectInfo.length > 0) {
            if (!cfgDataSourceId) {
                await loadTablesCfgDataTable(
                    dbNotDirectInfo[dbNotDirectInfo.length - 1].id,
                );
            } else {
                await loadTablesCfgDataTable(cfgDataSourceId);
            }

            count++; // fix bug that cannot update data on UI when switch database in first time
        } else {
            dataTableSettingModalElements.tables.append(
                $('<option/>', {
                    value: '',
                    text: '---',
                }),
            );
            dataTableSettingModalElements.tables.prop('disabled', true);
        }

        const dsType = dicSelectedDataSource.type.toLocaleLowerCase();
        if (
            [DB.DEFAULT_CONFIGS.V2.type, DB.DEFAULT_CONFIGS.CSV.type].includes(
                dsType,
            )
        ) {
            isCsv = true;
        }

        isReadOnlyMode = false;
    }

    return [isCsv, isReadOnlyMode];
};

const getColDataTypeCfgDataTable = (colName) => {
    const colInSettingTbl = $('#selectedColumnsTable').find(
        `input[name="columnName"][value="${colName}"]`,
    );
    const datType = colInSettingTbl
        ? $(colInSettingTbl[0]).attr('data-type')
        : '';
    return datType;
};

// --- B-Sprint36+80 #5 ---
const storePreviousValueCfgDataTable = (element) => {
    element.previousValue = element.value;
};
// --- B-Sprint36+80 #5 ---

// generate 5 records and their header
const genColumnWithCheckboxCfgDataTable = (cols, rows, yoyakugoRows) => {
    const htmlRows = [];

    const intlabel = $(`#${DataTypes.INTEGER.i18nLabelID}`).text();
    const strlabel = $(`#${DataTypes.STRING.i18nLabelID}`).text();
    const dtlabel = $(`#${DataTypes.DATETIME.i18nLabelID}`).text();
    const reallabel = $(`#${DataTypes.REAL.i18nLabelID}`).text();
    const allRealLabel = $(`#${DataTypes.REAL.i18nAllLabel}`).text();
    const allIntLabel = $(`#${DataTypes.INTEGER.i18nAllLabel}`).text();
    const allStrLabel = $(`#${DataTypes.STRING.i18nAllLabel}`).text();
    const isShowInJP =
        document.getElementById('select-language').value === 'ja';

    const yoyakugoOptions = [];
    yoyakugoOptions.push(
        '<optgroup><option value="head" disabled>---</option><option value="">---</option>',
    );
    yoyakugoRows
        .filter((el) => el.group === 1)
        .forEach((row) => {
            const { data_group_type, name_sys, name } = row;
            yoyakugoOptions.push(
                `<option data-row='${JSON.stringify(row)}' value="${data_group_type}" title="${isShowInJP ? name_sys : name}">${isShowInJP ? name : name_sys}</option>`,
            );
        });

    yoyakugoOptions.push('</optgroup><optgroup class="dash-line" label="">');

    yoyakugoRows
        .filter((el) => el.group === 2)
        .forEach((row) => {
            const { id, name_sys, name } = row;
            yoyakugoOptions.push(
                `<option data-row='${JSON.stringify(row)}' value="${id}" title="${name}">${isShowInJP ? name : name_sys}</option>`,
            );
        });
    yoyakugoOptions.push('</optgroup>');

    cols.forEach((col, i) => {
        let isNull = true;
        rows.forEach((row) => {
            if (!isEmpty(row[col.column_name])) {
                isNull = false;
            }
        });

        const checkedColType = (type) => {
            const orgColType = getColDataTypeCfgDataTable(col.column_name);
            const colType = orgColType || col.data_type;
            if (type === colType) {
                return ' selected="selected"';
            }
            return '';
        };

        const sampleData = rows.map((row) => {
            const key = col.column_name;
            let val;
            if (col.is_get_date) {
                val = moment(row[key]).format(DATE_FORMAT_TZ);
                if (val === 'Invalid date') {
                    val = '';
                }
            } else {
                val = row[key];
                if (col.data_type === DataTypes.INTEGER.name) {
                    val = parseIntData(val);
                } else if (col.data_type === DataTypes.REAL.name) {
                    val = parseFloatData(val);
                }
            }
            return `<td class="px-2" data-original="${row[key]}">${val}</td>`;
        });

        let isCandidateColAttr = '';
        if (col.is_candidate_col != null) {
            isCandidateColAttr = `is-candidate-col="${col.is_candidate_col}"`;
        }

        htmlRows.push(`
            <tr class="${col.is_show === true ? '' : 'd-none'}">
                <td style="width: 30px" class="px-2 text-center fixed-column fixed-column1">${i + 1}</td>
                <td class="px-2 fixed-column fixed-column2 name">
                    <div class="custom-control custom-checkbox">
                        <input type="checkbox" onclick="selectTargetColDataTable(this)"
                            class="check-item custom-control-input col-checkbox" value="${col.name}"
                            id="checkbox-${col.romaji}" 
                            data-type="${col.data_type}" 
                            data-column-name="${col.column_name}" 
                            data-romaji="${col.romaji}"
                            data-isnull="${isNull}" 
                            data-col-index="${i}"
                            data-m-data-group-id="${col.data_group_type || ''}"
                            ${isCandidateColAttr}>
                        <label class="custom-control-label" for="checkbox-${col.romaji}">${col.column_name}</label>
                    </div>
                </td>
                <td style="width: 150px; min-width: 150px" class="form-control-small p-0 fixed-column fixed-column3 yoyaku" id="yoyakuSelects-parent">
                    <select type="select" id="yoyakugo-col-${i}" name="yoyakuSelects" class="form-control select2-selection--single"
                        name="dataGroupType" onchange="changeSelectedYoyakugoRaw(this)">  <!-- B-Sprint36+80 #5 -->
                        ${yoyakugoOptions.join('')}
                    </select>
                </td>
                <td style="width: 120px; min-width: 120px" class="p-0 fixed-column fixed-column4 d-none">
                    <select id="col-${i}" class="form-control form-control-small csv-datatype-selection" style="border-radius: 0"
                        onchange="parseDataType(this, ${i})" onfocus="storePreviousValueCfgDataTable(this)">  <!-- B-Sprint36+80 #5 -->
                        <option value="${DataTypes.REAL.value}"${checkedColType(DataTypes.REAL.name)}>${reallabel}</option>
                        <option value="${DataTypes.INTEGER.value}"${checkedColType(DataTypes.INTEGER.name)}>${intlabel}</option>
                        <option value="${DataTypes.STRING.value}"${checkedColType(DataTypes.STRING.name)}>${strlabel}</option>
                        <option value="${DataTypes.DATETIME.value}"${checkedColType(DataTypes.DATETIME.name)}>${dtlabel}</option>
                        <option value="${DataTypes.REAL.value}" data-all="${DataTypes.REAL.value}">${allRealLabel}</option>
                        <option value="${DataTypes.INTEGER.value}" data-all="${DataTypes.INTEGER.value}">${allIntLabel}</option>
                        <option value="${DataTypes.STRING.value}" data-all="${DataTypes.STRING.value}">${allStrLabel}</option>
                    </select>
                </td>
               ${sampleData.join('') ? sampleData.join('') : '<td></td>'}
            </tr>
        `);
    });

    dataTableSettingModalElements.latestDataHeader.empty().append(`<tr>
            <th class="fixed-column fixed-column1"></th>
            <th class="fixed-column fixed-column2">${i18nCommon.columnName}</th>
            <th class="fixed-column fixed-column3">${i18nCommon.columnAttribute}</th>
            <th class="fixed-column fixed-column4 d-none">${i18nCommon.type}</th>
            <th class="text-left" colspan="${rows.length ?? 5}">${i18nCommon.sampleData}</th>
        </tr>
    `);

    dataTableSettingModalElements.latestDataBody.empty().append(htmlRows);

    initYoyakugoSelect2();
    onSearchTableContent('searchDataTableModal', 'latestDataTable');
    fixedWidthForName();
};

const initYoyakugoSelect2 = () => {
    function customYoyakuConfig(state) {
        if (!state.id) return state.text;
        let $state;
        if (state.id === 'head') {
            $state = $(`
                <div class="custom-select2 d-flex">
                    <span style="width: 25%">COLUMN NAME</span>
                    <span style="width: 25%">${i18nCommon.columnName}</span>
                    <span style="width: 35%">${i18nCommon.type}</span>
                    <span style="width: 35%">補足説明</span>
                </div>
            `);
        } else {
            const row = jsonParse(state.element.getAttribute('data-row'));
            $state = $(`
                <div class="custom-select2 d-flex">
                    <span title=${row.name_sys}" style="width: 25%">${row.name_sys}</span>
                    <span title="${row.name}" style="width: 25%">${row.name}</span>
                    <span title="${row.sample || ''}" style="width: 35%">${row.sample || ''}</span>
                    <span title="${row.hint}" style="width: 35%">${row.hint}</span>
                </div>
            `);
        }
        return $state;
    }

    function matchStart(params, data) {
        // If there are no search terms, return all of the data
        if ($.trim(params.term) === '') {
            return data;
        }

        // Skip if there is no 'children' property
        if (typeof data.children === 'undefined') {
            return null;
        }

        // `data.children` contains the actual options that we are matching against
        var filteredChildren = [];
        $.each(data.children, function (idx, child) {
            if (
                child.text.toUpperCase().includes(params.term.toUpperCase()) ||
                child.title.toUpperCase().includes(params.term.toUpperCase())
            ) {
                filteredChildren.push(child);
            }
        });

        // If we matched any of the timezone group's children, then set the matched children on the group
        // and return the group object
        if (filteredChildren.length) {
            var modifiedData = $.extend({}, data, true);
            modifiedData.children = filteredChildren;

            // You can return modified objects from here
            // This includes matching the `children` how you want in nested data sets
            return modifiedData;
        }

        // Return `null` if the term should not be displayed
        return null;
    }

    const yoyakuSelects = $('[name=yoyakuSelects]');

    yoyakuSelects.select2({
        templateResult: customYoyakuConfig,
        matcher: matchStart,
    });

    $('.select2-selection--single').off('select2:open');
    $('.select2-selection--single').on('select2:open', (e) => {
        const target = e.target.closest('.yoyaku');
        if (!target) return;

        const selects = $(
            '.select2-container.select2-container--default.select2-container--open',
        );
        const targetSelect = $(selects[selects.length - 1]);
        const top = targetSelect.css('top');
        const left = targetSelect.css('left');
        const ul = targetSelect.find('.select2-results__options');
        const selectDropdown = $('.select2-dropdown');
        const dropdownHeight = selectDropdown ? selectDropdown.height() : 0;
        selectDropdown.css({
            maxWidth: `calc(100vw - ${left} - 38px)`,
            minWidth: '780px',
        });
        // if (dropdownHeight && dropdownHeight < 300) {
        //     selectDropdown.closest('.select2-container').css({
        //         top: `calc(${top} - ${dropdownHeight}px)`,
        //     });
        // }
        ul.css({
            maxHeight: `calc(100vh - ${top} + ${window.scrollY - 48}px)`,
        });
        setTimeout(() => {
            $('.select2-results__group:not(:eq(0))').addClass('dash-line');
        }, 500);
    });
};

const fixedWidthForName = () => {
    const labels = dataTableSettingModalElements.latestDataBody.find(
        '.name .custom-control-label',
    );
    const [, maxWidth] = findMinMax([...labels].map((el) => $(el).width()));
    dataTableSettingModalElements.latestDataBody.find('.name').css({
        width: `${maxWidth + 50}px`,
    });
};

const validateFixedColumnsDataTable = () => {
    if (isAddNewDataTableMode()) {
        return;
    }
    $(
        `table[name=selectedColumnsTable] input:checkbox[name="${dataTableSettingModalElements.dateTime}"]`,
    ).each(function disable() {
        $(this).attr('disabled', true);
        if ($(this).is(':checked')) {
            // disable serial as the same row
            $(this)
                .closest('tr')
                .find(
                    `input:checkbox[name="${dataTableSettingModalElements.serial}"]`,
                )
                .attr('disabled', true);
        }
    });
    $(
        `table[name=selectedColumnsTable] input:checkbox[name="${dataTableSettingModalElements.auto_increment}"]`,
    ).each(function disable() {
        $(this).attr('disabled', true);
    });
};

// validation checkboxes of selected columns
const validateCheckBoxesAllDataTable = () => {
    $(
        `table[name=selectedColumnsTable] input:checkbox[name="${dataTableSettingModalElements.dateTime}"]`,
    ).each(function validateDateTime() {
        $(this).on('change', function f() {
            if ($(this).is(':checked')) {
                $(
                    `table[name=selectedColumnsTable] input:checkbox[name="${dataTableSettingModalElements.dateTime}"]`,
                )
                    .not(this)
                    .prop('checked', false);
                // uncheck serial at the same row
                $(this)
                    .closest('tr')
                    .find(
                        `input:checkbox[name="${dataTableSettingModalElements.serial}"]`,
                    )
                    .prop('checked', false);
            }
        });
    });

    $(
        `table[name=selectedColumnsTable] input:checkbox[name="${dataTableSettingModalElements.serial}"]`,
    ).each(function validateSerial() {
        $(this).on('change', function f() {
            if ($(this).is(':checked')) {
                // uncheck datetime at the same row
                $(this)
                    .closest('tr')
                    .find(
                        `input:checkbox[name="${dataTableSettingModalElements.dateTime}"]`,
                    )
                    .prop('checked', false);
            }

            // show warning about resetting trace config
            showResetDataLinkDataTable($(this));
        });
    });
};

// temp. todo có thể không cần dùng (business k có tình huống này)
const showResetDataLinkDataTable = (boxElement) => {
    const currentCfgDataTableId =
        dataTableSettingModalElements.dataTableID.val() || null;
    if (!currentCfgDataTableId) {
        return;
    }
    currentAsLinkIdBoxDataTable = boxElement;
    $(dataTableSettingModalElements.warningResetDataLinkModalDataTable).modal(
        'show',
    );
};

const validateSelectedColumnInputCfgDataTable = () => {
    validateCheckBoxesAllDataTable();
    handleEnglishNameChange_cfgDataTable();
    // addAttributeToElement();
    validateAllCoefs();
    validateFixedColumns();
    updateSelectedRows();
    updateTableRowNumber(null, $('table[name=selectedColumnsTable]'));
};

const createOptCoefHTMLCfgDataTable = (operator, coef, isNumeric) => {
    const operators = ['+', '-', '*', '/'];
    let numericOperators = '';
    operators.forEach((opr) => {
        const selected = operator === opr ? ' selected="selected"' : '';
        numericOperators += `<option value="${opr}" ${selected}>${opr}</option>`;
    });
    const selected = operator === 'regex' ? ' selected="selected"' : '';
    const textOperators = `<option value="regex" ${selected}>${i18nCfgDataTable.validLike}</option>`;
    let coefHTML = `<input name="coef" class="form-control" type="text" value="${coef || ''}">`;
    if (!isNumeric) {
        coefHTML = `<input name="coef" class="form-control text" type="text" value="${coef || ''}">`;
    }
    return [numericOperators, textOperators, coefHTML];
};

const selectTargetColDataTable = (col, doValidate = true) => {
    const dataGroupType = col.getAttribute('data-m-data-group-id');
    if (dataGroupType === '') return;
    if (col.checked) {
        // add new record
        const colDataType = col.getAttribute('data-type');
        const romaji = col.getAttribute('data-romaji');
        const columnName = col.getAttribute('data-column-name');
        const colConfig = {
            data_type: colDataType,
            column_name: columnName,
            english_name: romaji,
            data_group_type: dataGroupType,
            name: col.value,
        };
        dataTableSettingModalElements.seletedColumnsBody.append(
            getDataTableColumnConfigHTML(colConfig),
        );
        if (dataGroupType !== masterDataGroup.HORIZONTAL_DATA) {
            const yoyakuSelects = $(`select[name='yoyakuSelects']`);
            yoyakuSelects.each(function () {
                $(this)
                    .find(`option[value=${dataGroupType}]`)
                    .attr('disabled', true);
            });
        }
        if (doValidate) {
            updateSelectAllCheckboxCfgDataTable();
        }
    } else {
        // remove record
        if (dataGroupType !== masterDataGroup.HORIZONTAL_DATA) {
            const yoyakuSelects = $(`select[name='yoyakuSelects']`);
            yoyakuSelects.each(function () {
                $(this)
                    .find(`option[value=${dataGroupType}]`)
                    .removeAttr('disabled');
            });
        }
        $(`#selectedColumnsTable tr[uid="${col.value}"]`).remove();
        updateTableRowNumber(null, $('table[name=selectedColumnsTable]'));
    }

    // update selectAll input
    if (doValidate) {
        updateSelectAllCheckboxCfgDataTable();
    }
};

const autoSelectColumnEventCfgDataTable = (selectAllElement) => {
    const selectedMasterType = dataTableSettingModalElements.databases
        .find('option:selected')
        .attr('master_type');
    renderedColsCfgDataTable = false;
    const isAllChecked = selectAllElement.checked;

    changeSelectionCheckboxCfgDataTable(isAllChecked);

    if (!isAllChecked) {
        $('.col-checkbox').each(function f() {
            const isColChecked = $(this).prop('checked');
            // const isNull = $(this).data('isnull');
            if (!isColChecked || !isAllChecked) {
                // select null cols only and select only once
                $(this).prop('checked', isAllChecked).trigger('change');
            }
        });
    } else {
        const sourceColumns = $(
            '#latestDataTable tbody tr input[type=checkbox]',
        );
        const yoyakuGoSelects = $('[name=yoyakuSelects]');
        for (let i = 0; i < sourceColumns.length; i++) {
            const sourceColumn =
                sourceColumns[i].getAttribute('data-column-name');
            const isCandidate =
                sourceColumns[i].getAttribute('is-candidate-col'); // only for datetime & serial columns
            if (
                sourceColumn in autoMappingRules &&
                (isCandidate == null || isCandidate === 'true')
            ) {
                $(yoyakuGoSelects[i]).val(autoMappingRules[sourceColumn]);
                $(yoyakuGoSelects[i]).select2().trigger('change');
                if (
                    [DEFAULT_CONFIGS.V2.master_type].includes(
                        selectedMasterType,
                    ) &&
                    Number(autoMappingRules[sourceColumn]) ===
                        Number(masterDataGroup.DATA_TIME)
                ) {
                    $(yoyakuGoSelects[i]).prop('disabled', 'disabled');
                }
            }
        }
    }

    // validate after selecting all to save time
    validateSelectedColumnInput();

    // re-init column definition select boxes
    initYoyakugoSelect2();
};

const selectAllColumnEventDataTable = (selectAllElement) => {
    const isAllChecked = selectAllElement.checked;

    changeSelectionCheckboxCfgDataTable(false, isAllChecked);

    $('.col-checkbox').each(function f() {
        const isColChecked = $(this).prop('checked');
        // const isNull = $(this).data('isnull');
        if (!isColChecked || !isAllChecked) {
            // select null cols only and select only once
            $(this).prop('checked', isAllChecked).trigger('change');
        }
    });

    // validate after selecting all to save time
    validateSelectedColumnInputCfgDataTable();
};

const changeSelectionCheckboxCfgDataTable = (
    autoSelect = true,
    selectAll = false,
) => {
    $(dataTableSettingModalElements.autoSelect).prop('checked', autoSelect);
    $(dataTableSettingModalElements.selectAllSensor).prop('checked', selectAll);
};

const updateSelectAllCheckboxCfgDataTable = () => {
    let selectAll = true;
    let autoSelect = true;

    if (renderedColsCfgDataTable) {
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
        changeSelectionCheckboxCfgDataTable(autoSelect, selectAll);
    }
};

// update latest records table by yaml data
const updateLatestDataCheckboxCfgDataTable = () => {
    const getHtmlEleFunc = genJsonfromHTML(
        dataTableSettingModalElements.seletedColumnsBody,
        'selects',
        true,
    );
    const selectJson = getHtmlEleFunc(
        dataTableSettingModalElements.columnName,
        (ele) => [
            ele.value,
            ele.getAttribute('data-m-data-group-id'),
            ele.getAttribute('data-type'),
        ],
    );
    const SELECT_ROOT = Object.keys(selectJson)[0];
    if (
        dataTableSettingModalElements.columnName in selectJson[SELECT_ROOT] &&
        selectJson[SELECT_ROOT][dataTableSettingModalElements.columnName]
    ) {
        for (const [colname, dataGroupType, dataType] of selectJson[
            SELECT_ROOT
        ][dataTableSettingModalElements.columnName]) {
            const colNameInput = $(`input[value="${colname}"]`);
            colNameInput.prop('checked', true);
            colNameInput
                .parent()
                .parent()
                .parent()
                .find(
                    `select[name=${dataTableSettingModalElements.yoyakuselectName}]`,
                )
                .val(dataGroupType)
                .trigger('change');
        }
    }
};

const preventSelectAllCfgDataTable = (preventFlag = false) => {
    // change render flag
    renderedColsCfgDataTable = !preventFlag;
    $(dataTableSettingModalElements.selectAllSensor).prop(
        'disabled',
        preventFlag,
    );
    $(dataTableSettingModalElements.autoSelect).prop('disabled', preventFlag);
};

const updateCurrentDatasourceCfgDataTable = () => {
    const currentShownTableName =
        dataTableSettingModalElements.tables.val() || null;
    const currentShownDataSouce =
        dataTableSettingModalElements.databases.val() || null;
    // re-assign datasource id and table of process
    if (currentShownDataSouce) {
        currentProcData.ds_id = Number(currentShownDataSouce);
    }
    if (currentShownTableName) {
        currentProcData.table_name = currentShownTableName;
    }
};

const autoSelectColAttr = () => {
    const autoCheckStatus =
        $(dataTableSettingModalElements.autoSelect).is(':checked') || false;
    if (!autoCheckStatus) {
        $(dataTableSettingModalElements.autoSelect)
            .prop('checked', true)
            .trigger('change');
    }
};

let yoyakugoRows = [];
let autoMappingRules = {};

// get latestRecords
const showLatestRecordsCfgDataTable = (
    formData,
    clearSelectedColumnBody = true,
) => {
    dataTableSettingModalElements.dataTableLoading.show();

    ajaxWithLog({
        url: '/ap/api/setting/show_latest_records',
        data: formData,
        dataType: 'json',
        type: 'POST',
        contentType: false,
        processData: false,
        success: (json) => {
            json = jsonParse(json);
            // reset checkbox flag
            renderedColsCfgDataTable = undefined;
            dataTableSettingModalElements.dataTableLoading.hide();
            if (json.cols_duplicated) {
                showToastrMsg(
                    i18nCommon.colsDuplicated,
                    i18nCommon.warningTitle,
                );
            }

            showToastrMsgFailLimit(json);
            yoyakugoRows = json.yoyakugo;
            if (json.auto_mapping_rules) {
                autoMappingRules = json.auto_mapping_rules;
            }
            genColumnWithCheckboxCfgDataTable(
                json.cols,
                json.rows,
                json.yoyakugo,
            );

            //show to action bar
            dataTableSettingModalElements.latestDataTableTopAction.css({
                display: 'flex',
            });

            preventSelectAllCfgDataTable(renderedColsCfgDataTable);

            if (clearSelectedColumnBody) {
                dataTableSettingModalElements.seletedColumnsBody.empty();
            } else {
                // update column checkboxes from selected columns
                updateLatestDataCheckboxCfgDataTable();
            }

            // update changed datasource
            updateCurrentDatasourceCfgDataTable();

            // update select all check box after update column checkboxes
            updateSelectAllCheckboxCfgDataTable();

            // bind select columns with context menu
            bindSelectColumnsHandler_cfgDataTable();

            // trigger auto select as default
            autoSelectColAttr();

            // disable column attribute if scanned
            if (dataTableModalCurrentProcId) {
                $('[name="yoyakuSelects"]').attr('disabled', true);
            }
        },
        error: () => {
            dataTableSettingModalElements.dataTableLoading.hide();
            showToastrMsg('Server Error', 'Error', MESSAGE_LEVEL.ERROR);
        },
    });
};

const handleEnglishNameChange_cfgDataTable = () => {
    const englishCol = $(dataTableSettingModalElements.englishNameInput);
    englishCol.on('change', (event) => {
        const inputChanged = event.currentTarget.value;
        ajaxWithLog({
            url: '/ap/api/setting/to_eng',
            type: 'POST',
            data: JSON.stringify({ colname: inputChanged }),
            dataType: 'json',
            contentType: 'application/json',
        }).done((res) => {
            res = jsonParse(res);
            event.currentTarget.value = res.data;
        });
    });
};

const checkDuplicateCfgDataTableName = () => {
    // get current list of (process-mastername)
    const existingDataTableIdMasterNames = {};
    $('#tblDataTableConfig tr').each(function f() {
        const dataTableId = $(this).data('datatable-id');
        const rowId = $(this).attr('id');
        if (rowId) {
            const masterName =
                $(`#${rowId} input[name=dataTableName]`).val() || '';
            existingDataTableIdMasterNames[`${dataTableId}`] = masterName;
        }
    });

    // check for duplication
    const beingEditedDataTableName =
        dataTableSettingModalElements.dataTableName.val();
    const existingMasterNames = Object.values(existingDataTableIdMasterNames);
    const isEditingSameDataTable =
        existingDataTableIdMasterNames[
            currentDataTableItem.data('datatable-id')
        ] === beingEditedDataTableName;
    if (
        beingEditedDataTableName &&
        existingMasterNames.includes(beingEditedDataTableName) &&
        !isEditingSameDataTable
    ) {
        // show warning message
        const dupProcessNameMsg = $(
            dataTableSettingModalElements.msgProcNameAlreadyExist,
        ).text();
        displayRegisterMessage(
            dataTableSettingModalElements.alertDataTableNameErrorMsg,
            {
                message: dupProcessNameMsg,
                is_error: true,
            },
        );
        return true;
    }
    $(dataTableSettingModalElements.alertDataTableNameErrorMsg).css(
        'display',
        'none',
    );

    return false;
};

const scrollTopDataTableModalCfgDataTable = () => {
    $(dataTableSettingModalElements.dataTableSettingModal).animate(
        { scrollTop: 0 },
        'fast',
    );
    $('#dataTableName').focus();
};

const validateDataTableName = () => {
    // get current list of (process-mastername)
    const dataTableName = dataTableSettingModalElements.dataTableName.val();
    if (!dataTableName.trim()) {
        // show warning message
        displayRegisterMessage(
            dataTableSettingModalElements.alertDataTableNameErrorMsg,
            {
                message: $(
                    dataTableSettingModalElements.msgDataTableNameBlank,
                ).text(),
                is_error: true,
            },
        );
        return false;
    } else {
        $(dataTableSettingModalElements.alertDataTableNameErrorMsg).css(
            'display',
            'none',
        );
    }

    return !checkDuplicateCfgDataTableName();
};

const autoFillShownNameToModalCfgDataTable = () => {
    $('#selectedColumnsTable tbody tr').each(function f() {
        const shownName = $(this)
            .find(`input[name="${dataTableSettingModalElements.shownName}"]`)
            .val();
        const columnName = $(this)
            .find(`input[name="${dataTableSettingModalElements.columnName}"]`)
            .val();
        if (isEmpty(shownName)) {
            $(this)
                .find(
                    `input[name="${dataTableSettingModalElements.shownName}"]`,
                )
                .val(columnName);
        }
    });
};

const getDataColumnsAttribute = (checkBoxColumns) => {
    const colConfig = [];
    const isChecks = [];
    let isWide = true;
    checkBoxColumns.each((e, col) => {
        const isChecked = col.checked === true;
        const colDataType = col.getAttribute('data-type');
        const dataGroupType = col.getAttribute('data-m-data-group-id');
        const romaji = col.getAttribute('data-romaji');
        const columnName = col.getAttribute('data-column-name');
        const data = {
            data_type: colDataType,
            column_name: columnName,
            english_name: romaji,
            data_group_type: dataGroupType,
            name: col.value,
        };
        if (isChecked) {
            isChecks.push(isChecked);
            colConfig.push(data);
        }
        if (Number(dataGroupType) === masterDataGroup.PROCESS_NAME) {
            isWide = false;
        }
    });
    return [colConfig, isChecks, isWide];
};

const collectDataTableCfgData = (columnDataRaws) => {
    const procID = dataTableSettingModalElements.dataTableID.val() || null;
    const procName = dataTableSettingModalElements.dataTableName.val();
    const dataSourceId =
        dataTableSettingModalElements.databases.find(':selected').val() || '';
    const tableName =
        dataTableSettingModalElements.tables.find(':selected').val() || '';
    const masterType =
        dataTableSettingModalElements.detailMasterType.val() || '';
    const comment = dataTableSettingModalElements.comment.val();
    const partitionFrom =
        $('#cfgDataTableForm').find('[name=partitionFrom]').val() || null;
    const partitionTo =
        $('#cfgDataTableForm').find('[name=partitionTo]').val() || null;
    const procColumns = columnDataRaws;
    return {
        id: procID,
        name: procName,
        data_source_id: dataSourceId,
        table_name: tableName,
        partition_from: partitionFrom,
        partition_to: partitionTo,
        comment,
        columns: procColumns,
        detail_master_type: masterType,
    };
};

// Save Data Table Column
const saveDataTableCfg = (selectedJson, isChecks) => {
    clearWarning();
    dataTableSettingModalElements.dataTableSettingModal.modal('hide');
    dataTableSettingModalElements.confirmScanMasterDataModal.modal('hide');

    const dataTableCfgData = collectDataTableCfgData(selectedJson);
    const data = {
        proc_config: dataTableCfgData,
        is_checks: isChecks,
    };

    ajaxWithLog({
        url: 'api/setting/data_table_config',
        type: 'POST',
        data: JSON.stringify(data),
        dataType: 'json',
        contentType: 'application/json',
    }).done((res) => {
        res = jsonParse(res);
        // sync Vis network
        // reloadTraceConfigFromDB();  # data table no does not this

        // update GUI
        if (res.status !== HTTP_RESPONSE_CODE_500) {
            $(currentDataTableItem)
                .find('input[name="dataTableName"]')
                .val(res.data.name)
                .prop('disabled', true);
            $(currentDataTableItem)
                .find('select[name="cfgDataSourceName"]')
                .append(
                    `<option value="${res.data.data_source_id}" selected="selected">${res.data.data_source_name}</option>`,
                )
                .prop('disabled', true);
            $(currentDataTableItem)
                .find('textarea[name="comment"]')
                .val(res.data.comment)
                .prop('disabled', true);
            $(currentDataTableItem).attr('id', `data_table_${res.data.id}`);
            $(currentDataTableItem).attr('data-ds-id', res.data.data_source_id);
            $(currentDataTableItem).attr('data-datatable-id', res.data.id);
            if (res.data.mapping_page_enabled) {
                $(currentDataTableItem).attr('data-mapping-page-enabled', '');
            }
        }
    });

    $(`#tblDataTableConfig #${procModalCurrentProcId}`).data('type', '');
};

const runRegisterDataTableColumnConfigFlow = (edit = false) => {
    clearWarning();

    // validate data table name null
    const validateFlg = validateDataTableName();
    if (!validateFlg) {
        scrollTopDataTableModalCfgDataTable();
        return;
    }

    const checkBoxColumns = dataTableSettingModalElements.latestDataBody.find(
        'input[type=checkbox]',
    );
    const [selectJsons, isChecks] = getDataColumnsAttribute(checkBoxColumns);
    let isDatetime = false;
    let isLongColumn = false;
    let isWideColumn = false;
    let isDuplicateMaster = false;
    let isSelectDataGroup = true;
    let selectedDataGroup = [];
    const selectedOption =
        dataTableSettingModalElements.databases.find('option:selected');
    const selectedMasterType = selectedOption.attr('master_type');
    const selectedDataSourceType = selectedOption.attr('type');
    const isCSVOther =
        selectedDataSourceType === DEFAULT_CONFIGS.CSV.id &&
        selectedMasterType === DEFAULT_CONFIGS.CSV.master_type;
    selectJsons.map(function (e, i) {
        if (e.data_group_type === '') {
            isSelectDataGroup = false;
        }

        if (
            Number(e.data_group_type) !== masterDataGroup.HORIZONTAL_DATA &&
            selectedDataGroup.includes(e.data_group_type)
        ) {
            isDuplicateMaster = true;
        }

        selectedDataGroup.push(e.data_group_type);
        if (!isCSVOther) {
            if (
                isChecks[i] &&
                [
                    masterDataGroup.DATA_TIME,
                    masterDataGroup.AUTO_INCREMENTAL,
                ].includes(Number(e.data_group_type))
            ) {
                isDatetime = true;
            }
        }
        if (
            isChecks[i] &&
            Number(e.data_group_type) === masterDataGroup.HORIZONTAL_DATA
        ) {
            isWideColumn = true;
        }
        if (
            isChecks[i] &&
            [
                masterDataGroup.DATA_NAME,
                masterDataGroup.DATA_ABBR,
                masterDataGroup.DATA_VALUE,
            ].includes(Number(e.data_group_type))
        ) {
            isLongColumn = true;
        }
    });

    // validate column attribute cannot select HorizontalData & DataValue at same time
    // if (false && isLongColumn && isWideColumn) {
    //     // show warning message
    //     let errorMsg = $(
    //         dataTableSettingModalElements.msgColumnAttributeOnlyHorizontalDataOrDataValue,
    //     ).text();
    //     errorMsg = errorMsg.replace(
    //         '{0}',
    //         $(dataTableSettingModalElements.msgHorizontalData).text(),
    //     );
    //     errorMsg = errorMsg.replace(
    //         '{1}',
    //         $(dataTableSettingModalElements.msgDataValue).text(),
    //     );
    //     displayRegisterMessage(
    //         dataTableSettingModalElements.alertDataTableNameErrorMsg,
    //         {
    //             message: errorMsg,
    //             is_error: true,
    //         },
    //     );
    //     scrollTopDataTableModalCfgDataTable();
    //     return;
    // }

    // validate do not select HorizontalData or DataName, DataValue
    if (
        !selectedDataGroup.includes(String(masterDataGroup.HORIZONTAL_DATA)) &&
        !selectedDataGroup.includes([
            String(masterDataGroup.DATA_NAME),
            String(masterDataGroup.DATA_VALUE),
        ])
    ) {
        // show warning message
        let errorMsg =
            'Need to select horizontal or select both data name and data value.';
        displayRegisterMessage(
            dataTableSettingModalElements.alertDataTableNameErrorMsg,
            {
                message: errorMsg,
                is_error: true,
            },
        );
        scrollTopDataTableModalCfgDataTable();
        return;
    }
    // validate allow select DataName, DataValue same time
    if (
        (selectedDataGroup.includes(String(masterDataGroup.DATA_NAME)) &&
            !selectedDataGroup.includes(String(masterDataGroup.DATA_VALUE))) ||
        (selectedDataGroup.includes(String(masterDataGroup.DATA_VALUE)) &&
            !selectedDataGroup.includes(String(masterDataGroup.DATA_NAME)))
    ) {
        // show warning message
        let errorMsg = 'Both data name and data value must be selected.';
        displayRegisterMessage(
            dataTableSettingModalElements.alertDataTableNameErrorMsg,
            {
                message: errorMsg,
                is_error: true,
            },
        );
        scrollTopDataTableModalCfgDataTable();
        return;
    }

    // validate column attribute cannot select HorizontalData & DataValue at same time
    if (isDuplicateMaster) {
        // show warning message
        let errorMsg = 'Selected duplicate column attribute';
        displayRegisterMessage(
            dataTableSettingModalElements.alertDataTableNameErrorMsg,
            {
                message: errorMsg,
                is_error: true,
            },
        );
        scrollTopDataTableModalCfgDataTable();
        return;
    }

    // validate column attribute not select data group
    if (!isSelectDataGroup) {
        // show warning message
        let errorMsg = 'Have column select data group';
        displayRegisterMessage(
            dataTableSettingModalElements.alertDataTableNameErrorMsg,
            {
                message: errorMsg,
                is_error: true,
            },
        );
        scrollTopDataTableModalCfgDataTable();
        return;
    }

    // check if date is checked
    const getDateMsgs = [];
    if (!isCSVOther && !isDatetime) {
        getDateMsgs.push($(csvResourceElements.msgErrorNoGetdate).text());
    }

    let hasError = true;
    if (getDateMsgs.length > 0) {
        const messageStr = Array.from(getDateMsgs).join('<br>');
        displayRegisterMessage(
            dataTableSettingModalElements.alertDataTableNameErrorMsg,
            {
                message: messageStr,
                is_error: true,
            },
        );
    } else {
        hasError = false;
        // show confirm modal if validation passed
        if (edit) {
            $(dataTableSettingModalElements.confirmReRegisterProcModal).modal(
                'show',
            );
        } else {
            $(dataTableSettingModalElements.confirmScanMasterDataModal).modal(
                'show',
            );
        }
    }

    // scroll to where messages are shown
    // if (hasError) {
    //     const settingContentPos = dataTableSettingModalElements.dataTableSettingContent.offset().top;
    //     const bodyPos = dataTableSettingModalElements.dataTableSettingModalBody.offset().top;
    //     dataTableSettingModalElements.dataTableSettingModal.animate({
    //         scrollTop: settingContentPos - bodyPos,
    //     }, 'slow');
    // }
};

const checkClearColumnsTableCfgDataTable = (dsID, tableName) => {
    if (
        ((isEmpty(currentCfgDataTable.table_name) && isEmpty(tableName)) ||
            currentCfgDataTable.table_name === tableName) &&
        currentCfgDataTable.ds_id === dsID
    ) {
        return false;
    }
    return true;
};

const showHideModesDataTable = (isEditMode = false, isReadOnlyMode = false) => {
    // show/hide tables
    // change mode name from button
    const settingModeName = $(procModali18n.settingMode).text();
    const editModeName = $(procModali18n.editMode).text();
    if (isEditMode) {
        $(`${dataTableSettingModalElements.changeModeBtn} span`).text(
            ` ${settingModeName}`,
        );
        $(dataTableSettingModalElements.dataSM).parent().removeClass('hide');
        $(dataTableSettingModalElements.cfgDataTableContent).addClass('hide');
    } else {
        // clear editMode table
        $(dataTableSettingModalElements.dataSM).html('');
        $(`${dataTableSettingModalElements.changeModeBtn} span`).text(
            ` ${editModeName}`,
        );
        $(dataTableSettingModalElements.dataSM).parent().addClass('hide');
        $(dataTableSettingModalElements.cfgDataTableContent).removeClass(
            'hide',
        );
    }
    // disable register buttons
    $(dataTableSettingModalElements.reRegisterBtn).prop('disabled', isEditMode);
    $(dataTableSettingModalElements.scanBtn).prop('disabled', isEditMode);

    if (isReadOnlyMode) {
        $(dataTableSettingModalElements.scanBtn).hide();
        $(dataTableSettingModalElements.showRecordsBtn).hide();
    } else {
        $(dataTableSettingModalElements.scanBtn).show();
        $(dataTableSettingModalElements.showRecordsBtn).show();
    }
};

const selectAllColsHandlerCfgDataTable = (e) => {
    e.preventDefault();
    e.stopPropagation();

    // show context menu when right click
    const menu = $(dataTableSettingModalElements.checkColsContextMenu);
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

const bindSelectColumnsHandler_cfgDataTable = () => {
    $('table[name=latestDataTable] thead th').each((i, th) => {
        th.addEventListener(
            'contextmenu',
            selectAllColsHandlerCfgDataTable,
            false,
        );
        th.addEventListener('mouseover', hideCheckColMenu, false);
    });
};

const selectAllToRightCfgDataTable = (isSelect = true) => {
    const targetColIdx = $(
        dataTableSettingModalElements.checkColsContextMenu,
    ).attr('data-target-col');
    const allColsFromTable = $('table[name=latestDataTable] tr input');
    // update selection from column
    for (let i = targetColIdx; i < allColsFromTable.length; i++) {
        const targetCol = $(allColsFromTable[i]);
        const isChecked = targetCol.is(':checked');
        if (isChecked !== isSelect) {
            if (!targetCol.length) continue;
            targetCol.prop('checked', isSelect);
            selectTargetCol(targetCol[0], (doValidate = false));
        }
    }

    // add validation
    validateSelectedColumnInputCfgDataTable();

    updateSelectAllCheckboxCfgDataTable();

    // reset attribute in context menu
    $(dataTableSettingModalElements.checkColsContextMenu).attr(
        'data-target-col',
        '',
    );
    hideCheckColMenu();
};

const startImportTransactionData = (cfg_data_table_id) => {
    ajaxWithLog({
        url: 'api/setting/start_transaction_import',
        type: 'POST',
        data: JSON.stringify({
            cfg_data_table_id: cfg_data_table_id,
        }),
        dataType: 'json',
        contentType: 'application/json',
    }).done((res) => {});
};

let renderedColsCfgDataTable;
let count = 0;
let click_count = 0;
const checkCountClick = () => {
    click_count = click_count + 1;
    if (click_count > 1) {
        count = 0;
    }
};

$(() => {
    // workaround to make multiple modal work
    $(document).on('hidden.bs.modal', '.modal', () => {
        if ($('.modal:visible').length) {
            $(document.body).addClass('modal-open');
        }
    });

    // confirm auto fill master name
    $(dataTableSettingModalElements.msgConfirmBtn).click(() => {
        autoFillShownNameToModalCfgDataTable();

        $(dataTableSettingModalElements.msgModal).modal('hide');
    });

    // click Import Data
    dataTableSettingModalElements.scanBtn.click(() => {
        runRegisterDataTableColumnConfigFlow((edit = false));
    });

    // confirm Import Data
    dataTableSettingModalElements.confirmImportDataBtn.click(() => {
        $(dataTableSettingModalElements.confirmImportDataModal).modal('hide');

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

    // confirm Scan Master Data
    dataTableSettingModalElements.confirmScanMasterDataBtn.click(() => {
        $(dataTableSettingModalElements.confirmScanMasterDataModal).modal(
            'hide',
        );

        // const selectJson = getSelectedColumnsAsJson_cfgDataTable();
        const checkBoxColumns =
            dataTableSettingModalElements.latestDataBody.find(
                'input[type=checkbox]',
            );
        const [selectJson, isChecks, isWide] =
            getDataColumnsAttribute(checkBoxColumns);
        saveDataTableCfg(selectJson, isChecks);

        // save order to local storage
        setTimeout(() => {
            dragDropRowInTable.setItemLocalStorage(
                $(dataTableElements.tableDataTableList)[0],
            ); // set proc table order
        }, 2000);

        recentEdit(dataTableModalCurrentProcId);

        // show toastr
        showToastr();
        showJobAsToastr();
        if (isWide) {
            loadingShow(undefined, undefined, LOADING_TIMEOUT_FOR_COLUMN_ATTR);
        }

        dataTableSettingModalElements.latestDataTableTopAction.hide();
    });

    // confirm Scan Master Data
    dataTableSettingModalElements.pullDataBtn.click(() => {
        $(dataTableSettingModalElements.pullDataBtn).modal('hide');

        $(dataTableSettingModalElements.confirmPullDataModal).modal('show');
    });

    // confirm Scan Master Data
    dataTableSettingModalElements.confirmPullDataBtn.click(() => {
        $(dataTableSettingModalElements.confirmPullDataModal).modal('hide');
        $(dataTableSettingModalElements.dataTableSettingModal).modal('hide');

        startImportTransactionData(dataTableModalCurrentProcId);
        // show toastr
        showToastr();
        showJobAsToastr();
    });

    // re-register process config
    dataTableSettingModalElements.reRegisterBtn.click(() => {
        runRegisterProcConfigFlow((edit = true));
    });
    dataTableSettingModalElements.confirmReRegisterProcBtn.click(() => {
        $(dataTableSettingModalElements.confirmReRegisterProcModal).modal(
            'hide',
        );
        // save order to local storage
        setTimeout(() => {
            dragDropRowInTable.setItemLocalStorage(
                $(procElements.tableProcList)[0],
            ); // set proc table order
        }, 2000);
        recentEdit(procModalCurrentProcId);
    });

    // load tables to modal combo box
    loadTablesCfgDataTable(
        dataTableSettingModalElements.databases.find(':selected').val(),
    );

    // Databases onchange
    dataTableSettingModalElements.databases.change(() => {
        if (count == 0) {
            count = count + 1;
        } else {
            const dsSelected = dataTableSettingModalElements.databases
                .find(':selected')
                .val();
            loadTablesCfgDataTable(dsSelected);
        }
    });

    // Tables onchange
    dataTableSettingModalElements.tables.change(() => {
        const sourceType = $(
            '#dataTableSettingModal select[name=databaseName] option:selected',
        ).attr('type');
        const dataSourceId = $(
            '#dataTableSettingModal select[name=databaseName] option:selected',
        ).val();
        const tableNameSelection = $(
            '#dataTableSettingModal select[name=tableName] option:selected',
        ).text();
        if (
            sourceType === DEFAULT_CONFIGS.ORACLE.id &&
            EFA_TABLES.includes(tableNameSelection)
        ) {
            // show partition tables
            formData = {
                data_source_id: dataSourceId,
                table_prefix: tableNameSelection,
            };
            ajaxWithLog({
                url: '/ap/api/setting/get_partition_table',
                data: JSON.stringify(formData),
                dataType: 'json',
                type: 'POST',
                contentType: false,
                processData: false,
                success: (res) => {
                    showPartitions(res.partitions);
                },
            });
        }
        setDataTableName();
    });

    // Master type onchange
    dataTableSettingModalElements.detailMasterTypes.change(() => {
        setDataTableName();
    });

    dataTableSettingModalElements.dataTableName.on('mouseup', () => {
        userEditedProcName = true;
    });

    // Show records button click event
    dataTableSettingModalElements.showRecordsBtn.click((event) => {
        dataTableSettingModalElements.latestDataTableTopAction.hide();
        dataTableSettingModalElements.latestDataHeader.empty();
        dataTableSettingModalElements.latestDataBody.empty();
        event.preventDefault();
        const currentShownTableName =
            dataTableSettingModalElements.tables.find(':selected').val() ||
            null;
        const currentShownDataSouce =
            dataTableSettingModalElements.databases.find(':selected').val() ||
            null;
        const currentShownDataSouceType =
            dataTableSettingModalElements.databases
                .find(':selected')
                .attr('type') || null;
        // if (currentShownTableName === null && currentShownDataSouceType != DEFAULT_CONFIGS.CSV.id) {
        //     return;
        // }
        const clearDataFlg = checkClearColumnsTableCfgDataTable(
            currentShownDataSouce,
            currentShownTableName,
        );
        const procModalForm = $(dataTableSettingModalElements.formId);
        const formData = new FormData(procModalForm[0]);

        // preventSelectAll_cfgDataTable(true);  # tunghh temp comment out

        // reset select all checkbox when click showRecordsBtn
        $(dataTableSettingModalElements.selectAllColumn).css(
            'display',
            'block',
        );
        $(dataTableSettingModalElements.autoSelectAllColumn).css(
            'display',
            'block',
        );

        showLatestRecordsCfgDataTable(formData, clearDataFlg);
    });

    dataTableSettingModalElements.dataTableName.on(
        'focusout',
        checkDuplicateCfgDataTableName,
    );

    $(dataTableSettingModalElements.revertChangeAsLinkIdBtn).click(() => {
        currentAsLinkIdBoxDataTable.prop(
            'checked',
            !currentAsLinkIdBoxDataTable.prop('checked'),
        );
    });

    $('select[name="attrDefCols"]').select2();
});

const confirmSaveColumnAttribute = () => {
    $(dataTableSettingModalElements.confirmDirectColumnAttribute).modal('show');
};

const closeCSVTSVDataSourceConfigModal = (element) => {
    $(element).closest('.modal').modal('hide');
    $(dbConfigElements.csvModal).modal('show');
};

const closeColumnAttributeConfigModal = (element) => {
    $(element).closest('.modal').modal('hide');
    if (
        dataTableSettingModalElements.cancelBtn
            .isBackToCSVTSVDataSourceConfigModal === true
    ) {
        $(dbConfigElements.csvModal).modal('show');
    }
};
