/**
 * @file Manages the configuration settings for error bar.
 * @author Le Sy Khanh Duy <duylsk@fpt.com>
 * @author Tran Thi Kim Tuyen <tuyenttk5@fpt.com>
 */

const errorManagement = () => {
    let totalRow = 0
    let currentRow = -1;
    let currentRowEle = null;
    let incrementalNum = 0;

    let currentTableIdx;
    // const row_idx = 0;
    // const col_idx = 1;
    // const msg_idx = 2;

    const alertMsgControlDiv = $('[name=alertMsgControl]');
    // const alertMsgCount = alertMsgControlDiv.find('[name=alertMsgCount]');
    const currentAlertMsg = alertMsgControlDiv.find('[name=currentAlertMsg]');
    const currentAlertMsgIndex = alertMsgControlDiv.find('[name=alertMsgIndex]');
    // const errorTableDiv = alertMsgControlDiv.find('[name=ErrorTable]');
    const errorBody = alertMsgControlDiv.find('tbody');
    const nextBtn = alertMsgControlDiv.find('[name=nextAlert]');
    const prevBtn = alertMsgControlDiv.find('[name=prevAlert]');
    const jumpBtn = alertMsgControlDiv.find('[name=jumpToAlert]');
    const collapseLink = alertMsgControlDiv.find('a');
    // const collapseIcon = alertMsgControlDiv.find('[name=showTable]');

    const changeCollapseBtnLabel = () => {
        if (collapseLink.hasClass('minus-180')) {
            collapseLink.toggleClass('minus-180');
        } else {
            setTimeout(() => {
                collapseLink.toggleClass('minus-180');
            }, 100);
        }
    };

    const getAllRows = () => {
        return errorBody.find('tr');
    };

    const getByRow = (tableId, rowId) => {
         return errorBody.find(`tr[data-table-id="${tableId}"]`).filter((i, e) => ['-9999', rowId].includes($(e).attr('data-row-id')));
    };

    const getByRowAndErrorType = (tableId, rowId, errorType) => {
        return errorBody.find(`tr[data-table-id="${tableId}"][data-row-id="${rowId}"][data-error-type="${errorType}"]`);
    };

    const getByRowAndCol = (tableId, rowId, columnName) => {
        return errorBody.find(`tr[data-table-id="${tableId}"][data-row-id="${rowId}"][data-column-name="${columnName}"]`);
    };

    const getByRowAndColAndErrorType = (tableId, rowId, columnName, errorType) => {
        return errorBody.find(`tr[data-table-id="${tableId}"][data-row-id="${rowId}"][data-column-name="${columnName}"][data-error-type="${errorType}"]`);
    };

    const genTrID = () => {
        incrementalNum++;
        totalRow++;
        return `alertMsgRecord_${incrementalNum}`;
    };

    const genTableRecord = (trID, tableId, rowId, columnName, msg, errorType, index) => {
        return `<tr id="${trID}" index="${index}" data-table-id="${tableId}" data-row-id="${rowId}" data-column-name="${columnName}" data-error-type="${errorType}">
                    <td name="columnName">${i18nCommon[columnName] ?? columnName}</td>
                    <td name="msg">${msg}</td>
                </tr>`;
    };

    const addCurrentClass = () => {
        currentRowEle.addClass('alert-msg-bg');
    };
    const removeCurrentClass = () => {
        const rows = getAllRows();
        rows.removeClass('alert-msg-bg');
    };

    const annotateErrorCell = (tableId, rowId, columnName, hasError) => {
        const targetTable = $(`#${tableId}`);
        const errorCell = getErrorCell(targetTable, rowId, columnName)
        errorCell.attr(CONSTANT_VARIABLES.CELL_HAS_ERROR, hasError);
    };

    const getErrorCell = (targetTable, rowId, columnName) => {
        return targetTable
                .find('tbody > tr')
                .filter((i, e) => getHiddenIdRow(e) === rowId)
                .find(`td[column-title=${columnName}]`)
                .find('input, select')
    };

    const jumpToHtml = () => {
        if (jumpBtn.is(':checked')) {
            const tableId = getTableId(currentRowEle);
            const rowId = getRowId(currentRowEle) + '';
            const columnName = getColName(currentRowEle);
            const targetTable = $(`#${tableId}`);
            if (targetTable.is(':hidden')) {
                // Expend table if it is collapsed
                targetTable.closest('div.table-groups').find(`button[sub-table-id="${tableId}"]`).click();
            }

            scrollIntoView(targetTable, false);

            const errorCell = getErrorCell(targetTable, rowId, columnName)

            errorCell.focus();
            scrollIntoView(errorCell);
        }
    };

    const getMsg = (ele) => {
        return $(ele).find('td[name=msg]').text();
    };

    const getRowId = (ele) => {
        return $(ele).data('row-id');
    };

    const getColName = (ele) => {
        return $(ele).data('column-name');
    };

    const getTableId = (ele) => {
        return $(ele).data('table-id');
    };

    const getIndex = (ele) => {
        return $(ele).attr('index');
    };

    const getErrorType = (ele) => {
        return $(ele).data('error-type');
    };

    const getUniqueStr = (tableId, rowId, columnName, msg, errorType) => {
        return `${tableId}|${rowId}|${columnName}|${msg}|${errorType}`;
    };

    const showCurrentMsg = () => {
        // const count = `${currentRow}\/${totalRow}件`;
        const rows = getAllRows();
        const total = rows.length;
        if (currentRowEle !== null) {
            const index = currentRowEle.attr('index');
            let currentIndex = 1;
            for ( let i = 0; i < total; i++ ) {
                if ($(rows[i]).attr('index') === index) {
                     currentIndex = i + 1;
                }
            }
            const count = `${currentIndex}/${total}件:`;
            currentAlertMsgIndex.text(count);
            currentAlertMsg.text(`${getMsg(currentRowEle)}`);
            currentAlertMsg.attr('index', index);
        }
    };

    const moveToCurrentRow = (isJump = true) => {
        if ($(currentRowEle).length === 0) {
            currentRowEle = errorBody.find('tr:first-child');
        }

        removeCurrentClass();
        showCurrentMsg();
        addCurrentClass();
        if (isJump) {
            jumpToHtml();
        }
    };

    const moveNext = () => {
        if ($(currentRowEle).length === 0) {
            currentRowEle = errorBody.find('tr:first-child');
        } else {
            currentRowEle = currentRowEle.next();
            if (currentRowEle.length === 0) {
                currentRowEle = errorBody.find('tr:first-child');
            }
        }

        moveToCurrentRow();
    };

    const movePrevious = () => {
        if ($(currentRowEle).length === 0) {
            currentRowEle = errorBody.find('tr:last-child');
        } else {
            currentRowEle = currentRowEle.prev();
            if (currentRowEle.length === 0) {
                currentRowEle = errorBody.find('tr:last-child')
            }
        }
        moveToCurrentRow();
    };

    const loadMessages = (dicParam, isKeepOldMessage = true) => {
        if (!dicParam) {
            setRootPaddingTopCss(true);
            return;
        }

        if (!isKeepOldMessage) {
            errorBody.empty();
            totalRow = 0;
            currentRow = -1;
            currentRowEle = null;
        }

        for ([tableId_rowId_pair, dicColumns] of Object.entries(dicParam)) {
            const [tableId, rowId] = tableId_rowId_pair.split(',');
            if (dicColumns == null) {  // In case a row is removed
                const shownMsgs = getByRow(tableId, rowId);
                shownMsgs.remove();
                const messageRelationNotRegister = getByRowAndErrorType(tableId, rowId, 'isRelationNotRegister');
                messageRelationNotRegister.remove();
                continue;
            }

            for (const [columnName_errorType_pair, msg] of Object.entries(dicColumns)) {
                const [columnName, errorType] = columnName_errorType_pair.split(',');
                let shownMsgs;
                if (errorType === 'isAtLeastOneInput' && (msg === '' || msg == null)) {  // In case of isAtLeastOneInput, only remove truly message
                    shownMsgs = getByRowAndColAndErrorType(tableId, rowId, columnName, errorType);
                } else {
                    shownMsgs = getByRowAndCol(tableId, rowId, columnName); // lay message hien tai
                }

                if (msg === '' || msg == null) {
                    for (const shownMsg of shownMsgs) {
                        const msgIndex = shownMsg.getAttribute('index');
                        const currentMsgIndex = currentAlertMsg[0].getAttribute('index');
                        if (msgIndex === currentMsgIndex) {
                            currentAlertMsg.text('');
                            currentRowEle = null;
                        }

                    }
                    shownMsgs.remove();
                    showCurrentMsg();

                    annotateErrorCell(tableId, rowId, columnName, false)

                    continue;
                }

                const uniqueStr = getUniqueStr(tableId, rowId, columnName, msg, errorType);
                let isExist = false;
                for (const shownMsg of shownMsgs) {
                    if (uniqueStr === getUniqueStr(getTableId(shownMsg), getRowId(shownMsg), getColName(shownMsg), getMsg(shownMsg), getErrorType(shownMsg))) {
                        isExist = true;
                        break;
                    }
                    shownMsg.remove();
                    showCurrentMsg();
                }

                if (!isExist) {
                    const trIDStr = genTrID();
                    const ele = genTableRecord(trIDStr, tableId, rowId, columnName, msg, errorType, incrementalNum);
                     if (getUniqueStr(tableId, rowId, columnName) === getUniqueStr(getTableId(currentRowEle), getRowId(currentRowEle), getColName(currentRowEle))) {
                        currentRowEle = $(ele);
                        showCurrentMsg();
                    }
                    errorBody.append(ele);

                    annotateErrorCell(tableId, rowId, columnName, true)

                    // show first msg
                    moveToCurrentRow(false);
                    $(`#${trIDStr}`).click((e) => {
                        currentRowEle = $(e.currentTarget)
                        moveToCurrentRow();
                    });
                }
            }
        }

        const rows = getAllRows();
        if (rows.length === 0) {
            alertMsgControlDiv.hide();
            setRootPaddingTopCss(true);
            return false;
        }

        // Show message area
        const flaskMessage = { message: '', is_error: true, is_warning: false };
        displayRegisterMessage('#commonAlertMsg', flaskMessage);
        setRootPaddingTopCss(false, '44px');

        return true;
    };

    const init = () => {
        currentTableIdx = -1;
        errorBody.empty();
        nextBtn.click(moveNext);
        prevBtn.click(movePrevious);
        collapseLink.click(changeCollapseBtnLabel);
        setRootPaddingTopCss(true);
    };

    return {init, loadMessages};
}

