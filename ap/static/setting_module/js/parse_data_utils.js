/**
 * @file Contains all functions relate to parsing data.
 * @author Pham Minh Hoang <hoangpm6@fpt.com>
 * @author Tran Thi Kim Tuyen <tuyenttk5@fpt.com>
 */

/**
 * Parse to Integer data
 * @param {number|string} v
 * @return {number|string} - a parsed value or empty in case cannot be parsed
 */
const parseIntData = (v) => {
    let val = trimBoth(String(v));
    if (isEmpty(val)) {
        val = '';
    } else {
        val = parseInt(Number(val));
        if (isNaN(val)) {
            val = '';
        }
    }
    return val;
};

/**
 * Parse to Float data
 * @param {number|string} v
 * @return {string} - a parsed value or empty in case cannot be parsed
 */
const parseFloatData = (v) => {
    let val = trimBoth(String(v));
    if (isEmpty(val)) {
        val = '';
    } else if (val.toLowerCase() === COMMON_CONSTANT.INF.toLowerCase()) {
        val = COMMON_CONSTANT.INF.toLowerCase();
    } else if (val.toLowerCase() === COMMON_CONSTANT.MINF.toLowerCase()) {
        val = COMMON_CONSTANT.MINF.toLowerCase();
    } else {
        // TODO why do we need to re-parse?
        val = parseFloat(Number(val));
        if (isNaN(val)) {
            val = '';
        }
    }
    return val;
};

/**
 * Parse to boolean data
 * @param {number|boolean|string} v
 * @return {string} - a parsed value or empty in case cannot be parsed
 */
const parseBooleanData = (v) => {
    let val = trimBoth(String(v));
    if (['1', 'true'].includes(val)) {
        val = 'true';
    } else if (['0', 'false'].includes(val)) {
        val = 'false';
    } else {
        val = '';
    }
    return val;
};

/**
 * Parse Data Type for sample data in row
 * @param {HTMLLIElement} ele - a li HTML element object
 * @param {number|string?} idx - index of row in table's body
 * @param {HTMLDivElement?} dataTypeDropdownElement - a div HTML object of datatype dropdown menu
 */
const parseDataType_New = (ele, idx, dataTypeDropdownElement = null) => {
    dataTypeDropdownElement =
        dataTypeDropdownElement == null
            ? ele.closest('div.config-data-type-dropdown')
            : dataTypeDropdownElement;
    const rowIndex =
        idx == null ? $(dataTypeDropdownElement.closest('tr')).index() : idx;
    changeBackgroundColor(ele);

    const value = ele.getAttribute('raw-data-type');
    const dataColumn = $(ele).closest('tr').find('input[type=checkbox]');
    // do not parse `datetime`, `date`, and `time` here.
    if (
        value === DataTypes.DATETIME.bs_value ||
        value === DataTypes.DATE.bs_value ||
        value === DataTypes.TIME.bs_value
    ) {
        const dataType = Object.entries(DataTypes).find(
            ([, definition]) => definition.bs_value === value,
        );
        if (dataType !== null) {
            const dataTypeName = dataType[1].name;
            showProcDatetimeFormatSampleData({
                colIdx: rowIndex,
                dataType: dataTypeName,
                dataTypeDropdownElement: dataTypeDropdownElement,
            });
            dataColumn.attr('raw-data-type', dataTypeName);
        }
    }

    const vals = [
        ...$(dataTypeDropdownElement)
            .closest('div.proc-config-content')
            .find('table[name="processColumnsTableSampleData"] tbody')
            .find(`tr:eq(${rowIndex}) .sample-data`),
    ].map((el) => $(el));
    const attrName = 'data-original';

    switch (value) {
        case DataTypes.DATETIME.bs_value:
        case DataTypes.DATE.bs_value:
        case DataTypes.TIME.bs_value: {
            const condition = displayDatetimeFormatCondition();
            const appliedRows = [
                {
                    dataType: value,
                    colIdx: $(ele).closest('tr').find('td[title="index"]')[0]
                        .dataset.colIdx,
                    dataTypeDropdownElement: dataTypeDropdownElement,
                },
            ];
            if (condition.showRawData) {
                showRawFormatDatetimeData(...appliedRows);
            } else if (condition.showInputFormat) {
                showInputFormatDatetimeData(...appliedRows);
            } else if (condition.showAutoFormat) {
                showAutoFormatDatetimeData(...appliedRows);
            } else {
                notifyInvalidFormat();
            }
            dataColumn.attr('raw-data-type', value);
            break;
        }
        case DataTypes.INTEGER.bs_value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseIntData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.INTEGER.name);
            break;
        case DataTypes.SMALL_INT.bs_value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseIntData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.SMALL_INT.name);
            break;
        case DataTypes.BIG_INT.bs_value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseIntData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.BIG_INT.name);
            break;
        case DataTypes.BOOLEAN.bs_value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseBooleanData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.BOOLEAN.name);
            break;
        case DataTypes.REAL.bs_value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = parseFloatData(val);
                e.html(val);
            }
            dataColumn.attr('raw-data-type', DataTypes.REAL.name);
            break;
        case DataTypes.REAL_SEP.bs_value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = val.replaceAll(',', '');
                val = parseFloatData(val);
                e.html(val);
            }
            break;
        case DataTypes.INTEGER_SEP.bs_value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = val.replaceAll(',', '');
                val = parseIntData(val);
                e.html(val);
            }
            break;
        case DataTypes.EU_REAL_SEP.bs_value:
            for (const e of vals) {
                let val = e.attr(attrName);
                val = val.replaceAll('.', '');
                val = val.replaceAll(',', '.');
                val = parseFloatData(val);
                e.html(val);
            }
            break;
        case DataTypes.EU_INTEGER_SEP.bs_value:
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
};
